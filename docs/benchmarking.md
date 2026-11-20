# Benchmarking ixcache

`bench/` holds a small benchmark client, **ixbench**: a Python script that copies
whole files with `xrdcp` and records throughput. It runs as Kubernetes Jobs in the
`ixcache` namespace and answers two questions:

1. **Cold vs hot**: how fast is one file read through the cache the first time
   (a miss, data comes from the origin), the second time (a hit), and directly
   from the origin without the cache?
2. **Maximum combined throughput**: how many GB/s can one or more deployed
   caches deliver to many parallel clients?

The image `ivukotic/ixcache-bench` is built and pushed by
`.github/workflows/bench.yml` on every change under `bench/` on `main`.

## How it works

- **File selection** comes from the ixcache telemetry index in Elasticsearch
  (`ixcache-*`, see [telemetry.md](telemetry.md)): a random sample of files read
  in the last `--days` before `--until`, without errors, between `--min-size` and
  `--max-size`, one record per `cache_key`. The sample is per access, so it has
  the real origin mix (today about 90% `eosatlas.cern.ch`); use `--origin` or
  `--exclude-origin` to change that. Paths containing `scratchdisk` are skipped
  by default, as those files are often deleted.
- **Cold** files: `--cold-for=<instance>` drops every file that was ever read
  through that ixcache instance (its `IXCACHE_INSTANCE`), so the first read is a
  miss. ixbench's own reads are recorded too, so the next run picks other files.
- Each transfer is `xrdcp -s <url> -`; ixbench reads the data from the pipe and
  counts bytes as they arrive. That gives exact per-second rates (also across
  pods) and the time to first byte, at the cost of some client CPU.
- Results go to stdout as `IXBENCH {json}` lines. `ixbench report` reads the
  logs of all pods of a Job and prints the summary.

## Setup

1. An Elasticsearch secret for file selection (and `report --verify`), with the
   cluster URL and either a user and password or an API key with read access to
   `ixcache-*`:

   ```sh
   kubectl -n ixcache create secret generic ixbench-es \
     --from-literal=ES_URL=https://<es-host>:9200 \
     --from-literal=ES_USER=<user> --from-literal=ES_PASSWORD=<password>
   ```

   (`ES_API_KEY` instead of user and password; `ES_INSECURE=1` to skip TLS
   verification.) Without Elasticsearch, `--paths-file` takes a TSV of
   `origin`, `path`, `size`, `cache_key`, e.g. mounted from a ConfigMap; `ixbench
   select` writes one.
2. In `bench/k8s/*.yaml`, set `ENDPOINTS` to the caches under test,
   `host:port`, comma-separated (the deployment listens on the node's
   address, port 1094). Set `--cold-for` to their `IXCACHE_INSTANCE` names.
3. Client pods never run on a node with an ixcache pod (required pod
   anti-affinity), and stress pods prefer separate nodes.

`launch.sh` and `collect.sh` need `bash` and `kubectl`; `collect.sh` also runs
`ixbench.py report` with a local Python 3 (standard library only).

## 1. Cold and hot reads

```sh
bench/k8s/launch.sh coldhot
bench/k8s/collect.sh ixbench-coldhot-<run> --verify
```

One pod reads 50 files, one at a time. For each file: a cold read through the
cache and a direct read from the origin (alternating which goes first, since
the second read of the pair finds the origin's own caches warm), then, after
`--hot-delay` seconds, a hot read through the cache.

The direct read needs a grid proxy, made by the same x509 sidecar as in the
deployment from `xcache-cert-secret`; the first proxy can take ~15 minutes.
Without `--direct`, delete the sidecar and its volumes from `coldhot.yaml`.

The report has one row per file (MB/s and time to first byte for cold, hot and
direct), then per kind the median, 10th and 90th percentiles and the aggregate
(total bytes / total time), and the slowdown of cold reads against direct ones,
which is the number to compare with the 20% first-read budget.

`--verify` matches every read with ixcache's own telemetry record and shows the
fraction of bytes it served from its disks. A hot read is not guaranteed to be
100% hits: when a disk falls behind, ixcache skips caching blocks rather than
grow memory (see `WriteBudget`). If the hot hit ratio is low, raise
`--hot-delay`. Matching uses the pod IP as `client_host`; if traffic to the
caches is NATed, records will not match.

## 2. Maximum combined throughput

```sh
# hot: fill the caches with the working set, then measure hits
bench/k8s/launch.sh warm
kubectl -n ixcache wait --for=condition=complete --timeout=6h job/ixbench-warm-<run>
bench/k8s/launch.sh stress 180
bench/k8s/collect.sh ixbench-stress-<run> --verify --per-endpoint --timeseries stress.csv
```

`stress.yaml` starts `NPODS` client pods (Job `completions`, keep them equal).
`launch.sh` sets a common start time (here 180 s ahead) so that all pods step
through `--stages` together: with `--stages=2,4,8,16,32,64` and 8 pods, stage 0
runs 16 parallel streams, stage 5 runs 512. Each stage lasts `--stage-seconds`;
its first `--warmup-seconds` are left out of the mean. Streams are spread evenly
over `ENDPOINTS`.

- `--mode=hot` reads random files from the working set (300 files of
  100 MB-5 GB, a few hundred GB; `ixbench select` with the same arguments
  prints the total). It must fit in every cache. It must be warmed first with **exactly the same
  selection arguments** (`--count`, `--seed`, `--until`, sizes) in `warm.yaml`;
  the fixed `--until` keeps the sample the same between the two runs. Change
  both to get a new working set.
- `--mode=cold` reads every file once, each pod its own share, from files
  never read through the `--cold-for` instances, with a fresh seed per launch.
  This also measures the origins and the WAN. It needs enough files: about
  throughput x total time / mean file size (for 5 GB/s over 18 minutes and
  700 MB files, ~8000; 10000 is the per-query limit). The report warns if pods
  ran out.

Per stage the report shows the combined GB/s and Gbit/s, the best 10-second
average, completed and failed transfers, MB/s per stream and the range of
per-pod rates. The stage with the highest mean is reported as the maximum.
When the total stops growing with more streams while per-pod rates flatten
and the caches still have CPU and NIC headroom, the clients are the limit: add
pods or nodes. `--verify` sums ixcache telemetry for the bench pods' IPs per
instance: bytes out, hit ratio, and bytes fetched from origins.
`--timeseries` writes the per-second totals and per-endpoint rates to a CSV.

## Running ixbench by hand

```sh
docker run --rm -e ES_URL=... -e ES_USER=... -e ES_PASSWORD=... \
  ivukotic/ixcache-bench select --count 20 --cold-for xcache-uc-1
docker run --rm ivukotic/ixcache-bench coldhot --help
python bench/ixbench.py report <logs>...   # also on Windows
```

URLs are `root://<endpoint>//root://<origin>/<path>`, as clients use the cache.
