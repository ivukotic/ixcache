# ixcache Implementation Plan

## Goal

A single-node Java subfile cache for XRootD reads, run in Kubernetes on a node
with local NVMe. Requirements are in `README.md`:

- Serve `xrdcp` full-file copies and ROOT sparse reads (5–30% of a file).
- Less than 20% slowdown on the first (uncached) read compared with reading
  directly from the origin.
- 1,000 concurrent clients.
- One UDP record per transfer to `ixcache.atlas-ml.org:1095`.

## Decisions

| Topic | Decision |
| --- | --- |
| Language / runtime | Java 21+ (LTS; move to 25 when the build environment has it), Maven wrapper (matches xrootd4j/dCache) |
| Networking | Netty for client-facing server and origin client |
| Client-facing protocol | xrootd4j server-side codec + GSI plugin, confirmed by Phase 0 spike |
| Origin auth | One service proxy read from `X509_USER_PROXY`; reloaded when the file changes |
| Client auth | `IXCACHE_AUTH`: `none` (default, as native XCache: no client authentication) or `gsi` (mandatory GSI; any valid certificate/proxy is accepted, no VOMS or other authorization check; needs a host certificate matching the name clients use). Identity is used only for logging; no per-user origin identity |
| Tokens | Out of scope for v1; keep the auth layer pluggable so they can be added |
| Origin selection | Taken from the embedded URL (`root://cache//root://origin//path`); no replica discovery or failover in v1 |
| Cache key | Rucio `scope:name` when the path matches the deterministic Rucio layout, otherwise origin host, port and path |
| Block size | Configurable, 64 kB default; tuned up to ~512 kB |
| Eviction | File-level LRU between configurable high and low watermarks |
| Data mutability | Origin files are immutable; no revalidation |

### Cache key derivation

For paths of the form `.../rucio/<scope parts>/<xx>/<yy>/<name>`:

1. Build `scope` by joining the scope parts with `.` (`user/ivukotic` →
   `user.ivukotic`).
2. Compute `md5("<scope>:<name>")` and check that its first two hex bytes equal
   `xx/yy`.
3. If they match, the key is `scope:name`, shared across all origins.
   Otherwise fall back to origin host, port and path, so files that merely
   share a path on different origins are never merged.

The check prevents unrelated paths that only look like Rucio paths from being
merged. Origin and full path are still kept per open so misses go to the origin
the client named.

## Architecture

```text
client (xrdcp / ROOT)
   │  XRootD + GSI
   ▼
Front end (Netty + xrootd4j)     login, auth, open, stat, read, readv, [pgread], query, close
   │
   ▼
Cache engine                     block map, in-flight table, read planner, LRU
   │            │
   ▼            ▼
NVMe store     Origin client (Netty)   pooled, multiplexed sessions per origin host,
                                       service proxy, redirect/wait handling
```

### Front end

- Supports multiple outstanding requests per connection, matched by stream ID.
  `xrdcp` keeps several chunk reads in flight.
- Supports `kXR_readv`. A client vector read becomes at most one origin vector
  read for the missing blocks. It is never split into serial reads.
- `kXR_pgread`: in the spike, determine whether current clients require it.
  Either implement it (CRC32C per 4 kB page is cheap) or advertise a protocol
  level at which clients fall back to plain reads.
- `kXR_query` checksum: forward to the origin in v1.
- `kXR_stat` / file size: obtained from the origin at the first open and stored
  in the file metadata. Later opens answer from metadata.

### Cache engine read path

1. Map the request `[offset, length]` (or each readv element) to blocks.
2. Serve present blocks from NVMe.
3. For missing blocks, check the in-flight table keyed by `(cacheKey, block)`.
   Join an existing fetch where possible; otherwise register new fetches.
4. Merge adjacent missing blocks into origin requests up to a configured
   maximum size, and send scattered ones as one origin `readv`.
5. Return bytes to the client as soon as the origin delivers them. Write to
   NVMe asynchronously, off the response path.
6. Mark blocks present only after their data write completes (see Storage).

Prefetch is off by default because files are re-read only 2–3 times and reads
are sparse. Sequential-access detection enables a bounded read-ahead for
`xrdcp`-like patterns. `xrdcp`'s large requests already span many blocks, so
read-ahead mainly helps small-request sequential readers.

If a write fails (disk full, I/O error), the client still gets the data. The
block stays absent and the error is counted.

### Storage

- One sparse data file per cached file, plus one small metadata file holding:
  format version, cache key, origin URL that created it, file size, block size,
  block bitmap, last access time, and access counters.
- Block size is fixed per file when the file is created, so changing the
  configured block size does not invalidate existing entries.
- Publishing a block: the data write completes, then the in-memory bitmap is
  updated. Metadata is flushed periodically and on close, after `fdatasync` of
  the data file. A crash can therefore lose recent blocks but never mark
  unwritten blocks as present.
- Startup recovery scans the metadata files and rebuilds the in-memory index
  and LRU order. Missing or corrupt metadata deletes the entry.
- Eviction: when usage passes the high watermark, delete least-recently-used
  files that are not open until usage is below the low watermark.

### Origin client

- Minimal async XRootD client: protocol, login, GSI auth with the service
  proxy, open, stat, read, readv, close, plus `kXR_redirect` and `kXR_wait`
  handling. Check xrootd4j's TPC client code for reusable pieces first.
- One authenticated session per origin host, multiplexing many open files, with
  a small connection pool per host. The service proxy makes sessions shareable.
- Timeouts, bounded retries on the same origin, and clear error mapping back to
  the client (no failover in v1).

### Telemetry

- One UDP JSON datagram per file close, kept under 1,200 bytes. Schema version
  1 fields: `v`, `ts`, `cache_host`, `client_dn`, `origin`, `path`, `cache_key`,
  `file_size`, `bytes_out`, `bytes_in_new`, `bytes_hit`, `read_ops`,
  `readv_ops`, `duration_ms`, `status`.
- Sent from a bounded queue on its own thread; dropped records are counted.
  Telemetry never blocks or fails a transfer.

### Request-pattern logging (trace collection)

There are no good representative ATLAS traces, so ixcache collects its own.
Optionally log each client request (key, offset, length, readv layout,
timestamp) to a compact local file. These logs feed an offline simulator that
replays them against different block sizes and read-ahead settings.

## Phases

### Phase 0 — Protocol spike (gate)

- xrootd4j server with GSI accepting `xrdcp` (XRootD 6.2.0) and ROOT
  (6.40.04), including `readv` and multiple outstanding requests.
- Pass-through to `xrootd.aglt2.org` using the custom origin client and the
  service proxy, with no caching.
- Settle the `pgread` approach and the protocol version to advertise.
- Measure pass-through overhead against direct access for `xrdcp` and a ROOT
  sparse read.

**Gate:** both clients work correctly through the pass-through. If xrootd4j's
server side or GSI is inadequate, fall back to a native XRootD front end with
the Java engine behind it, and decide before Phase 2.

**Status (2026-10-19): pass-through works in Docker; gate passed pending a WAN check.**
The xrootd4j front end on port 1092 (GSI or no client authentication) and the
origin client (`origin` package) pass data through without caching. Tested with
the XRootD 6.2.0 and 5.9.7 clients and ROOT 6.40.04, against stock XRootD 5.9.7
and 6.1.0 origins that require GSI, directly and through a redirector. That covers
`xrdcp` copies, ROOT TTreeCache sparse reads, `readv` up to 1024 elements, 256
reads outstanding on one connection, origin errors, and reopening after the origin
restarts. Overhead against direct access (median of 5, Docker on one host):
`xrdcp` of 100 MB 1.11x and ROOT sparse read 0.98x with no added latency; 0.97x and
0.98x with 50 ms RTT to the origin. ixcache keeps origin connections open, so later
runs skip the login and GSI round trips a direct client pays. Still to do: the same
comparison from the cluster against a real WAN origin. Findings:

- xrootd4j 4.6.5 GSI needs three fixes, carried as a patched copy of
  `GSIRequestHandler` that must precede xrootd4j-gsi on the classpath (checked at
  startup). XRootD 6 clients refuse its random tags, which are not `[a-zA-Z0-9./]{8}`.
  It signs any tag a peer sends with the host key. As a client it rejects sha256,
  the only digest XRootD 5.9+ servers offer. To report upstream.
- xrootd4j ignores the result of client chain validation; `GsiLoginHandler`
  validates the chain itself.
- The origin client reuses xrootd4j's client-side GSI handler (4.9+ exchange),
  with the service proxy passed as its "delegated" proxy. The proxy is re-read
  when its file changes. Its server check is replaced: xrootd4j ignores the chain
  validation result there too, and accepts only the URL's host name. ixcache
  also accepts the reverse-DNS name of the address connected to, as XRootD
  clients do; `xrootd.aglt2.org` is an alias over six dCache doors, each with a
  certificate for its own name.
- dCache origins send random tags outside `[a-zA-Z0-9./]`, so a peer's tag is
  signed if it is 8 bytes long, whatever its characters (XRootD's own rule).
- canl must be 2.7.0 (BouncyCastle 1.68). The 2.1.1 that xrootd4j-gsi pins fails
  chain validation with `IllegalAccessError`.
- `pgread`: not needed. The server does not advertise `kXR_suppgrw`, and every
  client tested uses plain `kXR_read`.
- ROOT collapses `//` in URLs, so opens arrive as `/root:/origin/path`.
- A `readv` element past EOF fails the whole request with `kXR_FSError`, as in the
  reference server. Short elements make clients drop the connection.

### Phase 1 — Storage engine (in parallel with Phase 0)

- Block store, metadata format, in-flight table, read planner, LRU with
  watermarks, and startup recovery, behind an interface with no protocol
  dependency.
- Unit and concurrency tests: overlapping misses fetched once, no partial
  blocks visible, crash/restart consistency, eviction correctness.

### Phase 2 — Integration

- Connect the front end and origin client to the engine.
- Implement cache key derivation and Rucio hash verification.
- Integration tests: byte-for-byte equality for full and sparse reads, two
  origin URLs for the same `scope:name` sharing one entry, EOF, and origin
  errors.

**Status (2026-10-20): done.** `CachingBackend` puts the cache between the front
end and the origin client. A file the cache knows opens without contacting the
origin; the origin file is opened on the first miss, so a fully cached file never
touches it. Settings: `IXCACHE_CACHE_DIR`, `IXCACHE_CACHE_SIZE` (default: file
system size), `IXCACHE_HIGH_WM`, `IXCACHE_LOW_WM`, `IXCACHE_BLOCK_SIZE`. The
integration tests run the whole path over TCP in one JVM. In Docker, `xrdcp`, the
XRootD 6.2.0 client and ROOT read correctly through the cache from a GSI origin,
then again with the origin stopped, and again after an ixcache restart. Known
gaps: modification time is not stored, so opens and stats served from the cache
report 0; only one cache directory is used (the pod's second disk is not).

### Phase 3 — Operations

- Configuration validation, structured logging, telemetry, request-pattern
  logging, health/readiness endpoints, Prometheus metrics, graceful shutdown,
  and service proxy reload.
- Kubernetes manifests: single replica, node-local NVMe persistent volume,
  service proxy mounted from a Secret, IGTF CA certificates and CRLs at
  `/etc/grid-security/certificates` (kept up to date) so client and origin
  certificates can be validated.

### Phase 4 — Qualify and tune

- Workloads: `xrdcp` of large files, and ROOT TTreeCache reads of real ATLAS
  DAOD/PHYSLITE files reading a realistic subset of branches.
- Measure the first-read slowdown at real WAN latency (the remote origin, or a
  local origin with `tc netem`).
- Block-size sweep: 64, 128, 256 and 512 kB. Measure origin byte
  amplification, origin request count, client wall time, and NVMe IOPS.
- 1,000 concurrent clients: latency, throughput, CPU, heap and GC, NVMe
  utilisation.
- Fault cases: disk full, origin failure mid-read, service proxy expiry, and
  restart under load.

## Acceptance criteria

- `xrdcp` full-file copies and ROOT sparse reads through ixcache are
  byte-for-byte correct.
- First-read wall time is at most 1.2× direct-from-origin wall time from the
  same client location, for both workloads.
- Sparse reads fetch only the requested blocks plus at most one block of
  rounding per range when read-ahead is off.
- Concurrent overlapping misses are fetched once; no corrupt or partial data is
  ever served.
- The same `scope:name` from different origins shares one cache entry.
- Disk use stays within the watermarks, and restart recovers a consistent cache.
- 1,000 concurrent clients sustained without errors.
- Telemetry records match schema v1; telemetry outage does not affect reads.
- With `IXCACHE_AUTH=gsi`, clients without a valid GSI credential are rejected; with `none`, clients restricted to GSI (`XrdSecPROTOCOL=gsi`) still work.
- The service proxy never appears in logs, metrics or telemetry.

## Out of scope for v1

Token authentication, per-user origin identity, replica discovery and failover,
writes, multi-node clustering.

## References

- [XRootD protocol specification](https://xrootd.web.cern.ch/doc/dev6/XRdv520.html)
- [XRootD project](https://xrootd.org/)
- [dCache xrootd4j](https://github.com/dCache/xrootd4j)
- [ROOT](https://root.cern.ch/doc/v640/)
