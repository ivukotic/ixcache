#!/usr/bin/env python3
"""ixbench: measure ixcache by copying whole files with xrdcp.

Subcommands
  select   pick files from the ixcache telemetry index and print them (TSV)
  coldhot  per file: cold read through the cache, optional direct read from the
           origin, then a hot re-read through the cache; one client, sequential
  warm     read a set of files once through every endpoint (fills the caches)
  stress   many parallel xrdcp streams against one or more caches, in stages of
           increasing concurrency, to find the maximum combined throughput
  report   summarise the logs of the commands above (from one or many pods)

Transfers run `xrdcp -s <url> -` and the data is read from the pipe, so bytes
are counted as they arrive: per-second rates are exact and time to first byte
is measured. Results are written to stdout as lines starting with "IXBENCH "
followed by one JSON object; `report` reads them back, e.g. from
`kubectl logs --prefix` of every pod of a Job.

Only the Python standard library is used. `report` also runs on Windows.
"""

import argparse
import base64
import csv
import hashlib
import json
import os
import random
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request

try:
    import fcntl
except ImportError:  # Windows: only `report` is used there.
    fcntl = None

F_SETPIPE_SZ = 1031
CHUNK = 1 << 20
MB = 1e6

_print_lock = threading.Lock()


def emit(obj):
    line = "IXBENCH " + json.dumps(obj, separators=(",", ":"))
    with _print_lock:
        print(line, flush=True)


def log(msg):
    with _print_lock:
        print(time.strftime("%H:%M:%S ") + msg, file=sys.stderr, flush=True)


def parse_bytes(s):
    s = str(s).strip().upper().rstrip("B")
    mult = {"K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}
    if s and s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    return int(s)


def split_list(s):
    return [x.strip() for x in (s or "").split(",") if x.strip()]


# ---------------------------------------------------------------- Elasticsearch

def es_search(index, body):
    base = os.environ.get("ES_URL")
    if not base:
        raise SystemExit("ES_URL is not set")
    url = base.rstrip("/") + "/" + urllib.parse.quote(index, safe="*,-_") + "/_search"
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    if os.environ.get("ES_API_KEY"):
        req.add_header("Authorization", "ApiKey " + os.environ["ES_API_KEY"])
    elif os.environ.get("ES_USER"):
        tok = base64.b64encode(f"{os.environ['ES_USER']}:{os.environ.get('ES_PASSWORD', '')}".encode())
        req.add_header("Authorization", "Basic " + tok.decode())
    ctx = ssl.create_default_context()
    if os.environ.get("ES_INSECURE") == "1":
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
        return json.load(r)


def add_selection_args(p):
    g = p.add_argument_group("file selection (from the ixcache telemetry index, or a file)")
    g.add_argument("--paths-file", help="TSV of origin, path, size, cache_key (as printed by `select`); skips Elasticsearch")
    g.add_argument("--index", default=os.environ.get("ES_INDEX", "ixcache-*"))
    g.add_argument("--count", type=int, default=50, help="number of files")
    g.add_argument("--seed", type=int, default=1, help="random seed of the sample")
    g.add_argument("--days", type=int, default=30, help="look back this many days before --until")
    g.add_argument("--until", default="now-1h/h",
                   help="newest access considered (ES date math or ISO time). A fixed time makes the "
                        "selection reproducible: warm and hot stress must use the same value")
    g.add_argument("--min-size", default="100M")
    g.add_argument("--max-size", default="5G")
    g.add_argument("--origin", default="", help="only these origins (host:port, comma-separated)")
    g.add_argument("--exclude-origin", default="", help="skip these origins (comma-separated)")
    g.add_argument("--exclude-substr", default="scratchdisk",
                   help="skip paths containing any of these (comma-separated); scratch files are often deleted")
    g.add_argument("--cold-for", default="",
                   help="skip files ever read through these ixcache instances (IXCACHE_INSTANCE, comma-separated)")


def select_files(a):
    if a.paths_file:
        files = []
        with open(a.paths_file) as fh:
            for line in fh:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 3 and not line.startswith("#"):
                    files.append({"origin": parts[0], "path": parts[1], "size": int(parts[2]),
                                  "key": parts[3] if len(parts) > 3 else parts[1]})
        return files[:a.count] if a.count else files

    end = a.until
    # "now-1h/h" -> "now-30d-1h/h"; "2026-11-01T00:00:00Z" -> "2026-11-01T00:00:00Z||-30d"
    begin = f"now-{a.days}d{end[3:]}" if end.startswith("now") else f"{end}||-{a.days}d"
    filters = [
        {"range": {"ts": {"gte": begin, "lte": end}}},
        {"term": {"status": "closed"}},
        {"term": {"errors": 0}},
        {"range": {"file_size": {"gte": parse_bytes(a.min_size), "lte": parse_bytes(a.max_size)}}},
    ]
    if split_list(a.origin):
        filters.append({"terms": {"origin": split_list(a.origin)}})
    must_not = []
    if split_list(a.exclude_origin):
        must_not.append({"terms": {"origin": split_list(a.exclude_origin)}})

    want = a.count
    size = min(10000, max(want * 4, 200))
    body = {
        "size": size,
        "_source": ["origin", "path", "file_size", "cache_key"],
        "query": {"function_score": {
            "query": {"bool": {"filter": filters, "must_not": must_not}},
            "random_score": {"seed": a.seed, "field": "_seq_no"},
            "boost_mode": "replace"}},
        "collapse": {"field": "cache_key.raw"},
    }
    res = es_search(a.index, body)
    skip = split_list(a.exclude_substr)
    files, seen = [], set()
    for h in res["hits"]["hits"]:
        s = h["_source"]
        key = s.get("cache_key") or s["path"]
        if key in seen or any(x in s["path"] for x in skip):
            continue
        seen.add(key)
        files.append({"origin": s["origin"], "path": s["path"], "size": int(s["file_size"]), "key": key})

    cold_for = split_list(a.cold_for)
    if cold_for and files:
        warm = set()
        keys = [f["key"] for f in files]
        for i in range(0, len(keys), 1000):
            chunk = keys[i:i + 1000]
            r = es_search(a.index, {
                "size": 0,
                "query": {"bool": {"filter": [{"terms": {"cache_key.raw": chunk}},
                                              {"terms": {"instance": cold_for}}]}},
                "aggs": {"k": {"terms": {"field": "cache_key.raw", "size": len(chunk)}}}})
            warm.update(b["key"] for b in r["aggregations"]["k"]["buckets"])
        files = [f for f in files if f["key"] not in warm]
        log(f"select: {len(warm)} candidates already read through {','.join(cold_for)}, skipped")

    # Stable order independent of how Elasticsearch broke ties.
    files.sort(key=lambda f: hashlib.sha1(f"{a.seed}:{f['key']}".encode()).hexdigest())
    if len(files) < want:
        log(f"select: only {len(files)} files match (wanted {want})")
    return files[:want]


# ------------------------------------------------------------------ transfers

def cache_url(endpoint, f):
    return f"root://{endpoint}//root://{f['origin']}/{f['path']}"


def origin_url(f):
    return f"root://{f['origin']}/{f['path']}"


class Meter:
    """Bytes received per wall-clock second and label."""

    def __init__(self):
        self.lock = threading.Lock()
        self.bins = {}

    def add(self, label, n):
        s = int(time.time())
        with self.lock:
            d = self.bins.setdefault(s, {})
            d[label] = d.get(label, 0) + n

    def drain(self, upto):
        with self.lock:
            ready = sorted(s for s in self.bins if s < upto)
            return [(s, self.bins.pop(s)) for s in ready]


class Procs:
    """Running xrdcp processes, so that a stop can kill them all."""

    def __init__(self):
        self.lock = threading.Lock()
        self.live = set()
        self.stopped = False

    def add(self, p):
        with self.lock:
            if self.stopped:
                p.kill()
            self.live.add(p)

    def remove(self, p):
        with self.lock:
            self.live.discard(p)

    def kill_all(self):
        with self.lock:
            self.stopped = True
            for p in self.live:
                try:
                    p.kill()
                except OSError:
                    pass


def transfer(url, label=None, meter=None, procs=None, timeout=3600):
    """Copies url with xrdcp, reading the data from a pipe. Returns a result dict."""
    errf = tempfile.TemporaryFile()
    t0 = time.time()
    p = subprocess.Popen(["xrdcp", "-s", url, "-"], stdout=subprocess.PIPE, stderr=errf, bufsize=0)
    if procs:
        procs.add(p)
    killed = {"timeout": False}

    def on_timeout():
        killed["timeout"] = True
        p.kill()

    timer = threading.Timer(timeout, on_timeout)
    timer.daemon = True
    timer.start()
    out = p.stdout
    if fcntl is not None:
        try:
            fcntl.fcntl(out.fileno(), F_SETPIPE_SZ, CHUNK)
        except OSError:
            pass
    buf = bytearray(CHUNK)
    n = 0
    ttfb = None
    try:
        while True:
            k = out.readinto(buf)
            if not k:
                break
            if ttfb is None:
                ttfb = time.time() - t0
            n += k
            if meter:
                meter.add(label, k)
        rc = p.wait()
    finally:
        timer.cancel()
        out.close()
        if procs:
            procs.remove(p)
    t1 = time.time()
    errf.seek(0)
    err = errf.read().decode(errors="replace").strip()
    errf.close()
    res = {"start": round(t0, 3), "end": round(t1, 3), "bytes": n, "rc": rc,
           "ttfb": round(ttfb, 4) if ttfb is not None else None}
    if killed["timeout"]:
        res["timeout"] = True
    elif procs and procs.stopped and rc != 0:
        res["aborted"] = True
    if rc != 0 and err:
        res["err"] = err[-400:]
    return res


def pod_info():
    idx = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
    ip = os.environ.get("POD_IP")
    if not ip:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except OSError:
            ip = ""
    return idx, ip, socket.gethostname()


def endpoints_arg(a):
    eps = split_list(a.endpoints or os.environ.get("ENDPOINTS", ""))
    if not eps:
        raise SystemExit("no cache endpoints: use --endpoints or ENDPOINTS (host:port, comma-separated)")
    return eps


def wait_until(t, what):
    d = t - time.time()
    if d > 0:
        log(f"{what}: waiting {d:.0f}s")
        time.sleep(d)


# ------------------------------------------------------------------ commands

def cmd_select(a):
    files = select_files(a)
    out = open(a.out, "w") if a.out else sys.stdout
    print("# origin\tpath\tsize\tcache_key", file=out)
    for f in files:
        print(f"{f['origin']}\t{f['path']}\t{f['size']}\t{f['key']}", file=out)
    total = sum(f["size"] for f in files)
    log(f"select: {len(files)} files, {total / 1e9:.1f} GB")


def cmd_coldhot(a):
    eps = endpoints_arg(a)
    idx, ip, host = pod_info()
    files = select_files(a)
    run = a.run or time.strftime("%Y%m%d-%H%M%S")
    emit({"type": "config", "mode": "coldhot", "run": run, "pod": idx, "pod_ip": ip, "host": host,
          "endpoints": eps, "files": len(files), "direct": a.direct, "hot_delay": a.hot_delay,
          "hot_repeats": a.hot_repeats, "time": time.time()})
    for i, f in enumerate(files):
        ep = eps[i % len(eps)]
        runs = {}
        # Alternate which read goes first: the second read of the pair finds
        # the origin's own caches warm, so neither should always benefit.
        order = ["cold"]
        if a.direct:
            order = ["cold", "direct"] if i % 2 == 0 else ["direct", "cold"]
        for kind in order:
            url = cache_url(ep, f) if kind == "cold" else origin_url(f)
            runs[kind] = transfer(url, timeout=a.timeout)
        if runs["cold"]["rc"] == 0:
            for r in range(a.hot_repeats):
                time.sleep(a.hot_delay)
                runs["hot" if r == 0 else f"hot{r + 1}"] = transfer(cache_url(ep, f), timeout=a.timeout)
        emit({"type": "coldhot", "run": run, "pod": idx, "i": i, "endpoint": ep, "origin": f["origin"],
              "path": f["path"], "key": f["key"], "size": f["size"], "order": order, "runs": runs})
        rates = " ".join(f"{k}={v['bytes'] / MB / max(v['end'] - v['start'], 1e-6):.0f}MB/s"
                         + ("" if v["rc"] == 0 else f"(rc={v['rc']})") for k, v in runs.items())
        log(f"[{i + 1}/{len(files)}] {f['size'] / MB:.0f}MB {f['origin']} {rates}")
    emit({"type": "summary", "mode": "coldhot", "run": run, "pod": idx, "time": time.time()})


def run_pool(n, fn):
    ts = [threading.Thread(target=fn, args=(w,), daemon=True) for w in range(n)]
    for t in ts:
        t.start()
    return ts


def cmd_warm(a):
    eps = endpoints_arg(a)
    idx, ip, host = pod_info()
    files = select_files(a)[idx::a.pods]
    run = a.run or time.strftime("%Y%m%d-%H%M%S")
    emit({"type": "config", "mode": "warm", "run": run, "pod": idx, "pod_ip": ip, "host": host,
          "endpoints": eps, "files": len(files), "time": time.time()})
    jobs = [(f, ep) for f in files for ep in eps]
    lock = threading.Lock()
    done = [0]

    def worker(_w):
        while True:
            with lock:
                if not jobs:
                    return
                f, ep = jobs.pop(0)
            r = transfer(cache_url(ep, f), timeout=a.timeout)
            emit({"type": "xfer", "run": run, "pod": idx, "endpoint": ep, "path": f["path"],
                  "origin": f["origin"], "size": f["size"], **r})
            with lock:
                done[0] += 1
                if r["rc"] != 0:
                    log(f"warm: rc={r['rc']} {cache_url(ep, f)} {r.get('err', '')}")
                if done[0] % 20 == 0:
                    log(f"warm: {done[0]} transfers done, {len(jobs)} left")

    for t in run_pool(a.concurrency, worker):
        t.join()
    emit({"type": "summary", "mode": "warm", "run": run, "pod": idx, "time": time.time()})


def cmd_stress(a):
    eps = endpoints_arg(a)
    idx, ip, host = pod_info()
    stages = [int(x) for x in split_list(a.stages)]
    S = a.stage_seconds
    files = select_files(a)
    if a.mode == "cold":
        files = files[idx::a.pods]  # every pod reads different files, each once
    if not files:
        raise SystemExit("no files selected")
    run = a.run or time.strftime("%Y%m%d-%H%M%S")
    start = a.start_at if a.start_at else time.time() + 5
    end = start + S * len(stages)
    emit({"type": "config", "mode": "stress", "sub": a.mode, "run": run, "pod": idx, "pods": a.pods,
          "pod_ip": ip, "host": host, "endpoints": eps, "stages": stages, "stage_seconds": S,
          "warmup_seconds": a.warmup_seconds, "start": start, "end": end, "files": len(files),
          "bytes_selected": sum(f["size"] for f in files), "time": time.time()})

    meter, procs = Meter(), Procs()
    stop = threading.Event()
    lock = threading.Lock()
    queue = list(files)

    def reporter():
        while True:
            last = stop.wait(1.0)
            for s, d in meter.drain(int(time.time()) if not last else float("inf")):
                emit({"type": "rate", "run": run, "pod": idx, "t": s, "bytes": d})
            if last:
                return

    def worker(w):
        # Streams are spread evenly over endpoints within each pod and, when a
        # pod has fewer streams than endpoints, across pods.
        ep = eps[(w + idx) % len(eps)]
        rng = random.Random(f"{a.seed}:{idx}:{w}")
        while not stop.is_set():
            now = time.time()
            k = int((now - start) // S)
            if k >= len(stages):
                return
            if now < start or w >= stages[k]:
                nxt = start if now < start else start + (k + 1) * S
                stop.wait(max(nxt - now, 0.05))
                continue
            if a.mode == "hot":
                f = rng.choice(files)
            else:
                with lock:
                    if not queue:
                        log(f"worker {w}: out of cold files")
                        emit({"type": "exhausted", "run": run, "pod": idx, "w": w, "stage": k, "t": now})
                        return
                    f = queue.pop()
            r = transfer(cache_url(ep, f), label=ep, meter=meter, procs=procs, timeout=a.timeout)
            emit({"type": "xfer", "run": run, "pod": idx, "w": w, "stage": k, "endpoint": ep,
                  "path": f["path"], "origin": f["origin"], "size": f["size"], **r})
            if r["rc"] != 0 and not r.get("aborted"):
                log(f"worker {w}: rc={r['rc']} {cache_url(ep, f)} {r.get('err', '')}")
                stop.wait(1.0)  # do not spin on a failing endpoint

    rep = threading.Thread(target=reporter, daemon=True)
    rep.start()
    workers = run_pool(max(stages), worker)
    wait_until(start, "stress start")
    for k, c in enumerate(stages):
        log(f"stage {k}: {c} streams per pod for {S}s")
        wait_until(start + (k + 1) * S, f"stage {k}")
    stop.set()
    procs.kill_all()
    for t in workers:
        t.join(30)
    rep.join(10)
    emit({"type": "summary", "mode": "stress", "run": run, "pod": idx, "time": time.time()})


# ------------------------------------------------------------------ report

def read_events(paths):
    ev = []
    for p in paths:
        with open(p, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                i = line.find("IXBENCH {")
                if i >= 0:
                    try:
                        ev.append(json.loads(line[i + 8:]))
                    except json.JSONDecodeError:
                        pass
    return ev


def pct(xs, q):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def ok(r, size=None):
    return r and r["rc"] == 0 and (size is None or r["bytes"] == size)


def rate(r):
    return r["bytes"] / MB / max(r["end"] - r["start"], 1e-6)


def telemetry_for(index, ips, paths, t0, t1):
    """ixcache telemetry records of our transfers, by path."""
    out = {}
    paths = sorted(set(paths))
    for i in range(0, max(len(paths), 1), 500):
        flt = [{"range": {"ts": {"gte": int(t0 * 1000), "lte": int(t1 * 1000), "format": "epoch_millis"}}}]
        if ips:
            flt.append({"terms": {"client_host": ips}})
        if paths:
            flt.append({"terms": {"path.raw": paths[i:i + 500]}})
        r = es_search(index, {"size": 10000, "query": {"bool": {"filter": flt}},
                              "_source": ["ts", "path", "instance", "bytes_out", "bytes_hit",
                                          "bytes_fetched", "duration_ms"]})
        for h in r["hits"]["hits"]:
            s = h["_source"]
            out.setdefault(s["path"], []).append(s)
        if not paths:
            break
    return out


def match_record(recs, end):
    best, bd = None, 15.0
    for s in recs or []:
        ts = time.mktime(time.strptime(s["ts"][:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone
        d = abs(ts - end)
        if d < bd:
            best, bd = s, d
    return best


def report_coldhot(evs, cfgs, a):
    rows = sorted(evs, key=lambda e: (e["run"], e["i"]))
    kinds = ["cold", "hot", "direct"]
    tele = {}
    if a.verify:
        ips = sorted({c["pod_ip"] for c in cfgs if c.get("pod_ip")})
        t0 = min(r["start"] for e in rows for r in e["runs"].values()) - 60
        t1 = max(r["end"] for e in rows for r in e["runs"].values()) + 300
        tele = telemetry_for(a.index, ips, [e["path"] for e in rows], t0, t1)

    print(f"{'#':>3} {'size MB':>8} {'cold MB/s':>9} {'hot MB/s':>9} {'dir MB/s':>9} {'cold/dir':>8}"
          f" {'ttfb c/h/d (s)':>18}" + ("  hit% cold/hot" if a.verify else "") + "  origin")
    stats = {k: {"rates": [], "ttfb": [], "bytes": 0, "secs": 0.0, "fail": 0, "n": 0} for k in kinds}
    slow = []
    hits = {"cold": [0, 0], "hot": [0, 0]}
    for e in rows:
        rs = e["runs"]
        cells = []
        for k in kinds:
            r = rs.get(k)
            if r is None:
                cells.append(None)
                continue
            st = stats[k]
            st["n"] += 1
            if ok(r, e["size"]):
                st["rates"].append(rate(r))
                st["ttfb"].append(r["ttfb"])
                st["bytes"] += r["bytes"]
                st["secs"] += r["end"] - r["start"]
                cells.append(r)
            else:
                st["fail"] += 1
                cells.append(False)
        c, h, d = cells
        ratio = ""
        if c and d:
            tc, td = c["end"] - c["start"], d["end"] - d["start"]
            slow.append((tc, td))
            ratio = f"{td / tc:.2f}"

        def f(x, fmt):
            return "fail" if x is False else ("-" if x is None else fmt(x))

        ttfb = "/".join(f(x, lambda r: f"{r['ttfb']:.2f}") for x in cells)
        hitcol = ""
        if a.verify:
            parts = []
            for k in ("cold", "hot"):
                r = rs.get(k)
                rec = match_record(tele.get(e["path"]), r["end"]) if r else None
                if rec and rec["bytes_out"]:
                    hits[k][0] += rec["bytes_hit"]
                    hits[k][1] += rec["bytes_out"]
                    parts.append(f"{100 * rec['bytes_hit'] / rec['bytes_out']:.0f}")
                else:
                    parts.append("?")
            hitcol = "  " + "/".join(parts).rjust(13)
        print(f"{e['i']:>3} {e['size'] / MB:>8.0f} {f(c, lambda r: f'{rate(r):.0f}'):>9}"
              f" {f(h, lambda r: f'{rate(r):.0f}'):>9} {f(d, lambda r: f'{rate(r):.0f}'):>9} {ratio:>8}"
              f" {ttfb:>18}{hitcol}  {e['origin']}")

    print()
    print(f"{'kind':<7} {'ok':>4} {'fail':>5} {'median MB/s':>12} {'p10':>7} {'p90':>7} {'aggregate MB/s':>15}"
          f" {'median ttfb s':>14}")
    for k in kinds:
        st = stats[k]
        if not st["n"]:
            continue
        agg = st["bytes"] / MB / st["secs"] if st["secs"] else float("nan")
        print(f"{k:<7} {len(st['rates']):>4} {st['fail']:>5} {pct(st['rates'], .5):>12.1f}"
              f" {pct(st['rates'], .1):>7.1f} {pct(st['rates'], .9):>7.1f} {agg:>15.1f}"
              f" {pct(st['ttfb'], .5):>14.3f}")
    if slow:
        per = [tc / td - 1 for tc, td in slow]
        tot = sum(tc for tc, _ in slow) / sum(td for _, td in slow) - 1
        print(f"\ncold through cache vs direct from origin ({len(slow)} files): "
              f"median slowdown {100 * pct(per, .5):+.1f}%, total time {100 * tot:+.1f}%")
    if a.verify:
        for k in ("cold", "hot"):
            hb, ob = hits[k]
            print(f"telemetry {k}: {100 * hb / ob:.1f}% of bytes served from cache disks" if ob
                  else f"telemetry {k}: no matching records")


def report_stress(run, cfgs, rates, xfers, exhausted, a):
    c0 = cfgs[0]
    stages, S, W = c0["stages"], c0["stage_seconds"], c0["warmup_seconds"]
    start = min(c["start"] for c in cfgs)
    pods = sorted({c["pod"] for c in cfgs})
    eps = c0["endpoints"]
    if len({round(c["start"]) for c in cfgs}) > 1:
        print("warning: pods have different start times; set --start-at for all of them")
    tot, by_ep, by_pod = {}, {}, {}
    for r in rates:
        for ep, n in r["bytes"].items():
            tot[r["t"]] = tot.get(r["t"], 0) + n
            by_ep.setdefault(ep, {})
            by_ep[ep][r["t"]] = by_ep[ep].get(r["t"], 0) + n
            by_pod.setdefault(r["pod"], {})
            by_pod[r["pod"]][r["t"]] = by_pod[r["pod"]].get(r["t"], 0) + n

    def mean(series, t0, t1):
        return sum(series.get(t, 0) for t in range(t0, t1)) / max(t1 - t0, 1)

    print(f"run {run} ({c0['sub']}): {len(pods)} pods, endpoints {', '.join(eps)}, "
          f"{S}s per stage, first {W}s of each not counted")
    print(f"{'stage':>5} {'streams':>8} {'GB/s':>7} {'Gbit/s':>7} {'peak10s Gb/s':>12} {'xfers':>6} {'fail':>5}"
          f" {'MB/s/stream':>11} {'pod Gb/s min-max':>17}")
    best = None
    for k, c in enumerate(stages):
        t0, t1 = int(start + k * S + W), int(start + (k + 1) * S)
        m = mean(tot, t0, t1)
        n = max(min(10, t1 - t0), 1)
        win = [sum(tot.get(t, 0) for t in range(s, s + n)) / n for s in range(t0, max(t1 - n + 1, t0 + 1))]
        xs = [x for x in xfers if x.get("stage") == k]
        fails = sum(1 for x in xs if x["rc"] != 0 and not x.get("aborted"))
        done = sum(1 for x in xs if x["rc"] == 0)
        streams = c * len(pods)
        podr = [mean(by_pod.get(p, {}), t0, t1) * 8 / 1e9 for p in pods]
        print(f"{k:>5} {streams:>8} {m / 1e9:>7.2f} {m * 8 / 1e9:>7.1f} {max(win) * 8 / 1e9:>12.1f}"
              f" {done:>6} {fails:>5} {m / MB / streams:>11.1f} {min(podr):>8.1f}-{max(podr):<8.1f}")
        if best is None or m > best[1]:
            best = (k, m)
        if a.per_endpoint and len(eps) > 1:
            for ep in eps:
                print(f"{'':>14}{mean(by_ep.get(ep, {}), t0, t1) * 8 / 1e9:>7.1f} Gbit/s  {ep}")
    k, m = best
    print(f"\nmaximum combined throughput: {m / 1e9:.2f} GB/s = {m * 8 / 1e9:.1f} Gbit/s "
          f"(stage {k}, {stages[k] * len(pods)} streams)")
    if exhausted:
        first = min(e["stage"] for e in exhausted)
        print(f"WARNING: {len({e['pod'] for e in exhausted})} pod(s) ran out of cold files, first in stage "
              f"{first}: from there on fewer streams ran than listed. Use a larger --count.")
    aborted = sum(1 for x in xfers if x.get("aborted"))
    if aborted:
        print(f"{aborted} transfers were still running at the end and were stopped (their bytes are counted)")
    errs = {}
    for x in xfers:
        if x["rc"] != 0 and not x.get("aborted"):
            e = (x.get("err") or f"rc={x['rc']}").splitlines()[-1][:120]
            errs[e] = errs.get(e, 0) + 1
    for e, n in sorted(errs.items(), key=lambda kv: -kv[1])[:10]:
        print(f"  {n:>5} x {e}")

    if a.verify:
        ips = sorted({c["pod_ip"] for c in cfgs if c.get("pod_ip")})
        end = max(c["end"] for c in cfgs)
        r = es_search(a.index, {
            "size": 0,
            "query": {"bool": {"filter": [
                {"range": {"ts": {"gte": int(start * 1000), "lte": int((end + 120) * 1000),
                                  "format": "epoch_millis"}}},
                {"terms": {"client_host": ips}}]}},
            "aggs": {"i": {"terms": {"field": "instance", "size": 50}, "aggs": {
                "out": {"sum": {"field": "bytes_out"}}, "hit": {"sum": {"field": "bytes_hit"}},
                "fetched": {"sum": {"field": "bytes_fetched"}}}}}})
        print("\nixcache telemetry for these clients:")
        for b in r["aggregations"]["i"]["buckets"]:
            o, h, f = b["out"]["value"], b["hit"]["value"], b["fetched"]["value"]
            print(f"  {b['key']}: {b['doc_count']} files, {o / 1e12:.2f} TB out, "
                  f"{100 * h / o if o else 0:.1f}% hit, {f / 1e12:.2f} TB fetched from origins")
        if not r["aggregations"]["i"]["buckets"]:
            print("  none found (client IPs may be NATed, or telemetry not yet indexed)")

    if a.timeseries:
        with open(a.timeseries, "w", newline="") as fh:
            wr = csv.writer(fh)
            wr.writerow(["t", "stage", "total_bytes"] + eps)
            for t in range(int(start) + 1, int(start + S * len(stages))):
                wr.writerow([t, int((t - start) // S), tot.get(t, 0)] + [by_ep.get(ep, {}).get(t, 0) for ep in eps])
        print(f"\nper-second series written to {a.timeseries}")


def cmd_report(a):
    ev = read_events(a.logs)
    if not ev:
        raise SystemExit("no IXBENCH records found")
    runs = {}
    for e in ev:
        runs.setdefault(e.get("run"), []).append(e)
    for run, es in sorted(runs.items(), key=lambda kv: str(kv[0])):
        cfgs = [e for e in es if e["type"] == "config"]
        mode = cfgs[0]["mode"] if cfgs else "?"
        print(f"=== {mode} run {run}")
        if mode == "coldhot":
            report_coldhot([e for e in es if e["type"] == "coldhot"], cfgs, a)
        elif mode == "stress":
            report_stress(run, cfgs, [e for e in es if e["type"] == "rate"],
                          [e for e in es if e["type"] == "xfer"],
                          [e for e in es if e["type"] == "exhausted"], a)
        elif mode == "warm":
            xs = [e for e in es if e["type"] == "xfer"]
            good = [x for x in xs if ok(x, x["size"])]
            print(f"{len(good)}/{len(xs)} transfers ok, {sum(x['bytes'] for x in good) / 1e9:.1f} GB")
            for x in xs:
                if not ok(x, x["size"]):
                    print(f"  failed: {x['endpoint']} {x['origin']}{x['path']} {x.get('err', '')[-200:]}")
        print()


# ------------------------------------------------------------------ main

def main():
    ap = argparse.ArgumentParser(prog="ixbench", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("select", help="print the selected files")
    add_selection_args(p)
    p.add_argument("--out", help="write here instead of stdout")
    p.set_defaults(fn=cmd_select)

    def common(p):
        p.add_argument("--endpoints", help="cache host:port list (default $ENDPOINTS)")
        p.add_argument("--run", help="run label (default: start time)")
        p.add_argument("--timeout", type=int, default=3600, help="per-transfer timeout, s")
        add_selection_args(p)

    p = sub.add_parser("coldhot", help="cold, direct and hot read of each file, one at a time")
    common(p)
    p.add_argument("--direct", action="store_true", help="also read each file directly from its origin (needs a proxy)")
    p.add_argument("--hot-delay", type=float, default=5.0,
                   help="seconds between the cold and the hot read (lets ixcache finish writing)")
    p.add_argument("--hot-repeats", type=int, default=1)
    p.set_defaults(fn=cmd_coldhot)

    p = sub.add_parser("warm", help="read the selected files through every endpoint")
    common(p)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--pods", type=int, default=int(os.environ.get("NPODS", "1")),
                   help="pods sharing the work (index from JOB_COMPLETION_INDEX)")
    p.set_defaults(fn=cmd_warm)

    p = sub.add_parser("stress", help="parallel streams in stages of increasing concurrency")
    common(p)
    p.add_argument("--mode", choices=["hot", "cold"], default="hot",
                   help="hot: random reads from the (warmed) set; cold: every file read once")
    p.add_argument("--stages", default="1,2,4,8,16,32", help="streams per pod in each stage")
    p.add_argument("--stage-seconds", type=int, default=180)
    p.add_argument("--warmup-seconds", type=int, default=30, help="start of each stage left out of the mean")
    p.add_argument("--start-at", type=float, default=0, help="epoch seconds; the same for all pods")
    p.add_argument("--pods", type=int, default=int(os.environ.get("NPODS", "1")),
                   help="pods in the test (index from JOB_COMPLETION_INDEX)")
    p.set_defaults(fn=cmd_stress)

    p = sub.add_parser("report", help="summarise ixbench logs")
    p.add_argument("logs", nargs="+")
    p.add_argument("--verify", action="store_true",
                   help="match against ixcache telemetry in Elasticsearch (hit ratios); needs ES_URL")
    p.add_argument("--index", default=os.environ.get("ES_INDEX", "ixcache-*"))
    p.add_argument("--per-endpoint", action="store_true")
    p.add_argument("--timeseries", help="write the stress per-second series to this CSV")
    p.set_defaults(fn=cmd_report)

    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
