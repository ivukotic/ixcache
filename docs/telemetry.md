# Transfer telemetry

ixcache sends one UDP datagram per client open of a file, when the client
closes it or its connection ends. Each datagram is one JSON object of at most
1200 bytes, in UTF-8. The destination is `IXCACHE_TELEMETRY` (`host:port`); the
deployment uses `ixcache.atlas-ml.org:1095`.

Delivery is best effort: records are queued and sent by one background thread,
and are dropped (and counted) if the queue is full or a send fails. A transfer
is never slowed or failed by telemetry.

## Schema v1

```json
{
  "v": 1,
  "ts": "2026-10-20T12:34:56.789Z",
  "site": "MWT2",
  "instance": "xcache-uc-1",
  "client_host": "192.170.240.106",
  "client_dn": "",
  "origin": "xrootd.aglt2.org:1094",
  "path": "/atlaslocalgroupdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat",
  "cache_key": "rucio:user.ivukotic:xcache.test.dat",
  "file_size": 1048576,
  "bytes_out": 1048576,
  "bytes_hit": 0,
  "bytes_fetched": 1048576,
  "read_ops": 1,
  "readv_ops": 0,
  "readv_elements": 0,
  "errors": 0,
  "duration_ms": 71,
  "status": "closed"
}
```

Fields always appear, in this order. Strings are never null (empty when unknown).

| Field | Type | Meaning |
| --- | --- | --- |
| `v` | integer | Schema version, `1`. |
| `ts` | string, ISO 8601 UTC, ms | When the record was sent (the file's close). |
| `site` | string | `IXCACHE_SITE`. |
| `instance` | string | `IXCACHE_INSTANCE`. |
| `client_host` | string | Client IP address (IPv4 or IPv6). |
| `client_dn` | string | Client certificate DN with `IXCACHE_AUTH=gsi`; empty otherwise. |
| `origin` | string | Origin `host:port` from the client URL. |
| `path` | string | Path at the origin. |
| `cache_key` | string | `rucio:<scope>:<name>` for Rucio-layout paths, else `path:<host>:<port><path>`. |
| `file_size` | integer | File size, bytes. |
| `bytes_out` | integer | Bytes sent to the client. |
| `bytes_hit` | integer | Bytes served from the cache disks. |
| `bytes_fetched` | integer | Bytes requested from the origin; whole cache blocks, so can exceed what the client asked for. |
| `read_ops` | integer | Plain read requests. |
| `readv_ops` | integer | Vector read requests. |
| `readv_elements` | integer | Ranges in all vector reads. |
| `errors` | integer | Read requests that failed. |
| `duration_ms` | integer | Time from open to close. |
| `status` | string | `closed` (client closed the file) or `disconnected` (connection ended with the file open). |

When a record would exceed 1200 bytes, `path`, then `cache_key`, `client_dn`
and `origin` are shortened, ending in `...`. Numbers are never dropped.

Useful derived values: hit ratio `bytes_hit / bytes_out`; origin read
amplification `bytes_fetched / (bytes_out - bytes_hit)`; client throughput
`bytes_out / duration_ms`.

## Collecting with Logstash

```
input {
  udp { port => 1095 codec => json }
}
filter {
  date { match => ["ts", "ISO8601"] target => "@timestamp" }
}
output {
  elasticsearch { hosts => ["https://..."] index => "ixcache-transfers-%{+YYYY.MM}" }
}
```

Suggested Elasticsearch types: `keyword` for `site`, `instance`,
`client_host` (or `ip`), `client_dn`, `origin`, `path`, `cache_key`, `status`;
`long` for the counters and sizes; `date` for `ts`.
