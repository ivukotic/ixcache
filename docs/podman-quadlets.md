# Deploying on a node with Podman Quadlets

This guide runs ixcache on a single node as systemd services managed by
[Podman Quadlets](https://docs.podman.io/en/latest/markdown/podman-systemd.unit.5.html).
It reproduces the Kubernetes deployment in [deploy/deployment.yaml](../deploy/deployment.yaml)
without a cluster: the same images, the same three containers, the same
environment.

| Unit | Container | Role |
| --- | --- | --- |
| `ixcache-x509.service` | `atlas-xcache-heartbeats` running `run_x509_updater.sh` | Keeps a fresh service proxy in `/etc/proxy/x509up`, refreshes IGTF CA certificates and CRLs |
| `ixcache.service` | `ivukotic/ixcache` | The cache server, on the host network |
| `ixcache-heartbeats.service` | `atlas-xcache-heartbeats` running `run_heartbeats.sh` | Rucio heartbeat (optional) |
| `ixcache-ready.timer` | none (host `curl`) | Polls `/readyz`, which sends the VPS heartbeat (optional) |

All containers use the host network, as in Kubernetes (`hostNetwork: true`).
Clients connect to `root://<node>:1094//root://<origin>//<path>`.

## Requirements

- Podman 5.0 or newer (for `Notify=healthy`). EL 9.5+ and Fedora 40+ ship it.
- Run the units **rootful** (files in `/etc/containers/systemd/`). The x509
  updater runs as root inside its container and chowns the proxy to uid 10940.
- One file system per cache disk, mounted on the host (this guide uses
  `/xcache/1`, `/xcache/2`, …).
- A service (robot) certificate and key for the x509 updater.
- Outbound access to the origins, to `vps.cern.ch` (VPS heartbeat), to Rucio
  (Rucio heartbeat), and UDP to `ixcache.atlas-ml.org:1095` (telemetry).

## 1. Prepare the host

### Cache disks

ixcache runs as uid/gid 10940. Give it one subdirectory on each disk, so that
nothing else on the disk is counted or touched:

```sh
for d in /xcache/*/; do
  mkdir -p "$d/ixcache"
  chown 10940:10940 "$d/ixcache"
done
```

On SELinux hosts, label the cache directories once, while they are still
empty (a `:z` mount option would relabel every cached file on each start):

```sh
semanage fcontext -a -t container_file_t '/xcache(/.*)?'
restorecon -R /xcache
```

### Service certificate

```sh
install -d -m 0755 /etc/ixcache/grid-certs
install -m 0644 usercert.pem /etc/ixcache/grid-certs/usercert.pem
install -m 0400 userkey.pem  /etc/ixcache/grid-certs/userkey.pem
```

The key must stay `0400` (or `0600`): `voms-proxy-init` rejects anything
looser.

### Health probe

The Podman health checks run inside the ixcache container. This script needs
only bash, so it does not depend on what else the image contains:

```sh
cat > /etc/ixcache/probe.sh <<'EOF'
#!/bin/bash
# Usage: probe.sh /healthz | /readyz. Exits 0 when ixcache answers 200.
exec 3<>"/dev/tcp/127.0.0.1/${IXCACHE_HEALTH_PORT:-8092}" || exit 1
printf 'GET %s HTTP/1.0\r\nHost: localhost\r\n\r\n' "${1:-/healthz}" >&3
read -r -t 5 _ code _ <&3
[ "$code" = 200 ]
EOF
chmod 0755 /etc/ixcache/probe.sh
```

### Site settings

Put the settings that differ between sites in one environment file. The
values below are the RHUL ones from the Kubernetes deployment; replace them.

```sh
cat > /etc/ixcache/ixcache.env <<'EOF'
# Standard XRootD port (ixcache's default is 1092).
IXCACHE_PORT=1094
IXCACHE_HEALTH_PORT=8092

# Labels for telemetry and heartbeats.
IXCACHE_SITE=RHUL
IXCACHE_INSTANCE=xcache-1
# Address published in the VPS heartbeat: this node's IP or FQDN.
IXCACHE_ADDRESS=192.0.2.10:1094

# One UDP JSON record per closed file (see docs/telemetry.md).
IXCACHE_TELEMETRY=ixcache.atlas-ml.org:1095
# VPS heartbeat, sent at most once a minute when /readyz passes.
IXCACHE_HEARTBEAT_URL=https://vps.cern.ch/liveness

# No client authentication, like the native XCaches. See "GSI" below.
IXCACHE_AUTH=none
EOF
```

`IXCACHE_CACHE_DIR` is not set here: it must match the disk mounts, so step 3
generates both together.

## 2. Quadlet units

All files go in `/etc/containers/systemd/`.

### Shared volumes

`ixcache-proxy.volume` holds the proxy in memory, like the Kubernetes
`emptyDir` with `medium: Memory`:

```ini
[Volume]
VolumeName=ixcache-proxy
Type=tmpfs
Device=tmpfs
Options=size=10m,mode=0755
```

`ixcache-grid-security.volume` holds the CA certificates and CRLs that the x509
updater keeps current:

```ini
[Volume]
VolumeName=ixcache-grid-security
```

### x509 updater

`ixcache-x509.container`:

```ini
[Unit]
Description=ixcache service proxy, CA certificates and CRLs

[Container]
ContainerName=ixcache-x509
Image=harbor.tempest.uchicago.edu/docker.io/ivukotic/atlas-xcache-heartbeats:2026.10.18
Exec=/usr/local/sbin/run_x509_updater.sh
User=0
Group=0
Network=host
Volume=ixcache-proxy.volume:/etc/proxy
Volume=ixcache-grid-security.volume:/etc/CA
Volume=/etc/ixcache/grid-certs:/etc/grid-certs:ro,z
PodmanArgs=--memory=2g --cpus=0.5

# The unit becomes active only once the first proxy and CA directory exist,
# so ixcache.service (ordered after it) starts with credentials in place.
HealthCmd=sh -c 'test -d /etc/CA/certificates && test -s /etc/proxy/x509up'
HealthInterval=5s
HealthRetries=1
Notify=healthy

[Service]
Restart=always
# fetch-crl first sleeps up to 6 minutes, then takes a few minutes to download.
TimeoutStartSec=15min

[Install]
WantedBy=multi-user.target
```

### ixcache

`ixcache.container`:

```ini
[Unit]
Description=ixcache XRootD caching proxy
Wants=ixcache-x509.service network-online.target
After=ixcache-x509.service network-online.target

[Container]
ContainerName=ixcache
Image=docker.io/ivukotic/ixcache:latest
AutoUpdate=registry
Network=host
EnvironmentFile=/etc/ixcache/ixcache.env
Environment=X509_USER_PROXY=/etc/proxy/x509up
Environment=X509_CERT_DIR=/etc/grid-security/certificates
# Heap is half the container memory limit below: 16 GiB heap + 8 GiB direct
# memory, leaving room for the page cache of writes to the cache disks.
Environment="JDK_JAVA_OPTIONS=-XX:MaxRAMPercentage=50 -XX:MaxDirectMemorySize=8g -Djdk.nio.maxCachedBufferSize=1048576 -XX:+UseZGC -XX:+ExitOnOutOfMemoryError"
PodmanArgs=--memory=32g --cpus=32

Volume=ixcache-proxy.volume:/etc/proxy:ro
Volume=ixcache-grid-security.volume:/etc/grid-security:ro
Volume=/etc/ixcache/probe.sh:/usr/local/bin/probe.sh:ro,z
# Cache disks and IXCACHE_CACHE_DIR: see ixcache.container.d/disks.conf.

ReadOnly=true
ReadOnlyTmpfs=true
DropCapability=ALL
NoNewPrivileges=true
StopTimeout=120

# Liveness: XRootD port open and network threads responsive. A container that
# keeps failing is killed and systemd restarts it.
HealthCmd=/usr/local/bin/probe.sh /healthz
HealthStartPeriod=5m
HealthInterval=20s
HealthTimeout=5s
HealthRetries=3
HealthOnFailure=kill

[Service]
Restart=always
TimeoutStartSec=5min
# Allow in-flight transfers to finish and metadata to be flushed.
TimeoutStopSec=150

[Install]
WantedBy=multi-user.target
```

`Wants=` rather than `Requires=` on the x509 unit is deliberate: restarting the
updater should not restart the cache. ixcache reloads the proxy file whenever
it changes.

Always set a memory limit. The JVM sizes its heap from it
(`MaxRAMPercentage=50`); without one it would size the heap from the whole
node's RAM.

### Rucio heartbeat (optional)

`ixcache-heartbeats.container`. Every `FREQUENCY` seconds it asks ixcache for
its site name (`xrdfs query config sitename`) and, if it matches
`XC_RESOURCENAME`, posts a heartbeat to Rucio. The size it reports is the `df`
total of `/xcache/data_*`, so it mounts the cache disks too (step 3).

```ini
[Unit]
Description=ixcache Rucio heartbeat
Wants=ixcache.service
After=ixcache.service

[Container]
ContainerName=ixcache-heartbeats
Image=harbor.tempest.uchicago.edu/docker.io/ivukotic/atlas-xcache-heartbeats:2026.10.18
Exec=/usr/local/sbin/run_heartbeats.sh
User=0
Group=0
Network=host
Environment=XC_RESOURCENAME=RHUL
Environment=XC_INSTANCE=xcache-1
Environment=ADDRESS=192.0.2.10:1094
Environment=FREQUENCY=60
Volume=ixcache-proxy.volume:/etc/proxy:ro
Volume=/etc/ixcache/grid-certs:/etc/grid-certs:ro,z
PodmanArgs=--memory=512m --cpus=0.5
# Cache disks: see ixcache-heartbeats.container.d/disks.conf.

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
```

`XC_RESOURCENAME`, `XC_INSTANCE` and `ADDRESS` must match `IXCACHE_SITE`,
`IXCACHE_INSTANCE` and `IXCACHE_ADDRESS`.

## 3. Cache disks

Each host disk `/xcache/N` is mounted at `/data-N` in ixcache (cache directory
`/data-N/ixcache`) and read-only at `/xcache/data_N` in the heartbeat
container. With 20+ disks, generate the drop-ins instead of writing them by
hand:

```sh
cd /etc/containers/systemd
mkdir -p ixcache.container.d ixcache-heartbeats.container.d

dirs=()
{
  echo '[Container]'
  for d in /xcache/*/; do
    n=$(basename "$d")
    echo "Volume=/xcache/$n:/data-$n"
    dirs+=("/data-$n/ixcache")
  done
  echo "Environment=IXCACHE_CACHE_DIR=$(IFS=,; echo "${dirs[*]}")"
} > ixcache.container.d/disks.conf

{
  echo '[Container]'
  for d in /xcache/*/; do
    n=$(basename "$d")
    echo "Volume=/xcache/$n:/xcache/data_$n:ro"
  done
} > ixcache-heartbeats.container.d/disks.conf
```

Rerun it after adding or removing a disk, then restart the services. Leave a
failed disk out (the Kubernetes deployment skips `/xcache/18`). Each
directory's size defaults to its file system; set `IXCACHE_CACHE_SIZE` only if
the directories share one.

## 4. Readiness poll and VPS heartbeat (optional)

The VPS heartbeat is sent when `/readyz` passes, which in Kubernetes happens
every 10 s via the readiness probe. Podman has no separate readiness probe, so
poll it from a host timer. Skip this if `IXCACHE_HEARTBEAT_URL` is unset.

`/etc/systemd/system/ixcache-ready.service`:

```ini
[Unit]
Description=Poll ixcache /readyz (sends the VPS heartbeat)
After=ixcache.service

[Service]
Type=oneshot
ExecStart=/usr/bin/curl -fsS -o /dev/null -m 5 http://127.0.0.1:8092/readyz
```

`/etc/systemd/system/ixcache-ready.timer`:

```ini
[Unit]
Description=Poll ixcache /readyz every 30 s

[Timer]
OnActiveSec=1min
OnUnitActiveSec=30s
AccuracySec=1s

[Install]
WantedBy=timers.target
```

A failing `/readyz` (no writable cache disk, no valid proxy, no CA
certificates) shows up as failed runs of `ixcache-ready.service`, and the
reasons are in ixcache's log.

## 5. Start

Check that Quadlet understands the files, then start everything:

```sh
/usr/lib/systemd/system-generators/podman-system-generator --dryrun > /dev/null
systemctl daemon-reload
systemctl start ixcache-x509.service       # may take up to ~15 minutes the first time
systemctl start ixcache.service ixcache-heartbeats.service
systemctl enable --now ixcache-ready.timer
```

Quadlet-generated services cannot be `systemctl enable`d; the `[Install]`
sections above start them at boot.

Open the XRootD port in the firewall. The health port (8092) only needs to be
reachable from the host itself and from your monitoring:

```sh
firewall-cmd --permanent --add-port=1094/tcp && firewall-cmd --reload
```

## 6. Check it

```sh
systemctl status ixcache-x509 ixcache ixcache-heartbeats
journalctl -u ixcache -f
curl -s http://127.0.0.1:8092/readyz          # JSON report of each check
podman healthcheck run ixcache && echo healthy
xrdcp -f root://$(hostname -f):1094//root://xrootd.aglt2.org:1094//atlaslocalgroupdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat /dev/null
```

`/readyz` lists expired or unreadable CRLs as a warning without failing.

## Operations

- **Upgrade.** `ixcache.container` has `AutoUpdate=registry`: enable
  `podman-auto-update.timer` to pull a new `latest` daily and restart, or run
  `podman auto-update` by hand. To pin a version, use a tag such as
  `docker.io/ivukotic/ixcache:1.2` and drop `AutoUpdate`.
- **Restart.** `systemctl restart ixcache` stops the server with a grace period
  of 120 s for in-flight transfers and the metadata flush. The cache survives
  restarts.
- **Memory diagnosis.** Add `-XX:NativeMemoryTracking=summary` to
  `JDK_JAVA_OPTIONS` and run `podman exec ixcache jcmd 1 VM.native_memory summary`.
- **Changing settings.** Edit `/etc/ixcache/ixcache.env` and
  `systemctl restart ixcache`. After editing a Quadlet file or drop-in, run
  `systemctl daemon-reload` first.

## GSI client authentication

By default (`IXCACHE_AUTH=none`) clients are not authenticated. For GSI,
the server needs a host certificate whose name matches the node (the robot
certificate will not do):

```sh
install -d -m 0755 /etc/ixcache/hostcert
install -m 0644 hostcert.pem /etc/ixcache/hostcert/hostcert.pem
install -m 0400 -o 10940 -g 10940 hostkey.pem /etc/ixcache/hostcert/hostkey.pem
```

Add to `ixcache.container`:

```ini
Volume=/etc/ixcache/hostcert:/etc/ixcache/hostcert:ro,z
Environment=IXCACHE_AUTH=gsi
Environment=IXCACHE_HOSTCERT=/etc/ixcache/hostcert/hostcert.pem
Environment=IXCACHE_HOSTKEY=/etc/ixcache/hostcert/hostkey.pem
```

and remove `IXCACHE_AUTH=none` from the environment file.

## Environment reference

| Variable | Default | Meaning |
| --- | --- | --- |
| `IXCACHE_PORT` | `1092` | XRootD port |
| `IXCACHE_HEALTH_PORT` | `8092` | HTTP port for `/healthz` and `/readyz` |
| `IXCACHE_CACHE_DIR` | unset (no caching) | Comma-separated cache directories, one per disk |
| `IXCACHE_CACHE_SIZE` | file system size | Size per directory (bytes, or with K/M/G/T) |
| `IXCACHE_HIGH_WM` / `IXCACHE_LOW_WM` | `0.95` / `0.90` | Eviction watermarks, fractions of the size |
| `IXCACHE_BLOCK_SIZE` | `64K` | Cache block size |
| `IXCACHE_ORIGIN_TIMEOUT` | `120` | Origin request timeout, seconds |
| `IXCACHE_AUTH` | `none` | `none` or `gsi` |
| `IXCACHE_HOSTCERT` / `IXCACHE_HOSTKEY` | `/etc/grid-security/hostcert.pem` / `hostkey.pem` | Host certificate for GSI |
| `X509_USER_PROXY` | `/etc/proxy/x509up` (image) | Service proxy used towards origins; reloaded when it changes |
| `X509_CERT_DIR` | `/etc/grid-security/certificates` | Trusted CAs and CRLs |
| `IXCACHE_SITE` / `IXCACHE_INSTANCE` | unset | Labels for telemetry, heartbeats and `query config sitename` |
| `IXCACHE_TELEMETRY` | unset (off) | `host:port` for UDP transfer records |
| `IXCACHE_HEARTBEAT_URL` | unset (off) | VPS heartbeat URL; then `IXCACHE_SITE`, `IXCACHE_INSTANCE` and `IXCACHE_ADDRESS` are required |
| `IXCACHE_HEARTBEAT_INTERVAL` | `60` | Minimum seconds between VPS heartbeats |
| `IXCACHE_ADDRESS` | unset | `host:port` published in the VPS heartbeat |
