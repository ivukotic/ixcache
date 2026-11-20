# syntax=docker/dockerfile:1
# ixbench: xrdcp-based benchmark client for ixcache (see docs/benchmarking.md).
FROM almalinux:9

RUN dnf -y install epel-release \
 && dnf -y install xrootd-client python3 \
 && dnf clean all \
 && groupadd --gid 10940 ixbench \
 && useradd --uid 10940 --gid ixbench --create-home ixbench \
 && mkdir -p /etc/grid-security/certificates

COPY --chmod=0755 ixbench.py /usr/local/bin/ixbench

# X509_USER_PROXY is set only where a proxy is mounted (coldhot.yaml, for
# --direct reads from origins); the cache itself needs no credentials.
ENV X509_CERT_DIR=/etc/grid-security/certificates \
    PYTHONUNBUFFERED=1

USER 10940:10940
WORKDIR /home/ixbench
ENTRYPOINT ["ixbench"]
CMD ["--help"]
