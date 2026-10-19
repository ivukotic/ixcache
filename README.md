# ixcache

AI reimplementation of xcache

In HEP we often use xrootd protocol for data transfers. Often the same protocol is used to read the data. These reads are relatively spars, reading between 5% and 30% of the file. Often the data is read over large distances (large latency). Data is not very frequently re-read (cache hit rate is often 2-3). This make is hard to do very efficient caching. Access authorization is mediated by x509 proxy.

This is a Java implementation of the subfile caching system. It will be a single node cache, with NVMes in the node. We can't afford more than 20% slowdown on the first read. It has to be able to serve 1000 concurent clients, but the node can have a very beffy CPU.

It can expect user's x509 proxy file path in an evn variable X509_USER_PROXY.

There are two main clients used by the comunity:
xrdcp - copies a full file
root - used for data analysis so reading only parts of files.

To test access of the data access from origin user can do:

xrdcp -f root://xrootd.aglt2.org:1094//atlaslocalgroupdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat /dev/null

while copying the same data through ixcache would be done like:
xrdcp -f root://<ixcache_host>:1092//root://xrootd.aglt2.org:1094//atlaslocalgroupdisk/rucio/user/ivukotic/7d/9b/xcache.test.dat /dev/null

Logging is done by sendind a UDP packet to (ixcache.atlas-ml.org:1095) describing the transfer (new bytes in, bytes out, duration, original file path).

Some references:
xrootd project: https://xrootd.org/
xrootd protocol: https://xrootd.web.cern.ch/doc/dev6/XRdv520.html
root project: https://root.cern.ch/doc/v640/