package org.atlasml.ixcache.server;

/**
 * File metadata returned to clients by stat and open.
 *
 * @param size    file size in bytes
 * @param modTime modification time, seconds since the epoch
 */
public record FileInfo(long size, long modTime) {
}
