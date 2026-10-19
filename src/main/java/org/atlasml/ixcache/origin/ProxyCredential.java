package org.atlasml.ixcache.origin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;

import eu.emi.security.authn.x509.impl.PEMCredential;
import org.dcache.xrootd.plugins.authn.gsi.SerializableX509Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The service proxy used to authenticate to origins. It is re-read whenever the
 * file changes, so a renewed proxy (written by the x509 sidecar) is picked up by
 * the next connection. The proxy itself is never logged.
 */
final class ProxyCredential {

    private static final Logger log = LoggerFactory.getLogger(ProxyCredential.class);

    private final Path path;
    private FileTime loadedMtime;
    private SerializableX509Credential current;

    ProxyCredential(Path path) {
        this.path = path;
    }

    /** The proxy as of now, reloaded if the file changed. */
    synchronized SerializableX509Credential get() throws IOException {
        FileTime mtime;
        try {
            mtime = Files.getLastModifiedTime(path);
        } catch (NoSuchFileException e) {
            throw new IOException("service proxy " + path + " does not exist", e);
        }
        if (current == null || !mtime.equals(loadedMtime)) {
            try {
                PEMCredential pem = new PEMCredential(path.toString(), (char[]) null);
                current = new SerializableX509Credential(pem.getCertificateChain(), pem.getKey());
                loadedMtime = mtime;
                log.info("loaded service proxy {} (subject {}, expires {})", path,
                        pem.getCertificate().getSubjectX500Principal().getName(),
                        pem.getCertificate().getNotAfter().toInstant());
            } catch (GeneralSecurityException e) {
                throw new IOException("cannot load proxy " + path + ": " + e.getMessage(), e);
            }
        }
        return current;
    }
}
