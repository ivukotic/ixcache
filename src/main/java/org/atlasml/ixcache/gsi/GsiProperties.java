package org.atlasml.ixcache.gsi;

import java.nio.file.Path;
import java.util.Properties;

/** The settings xrootd4j's GSI classes read, for the server and the origin client. */
public final class GsiProperties {

    private GsiProperties() {
    }

    /**
     * @param caDir    trusted CA certificates and CRLs
     * @param hostCert host certificate (server side only; may be null)
     * @param hostKey  key for {@code hostCert} (server side only; may be null)
     */
    public static Properties of(Path caDir, Path hostCert, Path hostKey) {
        Properties p = new Properties();
        if (hostCert != null) {
            p.setProperty("xrootd.gsi.hostcert.cert", hostCert.toString());
            p.setProperty("xrootd.gsi.hostcert.key", hostKey.toString());
        }
        p.setProperty("xrootd.gsi.hostcert.refresh", "1");
        p.setProperty("xrootd.gsi.hostcert.refresh.unit", "HOURS");
        p.setProperty("xrootd.gsi.hostcert.verify", "true");
        p.setProperty("xrootd.gsi.ca.path", caDir.toString());
        p.setProperty("xrootd.gsi.ca.refresh", "1");
        p.setProperty("xrootd.gsi.ca.refresh.unit", "HOURS");
        p.setProperty("xrootd.gsi.ca.namespace-mode", "EUGRIDPMA_GLOBUS");
        p.setProperty("xrootd.gsi.ca.crl-mode", "IF_VALID");
        p.setProperty("xrootd.gsi.ca.ocsp-mode", "IGNORE");
        // xrootd4j's third-party-copy client settings; ixcache supplies the proxy itself.
        p.setProperty("xrootd.gsi.tpc.cred.refresh", "1");
        p.setProperty("xrootd.gsi.tpc.cred.refresh.unit", "HOURS");
        p.setProperty("xrootd.gsi.tpc.cred.verify", "false");
        return p;
    }
}
