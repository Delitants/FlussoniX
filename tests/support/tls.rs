#![allow(dead_code)]
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
use tokio_rustls::rustls::{
    self,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
pub struct Certificates {
    pub dir: tempfile::TempDir,
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}
fn openssl(dir: &Path, args: &[&str]) {
    let output = Command::new("openssl")
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "owned certificate generation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
impl Certificates {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        openssl(
            p,
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "ca.key",
                "-out",
                "ca.pem",
                "-days",
                "2",
                "-subj",
                "/CN=FlussoniX Owned Test CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
            ],
        );
        openssl(
            p,
            &[
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "server.key",
                "-out",
                "server.csr",
                "-subj",
                "/CN=localhost",
            ],
        );
        std::fs::write(p.join("extensions"),"subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n").unwrap();
        openssl(
            p,
            &[
                "x509",
                "-req",
                "-in",
                "server.csr",
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                "server.pem",
                "-days",
                "1",
                "-extfile",
                "extensions",
            ],
        );
        Self {
            ca: p.join("ca.pem"),
            cert: p.join("server.pem"),
            key: p.join("server.key"),
            dir,
        }
    }
    pub fn expire(&self) {
        let p = self.dir.path();
        std::fs::create_dir(p.join("issued")).unwrap();
        std::fs::write(p.join("index.txt"), "").unwrap();
        std::fs::write(p.join("serial"), "01\n").unwrap();
        std::fs::write(p.join("ca.cnf"), "[ca]\ndefault_ca=owned\n[owned]\ndatabase=index.txt\nnew_certs_dir=issued\ncertificate=ca.pem\nprivate_key=ca.key\nserial=serial\ndefault_md=sha256\npolicy=policy\nx509_extensions=server\n[policy]\ncommonName=supplied\n[server]\nsubjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n").unwrap();
        openssl(
            p,
            &[
                "ca",
                "-batch",
                "-config",
                "ca.cnf",
                "-startdate",
                "200101000000Z",
                "-enddate",
                "210101000000Z",
                "-in",
                "server.csr",
                "-out",
                "server.pem",
                "-notext",
            ],
        );
    }
    pub fn server(&self) -> Arc<rustls::ServerConfig> {
        let certs = CertificateDer::pem_file_iter(&self.cert)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_file(&self.key).unwrap();
        Arc::new(
            rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
        )
    }
    pub fn client(&self) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(&self.ca).unwrap() {
            roots.add(cert.unwrap()).unwrap();
        }
        Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
        )
    }
}
