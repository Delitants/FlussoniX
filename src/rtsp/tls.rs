//! TLS material is validated before admission or worker startup.
use std::{
    io::{Error, ErrorKind, Read},
    path::Path,
    sync::Arc,
};
use tokio_rustls::rustls::{
    self,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
pub(crate) fn pem(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024 {
        return Err(Error::new(ErrorKind::InvalidData, "TLS PEM exceeds 1 MiB"));
    }
    Ok(bytes)
}
pub fn server(cert: &Path, key: &Path) -> std::io::Result<Arc<rustls::ServerConfig>> {
    let certs = CertificateDer::pem_slice_iter(&pem(cert)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Error::other)?;
    if certs.is_empty() || certs.len() > 16 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "TLS certificate chain requires 1..16 certificates",
        ));
    }
    let key = PrivateKeyDer::from_pem_slice(&pem(key)?).map_err(Error::other)?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(Error::other)?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(Error::other)?;
    Ok(Arc::new(config))
}
