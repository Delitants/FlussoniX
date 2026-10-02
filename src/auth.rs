use base64::{Engine, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Role {
    View,
    Edit,
}
pub struct Credentials {
    edit: [u8; 32],
    view: Option<[u8; 32]>,
    peer: [u8; 32],
}
fn digest(v: &str) -> [u8; 32] {
    Sha256::digest(v.as_bytes()).into()
}
impl Credentials {
    pub fn new(user: &str, password: &str, view: Option<(&str, &str)>, peer: &str) -> Self {
        Self {
            edit: digest(&format!("{user}:{password}")),
            view: view.map(|(u, p)| digest(&format!("{u}:{p}"))),
            peer: digest(peer),
        }
    }
    pub fn authorize(&self, header: Option<&str>) -> Option<Role> {
        let header = header?;
        let (scheme, encoded) = header.split_once(' ')?;
        if !matches!(scheme, "Basic" | "Bearer") {
            return None;
        }
        let pair = String::from_utf8(STANDARD.decode(encoded).ok()?).ok()?;
        let h = digest(&pair);
        if bool::from(h.ct_eq(&self.edit)) {
            Some(Role::Edit)
        } else if self.view.is_some_and(|v| bool::from(h.ct_eq(&v))) {
            Some(Role::View)
        } else {
            None
        }
    }
    pub fn peer(&self, key: Option<&str>) -> bool {
        key.is_some_and(|k| bool::from(digest(k).ct_eq(&self.peer)))
    }
}
