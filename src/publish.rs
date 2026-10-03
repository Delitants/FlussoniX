//! Publisher credentials and policy are separate from viewer and peer admission.
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

#[derive(Clone, PartialEq, Eq)]
pub struct Policy {
    password: Option<[u8; 32]>,
    pub url: Option<String>,
}
impl Policy {
    pub fn from_config(cfg: &Value, root: &Value) -> Result<Self, String> {
        let password = cfg
            .get("password")
            .map(|v| {
                let value = v
                    .as_str()
                    .filter(|p| {
                        !p.is_empty() && p.len() <= 1024 && !p.chars().any(char::is_control)
                    })
                    .ok_or("publication password must be a nonempty string up to 1024 bytes")?;
                Ok::<[u8; 32], String>(Sha256::digest(value.as_bytes()).into())
            })
            .transpose()?;
        let url = cfg.get("on_publish").map(|v| -> Result<String, String> {
            let value = v.as_str().ok_or("on_publish currently requires a URL string")?;
            let value = if let Some(name) = value.strip_prefix("auth://") {
                crate::config::valid_name(name)?;
                root["auth_backends"].as_array().and_then(|a| a.iter().find(|b| b["name"] == name)).and_then(|v| v["url"].as_str()).ok_or("publish auth backend not found")?
            } else { value };
            let parsed = url::Url::parse(value).map_err(|_| "invalid publication callback URL")?;
            if !["http", "https"].contains(&parsed.scheme()) || parsed.host_str().is_none() || !parsed.username().is_empty() || parsed.password().is_some() || parsed.fragment().is_some() || value.len() > 4096 {
                return Err("publication callback requires an absolute HTTP(S) URL without credentials/fragments".into());
            }
            Ok(parsed.to_string())
        }).transpose()?;
        Ok(Self { password, url })
    }
    pub fn accepts_password(&self, value: &str) -> bool {
        let actual: [u8; 32] = Sha256::digest(value.as_bytes()).into();
        self.password
            .is_none_or(|expected| bool::from(expected.ct_eq(&actual)))
    }
}
pub fn is_input(cfg: &Value) -> bool {
    cfg["inputs"]
        .as_array()
        .is_some_and(|inputs| inputs.len() == 1 && inputs[0]["url"] == "publish://")
}
