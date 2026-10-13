//! Local processing-template projection. No source, policy or publication fields.
use serde_json::{Value, json};
pub(crate) const FIELDS: &[&str] = &[
    "transcoder",
    "flussonix_input_timeout",
    "flussonix_subtitle_tracks",
    "flussonix_hls_subtitles",
    "flussonix_hls_captions",
];
const INVALID: &str = "invalid vendor processing template or settings";
pub(crate) fn resolve(root: &Value, source: &Value) -> Result<Value, String> {
    let Some(reference) = source.get("flussonix_output_template") else {
        return Ok(json!({}));
    };
    let work = || {
        if source["flussonix_source_kind"] != "flussonic" {
            return Err(INVALID.to_owned());
        }
        let name = reference.as_str().ok_or(INVALID)?;
        crate::config::valid_name(name)?;
        let template = root["templates"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["name"] == name))
            .ok_or(INVALID)?;
        let mut settings = json!({});
        for field in FIELDS {
            if let Some(value) = template.get(*field) {
                settings[*field] = value.clone();
            }
        }
        validate(&settings)?;
        Ok(settings)
    };
    work().map_err(|_| INVALID.to_owned())
}
pub(crate) fn validate(settings: &Value) -> Result<(), String> {
    let object = settings.as_object().ok_or(INVALID)?;
    if object.keys().any(|k| !FIELDS.contains(&k.as_str())) {
        return Err(INVALID.to_owned());
    }
    if object
        .get("flussonix_input_timeout")
        .is_some_and(|v| !v.as_u64().is_some_and(|n| (1..=300).contains(&n)))
    {
        return Err(INVALID.to_owned());
    }
    crate::transcoder::Profile::resolve(settings, false).map_err(|_| INVALID)?;
    crate::config::subtitle_tracks(settings).map_err(|_| INVALID)?;
    let mode = crate::config::hls_subtitles(settings).map_err(|_| INVALID)?;
    let services = crate::captions::configuration(settings).map_err(|_| INVALID)?;
    if mode == "convert" && services.is_empty() {
        return Err(INVALID.to_owned());
    }
    if !services.is_empty()
        && matches!(
            settings["transcoder"]["encoder"].as_str(),
            Some("h264_nvenc" | "hevc_nvenc" | "h264_vaapi" | "hevc_vaapi")
        )
    {
        // Same known restriction as Engine startup; hardware readiness is separate.
        return Err(INVALID.to_owned());
    }
    Ok(())
}
