pub mod store;

use serde_json::Value;

pub(crate) const TOKENLESS_OUTPUT_ERROR: &str = "IPTV stream outputs require a subscriber token; configured push and RTP/SRTP outputs cannot carry one";

pub(crate) fn tokenless_output_conflict(config: &Value) -> bool {
    let iptv = config["on_play"] == "iptv://" || config["on_play"]["url"] == "iptv://";
    iptv && (crate::direct_rtp::config::enabled(config)
        || config["pushes"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["disabled"] != true)))
}
