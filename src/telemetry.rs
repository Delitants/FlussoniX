//! Interval telemetry: management requests never drive or reset sampling.
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::Mutex,
    time::{Duration, Instant},
};
const STALE: Duration = Duration::from_secs(3);
struct Sample {
    at: Instant,
    interface: Option<String>,
    bytes: Option<u64>,
    process: u64,
    rtsp: u64,
    srt: u64,
    cpu: Option<(u64, u64)>,
}
struct History {
    previous: Option<Sample>,
    measured: Option<Measurement>,
}
struct Measurement {
    at: Instant,
    interface: Option<String>,
    mbps: Option<f64>,
    http_mbps: Option<f64>,
    rtsp_mbps: Option<f64>,
    srt_mbps: Option<f64>,
    cpu: Option<f64>,
    ram: Option<f64>,
    bytes: u64,
}
pub struct Sampler {
    selection: String,
    proc_root: PathBuf,
    sys_root: PathBuf,
    history: Mutex<History>,
}
impl Sampler {
    pub fn new(selection: &str) -> Result<Self, String> {
        Self::with_roots(selection, "/proc".into(), "/sys".into())
    }
    pub fn with_roots(
        selection: &str,
        proc_root: PathBuf,
        sys_root: PathBuf,
    ) -> Result<Self, String> {
        if !["auto", "process"].contains(&selection)
            && (!valid_interface(selection) || !sys_root.join("class/net").join(selection).is_dir())
        {
            return Err(
                "uplink interface must be auto, process or an existing interface name".into(),
            );
        }
        Ok(Self {
            selection: selection.into(),
            proc_root,
            sys_root,
            history: Mutex::new(History {
                previous: None,
                measured: None,
            }),
        })
    }
    pub fn sample(&self, bytes: u64) {
        self.sample_at(Instant::now(), bytes)
    }
    pub fn sample_media(&self, http: u64, rtsp: u64) {
        self.sample_media_at(Instant::now(), http, rtsp)
    }
    pub fn sample_all_media(&self, http: u64, rtsp: u64, srt: u64) {
        self.sample_all_media_at(Instant::now(), http, rtsp, srt)
    }
    pub fn sample_at(&self, at: Instant, process: u64) {
        self.sample_media_at(at, process, 0)
    }
    pub fn sample_media_at(&self, at: Instant, process: u64, rtsp: u64) {
        self.sample_all_media_at(at, process, rtsp, 0)
    }
    pub fn sample_all_media_at(&self, at: Instant, process: u64, rtsp: u64, srt: u64) {
        let interface = match self.selection.as_str() {
            "process" => None,
            "auto" => std::fs::read_to_string(self.proc_root.join("net/route"))
                .ok()
                .and_then(|data| default_interface(&data)),
            name => Some(name.into()),
        };
        let bytes = if self.selection == "process" {
            Some(process.saturating_add(rtsp).saturating_add(srt))
        } else {
            interface.as_ref().and_then(|name| {
                std::fs::read_to_string(
                    self.sys_root
                        .join("class/net")
                        .join(name)
                        .join("statistics/tx_bytes"),
                )
                .ok()?
                .trim()
                .parse()
                .ok()
            })
        };
        let cpu = std::fs::read_to_string(self.proc_root.join("stat"))
            .ok()
            .and_then(|data| cpu_ticks(&data));
        let ram = std::fs::read_to_string(self.proc_root.join("meminfo"))
            .ok()
            .and_then(|data| ram_fraction(&data));
        let mut h = self.history.lock().unwrap();
        let mut mbps = None;
        let mut http_mbps = None;
        let mut rtsp_mbps = None;
        let mut srt_mbps = None;
        let mut fraction = None;
        if let Some(prev) = &h.previous {
            let elapsed = at.saturating_duration_since(prev.at).as_secs_f64();
            if elapsed > 0.0 && elapsed <= 3.0 {
                http_mbps = rate(Some(process), Some(prev.process), elapsed);
                rtsp_mbps = rate(Some(rtsp), Some(prev.rtsp), elapsed);
                srt_mbps = rate(Some(srt), Some(prev.srt), elapsed);
                if interface == prev.interface {
                    mbps = if self.selection == "process" {
                        http_mbps
                            .zip(rtsp_mbps)
                            .zip(srt_mbps)
                            .map(|((http, rtsp), srt)| http + rtsp + srt)
                    } else {
                        rate(bytes, prev.bytes, elapsed)
                    };
                }
                if let (Some((total, idle)), Some((old_total, old_idle))) = (cpu, prev.cpu) {
                    if total > old_total && idle >= old_idle && idle - old_idle <= total - old_total
                    {
                        fraction =
                            Some(1.0 - (idle - old_idle) as f64 / (total - old_total) as f64);
                    }
                }
            }
        }
        h.previous = Some(Sample {
            at,
            interface: interface.clone(),
            bytes,
            process,
            rtsp,
            srt,
            cpu,
        });
        h.measured = Some(Measurement {
            at,
            interface,
            mbps,
            http_mbps,
            rtsp_mbps,
            srt_mbps,
            cpu: fraction,
            ram,
            bytes: process.saturating_add(rtsp).saturating_add(srt),
        });
    }
    pub fn snapshot(&self, capacity: f64) -> Value {
        self.snapshot_at(Instant::now(), capacity)
    }
    pub fn snapshot_at(&self, at: Instant, capacity: f64) -> Value {
        let h = self.history.lock().unwrap();
        let Some(m) = &h.measured else {
            return json!({"cpu":null,"ram":null,"uplink":null,"egress_mbps":null,"http_egress_mbps":null,"rtsp_egress_mbps":null,"srt_egress_mbps":null,"media_egress_mbps":null,"uplink_source":if self.selection=="process" {"process"}else{"interface"},"uplink_interface":null,"age_ms":null,"bytes_out":0});
        };
        let age = at.saturating_duration_since(m.at);
        let fresh = age <= STALE;
        let mbps = if fresh { m.mbps } else { None };
        json!({"cpu":if fresh {m.cpu}else{None},"ram":if fresh {m.ram}else{None},"uplink":mbps.map(|v|v/capacity),"egress_mbps":mbps,"http_egress_mbps":if fresh {m.http_mbps}else{None},"rtsp_egress_mbps":if fresh {m.rtsp_mbps}else{None},"srt_egress_mbps":if fresh {m.srt_mbps}else{None},"media_egress_mbps":if fresh {m.http_mbps.zip(m.rtsp_mbps).zip(m.srt_mbps).map(|((h,r),s)|h+r+s)}else{None},"uplink_source":if self.selection=="process" {"process"}else{"interface"},"uplink_interface":m.interface,"age_ms":age.as_millis() as u64,"bytes_out":m.bytes})
    }
}
fn rate(current: Option<u64>, previous: Option<u64>, elapsed: f64) -> Option<f64> {
    Some(current?.checked_sub(previous?)? as f64 * 8.0 / 1_000_000.0 / elapsed)
}
fn valid_interface(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 15
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
}
fn default_interface(data: &str) -> Option<String> {
    data.lines()
        .skip(1)
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 8
                || fields[1] != "00000000"
                || fields[7] != "00000000"
                || !valid_interface(fields[0])
                || u32::from_str_radix(fields[3], 16).ok()? & 1 == 0
            {
                return None;
            }
            Some((fields[6].parse::<u64>().ok()?, fields[0]))
        })
        .min_by_key(|(metric, _)| *metric)
        .map(|(_, name)| name.to_owned())
}
fn cpu_ticks(data: &str) -> Option<(u64, u64)> {
    let line = data.lines().find(|l| l.starts_with("cpu "))?;
    let values = line
        .split_whitespace()
        .skip(1)
        .take(8)
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if values.len() < 4 {
        return None;
    }
    Some((
        values.iter().try_fold(0u64, |sum, v| sum.checked_add(*v))?,
        values[3].checked_add(values.get(4).copied().unwrap_or(0))?,
    ))
}
fn ram_fraction(data: &str) -> Option<f64> {
    let number = |key: &str| {
        data.lines()
            .find(|l| l.starts_with(key))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
    };
    let total = number("MemTotal:")?;
    let available = number("MemAvailable:")?;
    if total == 0 || available > total {
        return None;
    }
    Some(1.0 - available as f64 / total as f64)
}
