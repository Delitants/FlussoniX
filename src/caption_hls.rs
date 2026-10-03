//! Generation-owned, authorized HLS rendition resources from real AV clocks.
use crate::{
    captions::{Cue, Decoder},
    m4s::boxes,
};
use bytes::Bytes;
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
#[derive(Default)]
struct Variant {
    anchor: Option<u64>,
    bandwidth: u64,
    last_raw: String,
    av: Option<String>,
    lists: HashMap<String, Bytes>,
    segments: HashMap<String, Bytes>,
}
pub struct State {
    pub decoder: Mutex<Decoder>,
    variants: Mutex<[Variant; 2]>,
    pub failed: AtomicBool,
    generation: String,
}
impl State {
    pub fn new(decoder: Decoder, generation: String, _sequence: u64) -> Self {
        Self {
            decoder: Mutex::new(decoder),
            variants: Mutex::new(std::array::from_fn(|_| Variant::default())),
            failed: AtomicBool::new(false),
            generation,
        }
    }
    pub fn observe_ts(
        &self,
        bytes: &[u8],
        transport: &mut crate::caption_transport::Transport,
        decoder: &mut Decoder,
    ) {
        if decoder.first_pts.is_none() {
            transport.push(bytes, decoder);
            if let Some(pts) = decoder.first_pts {
                self.variants.lock().unwrap()[0].anchor.get_or_insert(pts);
            }
        }
    }
    pub fn stats(&self) -> serde_json::Value {
        let ready = self.variants.lock().unwrap().iter().all(|v| v.av.is_some());
        let d = self.decoder.lock().unwrap();
        serde_json::json!({"status":if self.failed.load(Ordering::Relaxed){"failed"}else if d.error.is_some(){"degraded"}else if d.first_pts.is_some()&&ready{"running"}else{"starting"},"last_error":if self.failed.load(Ordering::Relaxed){d.error.or(Some("caption_decoder_lag"))}else{d.error},"channels":d.services,"cues":d.snapshot().len()})
    }
    pub fn read(&self, file: &str) -> Option<Bytes> {
        let (index, file) = if let Some(f) = file.strip_prefix("fmp4/") {
            (1, f)
        } else {
            (0, file)
        };
        let variants = self.variants.lock().unwrap();
        let v = &variants[index];
        v.av.as_ref()?;
        if self.failed.load(Ordering::Relaxed) {
            return if file == "index.m3u8" || file == "av.m3u8" {
                Some(Bytes::from(v.av.clone().unwrap()))
            } else {
                None
            };
        }
        if file == "index.m3u8" {
            let d = self.decoder.lock().unwrap();
            let mut text = String::from("#EXTM3U\n#EXT-X-VERSION:7\n");
            for s in &d.services {
                text += &format!(
                    "#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"captions\",NAME=\"{}\",LANGUAGE=\"{}\",DEFAULT={},AUTOSELECT=YES,FORCED=NO,URI=\"cc{}.m3u8\"\n",
                    s.name,
                    s.language,
                    if s.channel == d.services[0].channel {
                        "YES"
                    } else {
                        "NO"
                    },
                    s.channel
                )
            }
            text += &format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={},SUBTITLES=\"captions\"\nav.m3u8\n",
                v.bandwidth.max(1000)
            );
            return Some(Bytes::from(text));
        }
        if file == "av.m3u8" {
            return Some(Bytes::from(v.av.clone().unwrap()));
        }
        v.lists.get(file).or_else(|| v.segments.get(file)).cloned()
    }
    pub async fn watch(self: Arc<Self>, dir: std::path::PathBuf, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {biased;_=cancel.cancelled()=>break,_=tick.tick()=>{}}
            for (index, prefix) in [(0, ""), (1, "fmp4/")] {
                let Ok(raw) =
                    tokio::fs::read_to_string(dir.join(format!("{prefix}index.m3u8"))).await
                else {
                    continue;
                };
                if self.variants.lock().unwrap()[index].last_raw == raw
                    && !self.failed.load(Ordering::Relaxed)
                {
                    continue;
                }
                let Ok(grid) = Grid::parse(&raw) else {
                    continue;
                };
                if grid.segments.len() < 2 {
                    continue;
                }
                if self.failed.load(Ordering::Relaxed) {
                    self.variants.lock().unwrap()[index].av =
                        Some(grid.playlist(&grid.segments, None));
                    continue;
                }
                let need_anchor = self.variants.lock().unwrap()[index].anchor.is_none();
                if need_anchor {
                    if index == 0 {
                        continue;
                    }
                    let Some(ts_anchor) = self.variants.lock().unwrap()[0].anchor else {
                        continue;
                    };
                    let first = &grid.segments[0];
                    let Ok(media) = bounded_read(&dir.join(format!("fmp4/{}", first.file))).await
                    else {
                        continue;
                    };
                    let Some(init) = grid.init.as_ref() else {
                        continue;
                    };
                    let Ok(init) = bounded_read(&dir.join(format!("fmp4/{init}"))).await else {
                        continue;
                    };
                    let Ok(ts) = bounded_read(&dir.join(first.file.replace(".m4s", ".ts"))).await
                    else {
                        continue;
                    };
                    let (Some(mp4), Some(ts)) = (mp4_clock(&init, &media), ts_clock(&ts)) else {
                        continue;
                    };
                    self.variants.lock().unwrap()[1].anchor = Some(
                        ((i128::from(ts_anchor) + i128::from(mp4) - i128::from(ts))
                            .rem_euclid(1 << 33)) as u64,
                    );
                }
                let (first, latest, cues, services) = {
                    let d = self.decoder.lock().unwrap();
                    let Some(first) = d.first_pts else { continue };
                    (first, d.latest_pts, d.snapshot(), d.services.clone())
                };
                let (anchor, previous, mut bandwidth) = {
                    let v = self.variants.lock().unwrap();
                    let Some(anchor) = v[index].anchor else {
                        continue;
                    };
                    (anchor, v[index].segments.clone(), v[index].bandwidth)
                };
                let segments = &grid.segments[..grid.segments.len() - 1];
                let init = if index == 1 {
                    let Some(init) = grid.init.as_ref() else {
                        continue;
                    };
                    let Ok(data) = bounded_read(&dir.join(format!("{prefix}{init}"))).await else {
                        continue;
                    };
                    Some(data)
                } else {
                    None
                };
                let mut clocks = HashMap::new();
                let mut complete = true;
                for seg in segments {
                    let first_name = format!(
                        "cc{}_g{}_{}.vtt",
                        services[0].channel, self.generation, seg.sequence
                    );
                    if previous.contains_key(&first_name) {
                        continue;
                    }
                    let Ok(data) = bounded_read(&dir.join(format!("{prefix}{}", seg.file))).await
                    else {
                        complete = false;
                        break;
                    };
                    let clock = if let Some(init) = &init {
                        mp4_clock(init, &data)
                    } else {
                        ts_clock(&data)
                    };
                    let Some(clock) = clock else {
                        complete = false;
                        break;
                    };
                    let source_end =
                        first + ((clock.wrapping_sub(anchor)) & ((1 << 33) - 1)) + seg.duration;
                    if latest < source_end.saturating_add(90000) {
                        complete = false;
                        break;
                    }
                    bandwidth = bandwidth
                        .max((data.len() as u64 * 8 * 90000 / seg.duration) * 5 / 4 + 64000);
                    clocks.insert(seg.sequence, clock);
                }
                if !complete {
                    continue;
                }
                let mut lists = HashMap::new();
                let mut retained = HashMap::new();
                'services: for service in services {
                    let files: Vec<_> = segments
                        .iter()
                        .map(|seg| {
                            format!(
                                "cc{}_g{}_{}.vtt",
                                service.channel, self.generation, seg.sequence
                            )
                        })
                        .collect();
                    lists.insert(
                        format!("cc{}.m3u8", service.channel),
                        Bytes::from(grid.playlist(segments, Some(&files))),
                    );
                    for (seg, file) in segments.iter().zip(files) {
                        let content = if let Some(previous) = previous.get(&file) {
                            previous.clone()
                        } else {
                            let clock = clocks[&seg.sequence];
                            let offset = ((clock.wrapping_sub(anchor)) & ((1 << 33) - 1))
                                .saturating_add(first);
                            match webvtt(
                                &cues,
                                service.channel,
                                first,
                                anchor,
                                offset,
                                offset + seg.duration,
                                &self.generation,
                            ) {
                                Ok(text) => Bytes::from(text),
                                Err(reason) => {
                                    self.decoder.lock().unwrap().error = Some(reason);
                                    self.failed.store(true, Ordering::Relaxed);
                                    complete = false;
                                    break 'services;
                                }
                            }
                        };
                        retained.insert(file, content);
                    }
                }
                if !complete {
                    continue;
                }
                for (file, content) in previous {
                    if file
                        .trim_end_matches(".vtt")
                        .rsplit('_')
                        .next()
                        .and_then(|s| s.parse::<u64>().ok())
                        .is_some_and(|seq| seq >= grid.sequence.saturating_sub(2))
                    {
                        retained.entry(file).or_insert(content);
                    }
                }
                let mut variants = self.variants.lock().unwrap();
                let v = &mut variants[index];
                v.av = Some(grid.playlist(segments, None));
                v.lists = lists;
                v.segments = retained;
                v.bandwidth = bandwidth;
                v.last_raw = raw;
            }
        }
    }
}
async fn bounded_read(path: &Path) -> Result<Vec<u8>, String> {
    let size = tokio::fs::metadata(path)
        .await
        .map_err(|_| "media absent")?
        .len();
    if size > 32 * 1024 * 1024 {
        return Err("media limit".into());
    }
    tokio::fs::read(path)
        .await
        .map_err(|_| "media absent".into())
}
fn webvtt(
    cues: &[Cue],
    channel: u8,
    first: u64,
    anchor: u64,
    start: u64,
    end: u64,
    generation: &str,
) -> Result<String, &'static str> {
    let mut out = format!(
        "WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00:00.000,MPEGTS:{}\n\n",
        anchor & ((1 << 33) - 1)
    );
    for c in cues
        .iter()
        .filter(|c| c.channel == channel && c.start < end && c.end.unwrap_or(end) > start)
    {
        // Stable one-second display slices avoid rewriting an already served
        // live cue when its erase time becomes known in a later segment.
        let text = c
            .text
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");
        let mut slice = c.start + start.saturating_sub(c.start) / 90000 * 90000;
        while slice < end {
            let finish = c.end.unwrap_or(u64::MAX).min(slice.saturating_add(90000));
            if finish <= slice {
                break;
            }
            if finish > start {
                out += &format!(
                    "cc{}-{}-{}-{}\n{} --> {}\n{}\n\n",
                    channel,
                    generation,
                    c.start,
                    slice,
                    time(slice.saturating_sub(first)),
                    time(finish.saturating_sub(first)),
                    text
                );
            }
            if out.len() > 1024 * 1024 {
                return Err("caption_rendition_limit");
            }
            slice = slice.saturating_add(90000);
        }
    }
    Ok(out)
}
fn time(t: u64) -> String {
    let ms = t / 90;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        ms / 3600000,
        ms / 60000 % 60,
        ms / 1000 % 60,
        ms % 1000
    )
}
struct Segment {
    sequence: u64,
    duration: u64,
    file: String,
    tags: Vec<String>,
}
struct Grid {
    sequence: u64,
    header: Vec<String>,
    segments: Vec<Segment>,
    init: Option<String>,
}
impl Grid {
    fn parse(raw: &str) -> Result<Self, String> {
        let sequence = raw
            .lines()
            .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
            .ok_or("missing sequence")?
            .parse::<u64>()
            .map_err(|_| "bad sequence")?;
        let mut grid = Self {
            sequence,
            header: vec![],
            segments: vec![],
            init: None,
        };
        let mut tags = vec![];
        let mut duration = None;
        for line in raw.lines() {
            if line.starts_with("#EXTINF:") {
                let value = line
                    .trim_start_matches("#EXTINF:")
                    .trim_end_matches(',')
                    .parse::<f64>()
                    .map_err(|_| "bad duration")?;
                if !value.is_finite() || value <= 0.0 || value > 120.0 {
                    return Err("bad duration".into());
                }
                duration = Some((value * 90000.0).round() as u64);
                tags.push(line.to_owned());
            } else if line.starts_with("#EXT-X-DISCONTINUITY")
                && !line.starts_with("#EXT-X-DISCONTINUITY-SEQUENCE")
            {
                tags.push(line.to_owned())
            } else if !line.starts_with('#') && !line.is_empty() {
                let n = grid.segments.len() as u64;
                grid.segments.push(Segment {
                    sequence: sequence.checked_add(n).ok_or("sequence overflow")?,
                    duration: duration.take().ok_or("missing duration")?,
                    file: line.to_owned(),
                    tags: std::mem::take(&mut tags),
                });
            } else if !line.is_empty() && line != "#EXT-X-ENDLIST" {
                if let Some(map) = line.strip_prefix("#EXT-X-MAP:URI=\"") {
                    grid.init = map.split('"').next().map(str::to_owned)
                }
                grid.header.push(line.to_owned())
            }
        }
        if grid.segments.len() > 16 {
            return Err("playlist limit".into());
        }
        Ok(grid)
    }
    fn playlist(&self, segments: &[Segment], files: Option<&[String]>) -> String {
        let mut out = String::new();
        for header in &self.header {
            if files.is_some() && header.starts_with("#EXT-X-MAP:") {
                continue;
            }
            out += header;
            out.push('\n')
        }
        for (i, seg) in segments.iter().enumerate() {
            for tag in &seg.tags {
                out += tag;
                out.push('\n')
            }
            out += files.map_or(seg.file.as_str(), |f| f[i].as_str());
            out.push('\n')
        }
        out
    }
}
fn ts_clock(data: &[u8]) -> Option<u64> {
    let mut t = crate::caption_transport::Transport::default();
    let mut d = Decoder::new(vec![]);
    for chunk in data.chunks(188 * 64) {
        t.push(chunk, &mut d);
        if d.first_pts.is_some() {
            return d.first_pts;
        }
    }
    None
}
fn field<'a>(data: &'a [u8], kind: &[u8]) -> Option<&'a [u8]> {
    boxes(data)
        .ok()?
        .into_iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, b)| b)
}
fn n32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(b.get(at..at + 4)?.try_into().ok()?))
}
fn mp4_clock(init: &[u8], media: &[u8]) -> Option<u64> {
    let moov = field(init, b"moov")?;
    let mut video = None;
    for (kind, trak) in boxes(moov).ok()? {
        if kind != b"trak" {
            continue;
        }
        let mdia = field(trak, b"mdia")?;
        if field(mdia, b"hdlr")?.get(8..12)? != b"vide" {
            continue;
        }
        let tkhd = field(trak, b"tkhd")?;
        let id = n32(tkhd, if *tkhd.first()? == 1 { 20 } else { 12 })?;
        let mdhd = field(mdia, b"mdhd")?;
        let scale = n32(mdhd, if *mdhd.first()? == 1 { 20 } else { 12 })?;
        if scale == 0 {
            return None;
        }
        video = Some((id, u64::from(scale)));
        break;
    }
    let (id, scale) = video?;
    let moof = field(media, b"moof")?;
    for (kind, traf) in boxes(moof).ok()? {
        if kind != b"traf" {
            continue;
        }
        let tfhd = field(traf, b"tfhd")?;
        if n32(tfhd, 4)? != id {
            continue;
        }
        let tfdt = field(traf, b"tfdt")?;
        let dts = if *tfdt.first()? == 1 {
            u64::from_be_bytes(tfdt.get(4..12)?.try_into().ok()?)
        } else {
            u64::from(n32(tfdt, 4)?)
        };
        let trun = field(traf, b"trun")?;
        let flags = n32(trun, 0)? & 0xffffff;
        let mut at = 8;
        if flags & 1 != 0 {
            at += 4
        }
        if flags & 4 != 0 {
            at += 4
        }
        for f in [0x100, 0x200, 0x400] {
            if flags & f != 0 {
                at += 4
            }
        }
        let offset = if flags & 0x800 != 0 {
            let n = n32(trun, at)?;
            if *trun.first()? == 1 {
                i64::from(n as i32)
            } else {
                i64::from(n)
            }
        } else {
            0
        };
        return Some(dts.saturating_add_signed(offset).checked_mul(90000)? / scale);
    }
    None
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn an_open_display_has_stable_cues_after_its_later_clear() {
        let open = Cue {
            channel: 1,
            start: 104400,
            end: None,
            text: "HELLO".into(),
        };
        let mut closed = open.clone();
        closed.end = Some(585000);
        assert_eq!(
            webvtt(&[open], 1, 0, 126000, 0, 180000, "owned").unwrap(),
            webvtt(&[closed], 1, 0, 126000, 0, 180000, "owned").unwrap()
        );
    }
    #[test]
    fn silence_wrap_markup_and_discontinuity_are_safe() {
        let cue = Cue {
            channel: 1,
            start: 90000,
            end: Some(360000),
            text: "<b>&".into(),
        };
        let v = webvtt(&[cue], 1, 0, (1 << 33) + 90000, 180000, 270000, "owned").unwrap();
        assert!(v.contains("MPEGTS:90000"));
        assert!(v.contains("&lt;b&gt;&amp;"));
        assert!(
            !webvtt(&[], 1, 0, 0, 0, 180000, "owned")
                .unwrap()
                .contains("-->")
        );
        let g=Grid::parse("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:100\n#EXT-X-DISCONTINUITY\n#EXTINF:2,\ng1_100.ts\n#EXTINF:2,\ng1_101.ts\n").unwrap();
        let output = g.playlist(&g.segments[..1], Some(&["cc1_g1_100.vtt".into()]));
        assert!(output.contains("#EXT-X-DISCONTINUITY\n#EXTINF:2,"));
        assert!(!output.contains("g1_101"));
    }
}
#[cfg(test)]
mod failure_tests {
    use super::*;
    #[test]
    fn decoder_failure_keeps_av_playback_and_removes_caption_advertising() {
        let state = State::new(
            Decoder::new(vec![crate::captions::Service {
                channel: 1,
                language: "en".into(),
                name: "English".into(),
            }]),
            "owned".into(),
            1,
        );
        let av = "#EXTM3U\n#EXTINF:2,\ngowned_1.ts\n";
        {
            let mut variants = state.variants.lock().unwrap();
            variants[0].av = Some(av.into());
            variants[0]
                .lists
                .insert("cc1.m3u8".into(), Bytes::from_static(b"obsolete captions"));
        }
        state.failed.store(true, Ordering::Relaxed);
        assert_eq!(state.read("index.m3u8").unwrap(), av);
        assert!(state.read("cc1.m3u8").is_none());
        assert_eq!(state.stats()["status"], "failed");
    }
}
#[cfg(test)]
mod identity_limits {
    use super::*;
    #[test]
    fn each_generation_has_distinct_player_cue_ids() {
        let cues = [Cue {
            channel: 1,
            start: 90000,
            end: Some(270000),
            text: "HELLO".into(),
        }];
        let a = webvtt(&cues, 1, 0, 126000, 0, 180000, "one").unwrap();
        let b = webvtt(&cues, 1, 0, 126000, 0, 180000, "two").unwrap();
        assert_ne!(
            a.lines().find(|l| l.starts_with("cc1")),
            b.lines().find(|l| l.starts_with("cc1"))
        );
        assert!(a.contains("HELLO"));
        assert!(b.contains("HELLO"));
    }
    #[test]
    fn oversized_renditions_fail_conversion_instead_of_allocating_unbounded_output() {
        let cues: Vec<_> = (0..4096)
            .map(|i| Cue {
                channel: 1,
                start: i,
                end: Some(180000),
                text: "界".repeat(480),
            })
            .collect();
        assert!(webvtt(&cues, 1, 0, 0, 0, 180000, "owned").is_err());
    }
}
