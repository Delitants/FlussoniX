//! Independent, bounded CEA-608 display state. Times are source video PTS (90 kHz).
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
#[derive(Clone, Debug)]
pub struct Service {
    pub channel: u32,
    pub language: String,
    pub name: String,
    pub ocr_language: Option<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Row {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    channel: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    service: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    teletext_page: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dvb_page: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ocr_language: Option<String>,
    language: String,
    name: String,
}
impl<'de> Deserialize<'de> for Service {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let row = Row::deserialize(d)?;
        let channel = match (row.channel, row.service, row.teletext_page, row.dvb_page) {
            (Some(c), None, None, None) if (1..=4).contains(&c) => u32::from(c),
            (None, Some(s), None, None) if (1..=63).contains(&s) => 64 + u32::from(s),
            (None, None, Some(p), None) if (100..=899).contains(&p) => 1024 + u32::from(p),
            (None, None, None, Some(p)) => 65536 + u32::from(p),
            _ => {
                return Err(serde::de::Error::custom(
                    "choose channel 1..4 OR service 1..63 OR teletext_page 100..899 OR dvb_page 0..65535",
                ));
            }
        };
        if (channel >= 65536) != row.ocr_language.is_some()
            || row
                .ocr_language
                .as_ref()
                .is_some_and(|s| !ocr_model_names(s))
        {
            return Err(serde::de::Error::custom(
                "only DVB pages require valid OCR model names",
            ));
        }
        Ok(Self {
            channel,
            language: row.language,
            name: row.name,
            ocr_language: row.ocr_language,
        })
    }
}
impl Serialize for Service {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        Row {
            channel: (self.channel <= 4).then_some(self.channel as u16),
            service: if (65..=127).contains(&self.channel) {
                Some((self.channel - 64) as u8)
            } else {
                None
            },
            teletext_page: (1124..=1923)
                .contains(&self.channel)
                .then(|| (self.channel - 1024) as u16),
            dvb_page: (self.channel >= 65536).then(|| (self.channel - 65536) as u16),
            ocr_language: self.ocr_language.clone(),
            language: self.language.clone(),
            name: self.name.clone(),
        }
        .serialize(s)
    }
}
impl Service {
    pub fn key(&self) -> String {
        key(self.channel)
    }
}
pub fn key(channel: u32) -> String {
    if channel >= 65536 {
        format!("dvb{}", channel - 65536)
    } else if channel >= 1124 {
        format!("ttx{}", channel - 1024)
    } else if channel > 64 {
        format!("s{}", channel - 64)
    } else {
        format!("cc{channel}")
    }
}
fn ocr_model_names(s: &str) -> bool {
    s.len() <= 99
        && (1..=4).contains(&s.split('+').count())
        && s.split('+').all(|part| {
            (1..=24).contains(&part.len())
                && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
}
pub fn configuration(cfg: &Value) -> Result<Vec<Service>, String> {
    let Some(rows) = cfg.get("flussonix_hls_captions") else {
        return Ok(vec![]);
    };
    let services: Vec<Service> = serde_json::from_value(rows.clone()).map_err(
        |_| "HLS captions require one channel/service/teletext_page/dvb_page selector, language and name; DVB pages also require OCR model names",
    )?;
    if services.len() > 4 {
        return Err("at most four HLS caption renditions are supported".into());
    }
    let mut names = std::collections::HashSet::new();
    let mut seen = std::collections::HashSet::new();
    for s in &services {
        if !names.insert(&s.name) {
            return Err("caption display names must be unique".into());
        }
        if !seen.insert(s.channel) {
            return Err("caption selectors must be distinct".into());
        }
        if !(2..=3).contains(&s.language.len())
            || !s.language.bytes().all(|b| b.is_ascii_lowercase())
        {
            return Err("caption language must be a two or three letter lowercase code".into());
        }
        if s.name.is_empty()
            || s.name.len() > 80
            || s.name
                .chars()
                .any(|c| c.is_control() || matches!(c, '"' | '\\'))
        {
            return Err(
                "caption name must be 1..80 bytes without quotes, backslashes or controls".into(),
            );
        }
    }
    Ok(if crate::config::hls_subtitles(cfg)? == "convert" {
        services
    } else {
        vec![]
    })
}
#[derive(Clone, Debug)]
pub struct Cue {
    pub channel: u32,
    pub start: u64,
    pub end: Option<u64>,
    pub text: String,
}
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Pop,
    Paint,
    Roll(usize),
    Text,
}
struct Channel {
    visible: [[char; 32]; 15],
    hidden: [[char; 32]; 15],
    row: usize,
    col: usize,
    mode: Mode,
    last: Option<[u8; 2]>,
    open: Option<Cue>,
}
impl Default for Channel {
    fn default() -> Self {
        Self {
            visible: [[' '; 32]; 15],
            hidden: [[' '; 32]; 15],
            row: 14,
            col: 0,
            mode: Mode::Pop,
            last: None,
            open: None,
        }
    }
}
impl Channel {
    fn screen(&mut self) -> &mut [[char; 32]; 15] {
        if self.mode == Mode::Pop {
            &mut self.hidden
        } else {
            &mut self.visible
        }
    }
    fn put(&mut self, c: char) {
        if self.mode == Mode::Text || self.col >= 32 {
            return;
        }
        let (r, col) = (self.row, self.col);
        self.screen()[r][col] = c;
        self.col = col + 1
    }
    fn backspace(&mut self) {
        if self.col == 0 {
            return;
        }
        self.col -= 1;
        let (r, c) = (self.row, self.col);
        self.screen()[r][c] = ' '
    }
    fn text(&self) -> String {
        let rows: Vec<_> = self
            .visible
            .iter()
            .map(|r| r.iter().collect::<String>().trim_end().to_owned())
            .collect();
        let start = rows.iter().position(|r| !r.is_empty()).unwrap_or(15);
        let end = rows
            .iter()
            .rposition(|r| !r.is_empty())
            .map_or(start, |r| r + 1);
        rows[start..end].join("\n")
    }
    fn apply(&mut self, a: u8, b: u8) {
        if (0x10..=0x17).contains(&a) && (0x40..=0x7f).contains(&b) {
            let base = [10, 0, 2, 11, 13, 4, 6, 8][usize::from(a & 7)];
            let row = base + usize::from(b & 0x20 != 0);
            if row < 15 {
                if matches!(self.mode, Mode::Roll(_)) && self.row != row {
                    self.visible = [[' '; 32]; 15]
                }
                self.row = row;
                self.col = if b & 0x10 != 0 {
                    usize::from((b & 14) >> 1) * 4
                } else {
                    0
                };
            }
            return;
        }
        if (a == 0x14 || a == 0x15) && (0x20..=0x2f).contains(&b) {
            match b {
                0x20 => self.mode = Mode::Pop,
                0x21 => self.backspace(),
                0x24 => {
                    let (r, c) = (self.row, self.col);
                    self.screen()[r][c..].fill(' ')
                }
                0x25..=0x27 => {
                    let n = usize::from(b - 0x23);
                    if !matches!(self.mode, Mode::Roll(_)) {
                        self.visible = [[' '; 32]; 15];
                        self.hidden = [[' '; 32]; 15];
                        self.row = 14;
                        self.col = 0
                    }
                    self.mode = Mode::Roll(n)
                }
                0x29 => self.mode = Mode::Paint,
                0x2a | 0x2b => self.mode = Mode::Text,
                0x2c => self.visible = [[' '; 32]; 15],
                0x2d => {
                    if let Mode::Roll(n) = self.mode {
                        let start = (self.row + 1).saturating_sub(n);
                        for r in start..self.row {
                            self.visible[r] = self.visible[r + 1]
                        }
                        self.visible[self.row] = [' '; 32];
                        self.col = 0
                    }
                }
                0x2e => self.hidden = [[' '; 32]; 15],
                0x2f => {
                    std::mem::swap(&mut self.visible, &mut self.hidden);
                    self.mode = Mode::Pop
                }
                _ => {}
            }
            return;
        }
        if a == 0x17 && (0x21..=0x23).contains(&b) {
            self.col = (self.col + usize::from(b - 0x20)).min(31);
            return;
        }
        if a == 0x11 && (0x20..=0x2f).contains(&b) {
            self.put(' ');
            return;
        }
        if a == 0x11 && (0x30..=0x3f).contains(&b) {
            self.put(
                "®°½¿™¢£♪à èâêîôû"
                    .chars()
                    .nth(usize::from(b - 0x30))
                    .unwrap(),
            );
            return;
        }
        if (a == 0x12 || a == 0x13) && (0x20..=0x3f).contains(&b) {
            self.backspace();
            let table = if a == 0x12 {
                "ÁÉÓÚÜü´¡*‘-©℠·“”ÀÂÇÈÊËëÎÏïÔÙùÛ«»"
            } else {
                "ÃãÍÌìÒòÕõ{}\\^_|~ÄäÖöß¥¤¦ÅåØø┌┐└┘"
            };
            self.put(table.chars().nth(usize::from(b - 0x20)).unwrap());
            return;
        }
        if a >= 0x20 {
            self.put(basic(a));
            if b >= 0x20 {
                self.put(basic(b))
            }
        }
    }
}
fn basic(b: u8) -> char {
    match b {
        0x27 => '’',
        0x2a => 'á',
        0x5c => 'é',
        0x5e => 'í',
        0x5f => 'ó',
        0x60 => 'ú',
        0x7b => 'ç',
        0x7c => '÷',
        0x7d => 'Ñ',
        0x7e => 'ñ',
        0x7f => '█',
        _ => char::from(b),
    }
}
pub struct Decoder {
    pub services: Vec<Service>,
    channels: [Channel; 4],
    selected: [usize; 2],
    history: VecDeque<Cue>,
    digital: crate::cea708::Decoder,
    teletext: crate::teletext::Decoder,
    dvb: crate::dvb::Decoder,
    dvb_frames: VecDeque<crate::dvb::Frame>,
    pub(crate) dvb_ocr: crate::dvb_ocr::Store,
    digital_open: BTreeMap<u32, Cue>,
    pub first_pts: Option<u64>,
    pub latest_pts: u64,
    pub error: Option<&'static str>,
}
impl Decoder {
    pub(crate) fn process_dvb_frames(&mut self) {
        for frame in self.take_dvb_frames() {
            self.dvb_ocr.ingest(frame, std::time::Instant::now());
        }
    }
    pub(crate) fn publication_frontier(&mut self) -> u64 {
        self.dvb_ocr.expire(std::time::Instant::now());
        self.dvb_ocr.frontier(self.latest_pts)
    }
    fn reset_dvb_ocr(&mut self, pages: &[u16], pts: u64, reason: &'static str) {
        self.dvb_frames.retain(|f| !pages.contains(&f.page));
        self.dvb_ocr.reset(pages, pts, reason);
    }
    pub fn take_dvb_frames(&mut self) -> Vec<crate::dvb::Frame> {
        self.dvb_frames.drain(..).collect()
    }
    pub(crate) fn dvb_error(&self) -> Option<&'static str> {
        self.dvb.current_error()
    }
    pub fn dvb_stats(&self) -> Value {
        self.dvb.stats()
    }
    pub(crate) fn dvb_pages(&self) -> Vec<u16> {
        self.dvb.pages()
    }
    fn dvb_changes(&mut self, frames: Vec<crate::dvb::Frame>) {
        for f in frames {
            if self.dvb_frames.len() >= 8 {
                let pages = self.dvb.pages();
                self.reset_dvb_ocr(&pages, f.pts, "dvb_image_queue_limit");
                self.dvb_frames.clear();
                self.error = Some("dvb_image_queue_limit");
            }
            self.dvb_frames.push_back(f);
        }
        if self.dvb.error.is_some() {
            self.error = self.dvb.error;
        }
    }
    pub(crate) fn dvb_bindings(&mut self, b: &BTreeMap<u16, (u16, u16)>, pts: u64) {
        let frames = self.dvb.bindings(b, pts);
        let pages = frames.iter().map(|f| f.page).collect::<Vec<_>>();
        self.reset_dvb_ocr(&pages, pts, "dvb_page_rebound");
        self.dvb_changes(frames);
    }
    pub(crate) fn dvb_gap(&mut self, pid: u16, pts: u64) {
        let frames = self.dvb.reset_pid(pid, pts);
        let pages = frames.iter().map(|f| f.page).collect::<Vec<_>>();
        self.reset_dvb_ocr(&pages, pts, "dvb_transport_gap");
        self.dvb_changes(frames);
    }
    pub(crate) fn push_dvb(&mut self, pid: u16, body: &[u8], pts: u64) {
        let frames = self.dvb.push(pid, body, pts);
        let bad = self
            .dvb
            .stats()
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["pid"].as_u64() == Some(u64::from(pid)) && p["last_error"].is_string())
            .filter_map(|p| p["page"].as_u64().map(|p| p as u16))
            .collect::<Vec<_>>();
        if !bad.is_empty() {
            self.reset_dvb_ocr(&bad, pts, self.dvb.error.unwrap_or("dvb_decode_failed"));
        }
        self.dvb_changes(frames);
    }

    pub fn new(services: Vec<Service>) -> Self {
        let digital = crate::cea708::Decoder::new(services.iter().filter_map(|s| {
            (65..=127)
                .contains(&s.channel)
                .then_some(s.channel.wrapping_sub(64) as u8)
        }));
        let teletext = crate::teletext::Decoder::new(
            services
                .iter()
                .filter(|s| (1124..=1923).contains(&s.channel))
                .map(|s| (s.channel - 1024) as u16),
        );
        let dvb = crate::dvb::Decoder::new(
            services
                .iter()
                .filter(|s| s.channel >= 65536)
                .map(|s| (s.channel - 65536) as u16),
        );
        Self {
            dvb_ocr: crate::dvb_ocr::Store::new(&services),
            dvb,
            dvb_frames: VecDeque::new(),
            teletext,
            digital,
            digital_open: BTreeMap::new(),
            services,
            channels: std::array::from_fn(|_| Channel::default()),
            selected: [0, 2],
            history: VecDeque::new(),
            first_pts: None,
            latest_pts: 0,
            error: None,
        }
    }
    fn digital_changes(&mut self, changes: Vec<crate::cea708::Change>) {
        for change in changes {
            let id = 64 + u32::from(change.service);
            if let Some(mut cue) = self.digital_open.remove(&id) {
                cue.end = Some(change.pts.max(cue.start));
                if cue.end != Some(cue.start) {
                    self.history.push_back(cue);
                }
            }
            if !change.text.is_empty() {
                self.digital_open.insert(
                    id,
                    Cue {
                        channel: id,
                        start: change.pts,
                        end: None,
                        text: change.text,
                    },
                );
            }
        }
        while self.history.len() > 4096 {
            self.history.pop_front();
        }
        if self.digital.error.is_some() {
            self.error = self.digital.error;
        }
    }
    // Anchor to the first presentation timestamp, but only execute deadlines
    // and publish clock progress after preceding reordered events are safe.
    pub(crate) fn observe_video(&mut self, pts: u64, frontier: u64) {
        self.first_pts.get_or_insert(pts);
        self.observe(frontier);
    }
    pub(crate) fn teletext_pages(&self) -> Vec<u16> {
        self.teletext.pages()
    }
    pub fn teletext_stats(&self) -> Value {
        self.teletext.stats()
    }
    fn teletext_changes(&mut self, changes: Vec<crate::teletext::Change>) {
        for c in changes {
            let id = 1024 + u32::from(c.page);
            if let Some(mut cue) = self.digital_open.remove(&id) {
                cue.end = Some(c.pts.max(cue.start));
                if cue.end != Some(cue.start) {
                    self.history.push_back(cue);
                }
            }
            if !c.text.is_empty() {
                self.digital_open.insert(
                    id,
                    Cue {
                        channel: id,
                        start: c.pts,
                        end: None,
                        text: c.text,
                    },
                );
            }
        }
        while self.history.len() > 4096 {
            self.history.pop_front();
        }
        if self.teletext.error.is_some() {
            self.error = self.teletext.error;
        }
    }
    pub(crate) fn teletext_bindings(&mut self, b: &BTreeMap<u16, u16>, pts: u64) {
        let c = self.teletext.bindings(b, pts);
        self.teletext_changes(c);
    }
    pub(crate) fn teletext_gap(&mut self, pid: u16, pts: u64) {
        let c = self.teletext.reset_pid(pid, pts);
        self.teletext_changes(c);
    }
    pub(crate) fn push_teletext(&mut self, pid: u16, body: &[u8], pts: u64) {
        let c = self.teletext.push(pid, body, pts);
        self.teletext_changes(c);
    }
    pub fn observe(&mut self, pts: u64) {
        let frames = self.dvb.advance(pts);
        self.dvb_changes(frames);
        let changes = self.teletext.advance(pts);
        self.teletext_changes(changes);
        let changes = self.digital.advance(pts);
        self.digital_changes(changes);
        self.first_pts.get_or_insert(pts);
        self.latest_pts = self.latest_pts.max(pts);
        while self
            .history
            .front()
            .is_some_and(|c| c.end.unwrap_or(c.start).saturating_add(120 * 90000) < self.latest_pts)
        {
            self.history.pop_front();
        }
    }
    pub fn push(&mut self, field: u8, pair: [u8; 2], pts: u64) {
        if field == 2 || field == 3 {
            self.observe(pts);
            let changes = self.digital.push(field, pair, pts);
            self.digital_changes(changes);
            return;
        }
        if field > 1 || pair.iter().any(|b| b.count_ones() % 2 != 1) {
            return;
        }
        self.observe(pts);
        let (a, b) = (pair[0] & 127, pair[1] & 127);
        if a == 0 && b == 0 {
            return;
        }
        let control = (0x10..=0x1f).contains(&a);
        let f = usize::from(field);
        if control {
            self.selected[f] = f * 2 + usize::from(a & 8 != 0)
        }
        let index = self.selected[f];
        let channel = &mut self.channels[index];
        if control {
            if channel.last == Some([a, b]) {
                return;
            }
            channel.last = Some([a, b]);
        } else {
            channel.last = None
        }
        channel.apply(if control { a & !8 } else { a }, b);
        self.update(index, pts);
    }
    fn update(&mut self, index: usize, pts: u64) {
        let channel = &mut self.channels[index];
        let text = channel.text();
        if channel.open.as_ref().is_some_and(|c| c.text == text) {
            return;
        }
        if let Some(mut cue) = channel.open.take() {
            cue.end = Some(pts.max(cue.start));
            if cue.end != Some(cue.start) {
                self.history.push_back(cue)
            }
        }
        if !text.is_empty() {
            channel.open = Some(Cue {
                channel: index as u32 + 1,
                start: pts,
                end: None,
                text,
            })
        }
        while self.history.len() > 4096 {
            self.history.pop_front();
        }
    }
    pub fn reset(&mut self, pts: u64) {
        self.reset_dvb_ocr(&self.dvb.pages(), pts, "dvb_clock_reset");
        let frames = self.dvb.reset(pts);
        self.dvb_changes(frames);
        let changes = self.teletext.reset(pts);
        self.teletext_changes(changes);
        let changes = self.digital.reset(pts);
        self.digital_changes(changes);
        for i in 0..4 {
            self.channels[i].visible = [[' '; 32]; 15];
            self.update(i, pts);
            self.channels[i] = Channel::default()
        }
        self.selected = [0, 2];
    }
    pub fn snapshot(&self) -> Vec<Cue> {
        self.history
            .iter()
            .cloned()
            .chain(self.channels.iter().filter_map(|c| c.open.clone()))
            .chain(self.digital_open.values().cloned())
            .chain(self.dvb_ocr.snapshot())
            .collect()
    }
}
