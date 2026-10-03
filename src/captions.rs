//! Independent, bounded CEA-608 display state. Times are source video PTS (90 kHz).
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub channel: u8,
    pub language: String,
    pub name: String,
}
pub fn configuration(cfg: &Value) -> Result<Vec<Service>, String> {
    let Some(rows) = cfg.get("flussonix_hls_captions") else {
        return Ok(vec![]);
    };
    let services: Vec<Service> = serde_json::from_value(rows.clone())
        .map_err(|_| "HLS captions require rows with channel, language and name")?;
    if services.len() > 4 {
        return Err("at most four HLS caption channels are supported".into());
    }
    let mut seen = 0u8;
    for s in &services {
        if !(1..=4).contains(&s.channel) || seen & (1 << s.channel) != 0 {
            return Err("caption channels must be distinct CC1..CC4".into());
        }
        seen |= 1 << s.channel;
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
    Ok(services)
}
#[derive(Clone, Debug)]
pub struct Cue {
    pub channel: u8,
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
        if self.mode == Mode::Text {
            return;
        }
        let (r, col) = (self.row, self.col);
        self.screen()[r][col] = c;
        self.col = (col + 1).min(31)
    }
    fn backspace(&mut self) {
        self.col = self.col.saturating_sub(1);
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
        if a == 0x14 && (0x20..=0x2f).contains(&b) {
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
    pub first_pts: Option<u64>,
    pub latest_pts: u64,
    pub error: Option<&'static str>,
}
impl Decoder {
    pub fn new(services: Vec<Service>) -> Self {
        Self {
            services,
            channels: std::array::from_fn(|_| Channel::default()),
            selected: [0, 2],
            history: VecDeque::new(),
            first_pts: None,
            latest_pts: 0,
            error: None,
        }
    }
    pub fn observe(&mut self, pts: u64) {
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
                channel.last = None;
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
                channel: index as u8 + 1,
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
            .collect()
    }
}
