//! Independent bounded DTVCC service decoder. All deadlines use 90 kHz source PTS.
use std::collections::VecDeque;
#[derive(Debug)]
pub struct Change {
    pub service: u8,
    pub pts: u64,
    pub text: String,
}
struct Packet {
    sequence: u8,
    expected: usize,
    bytes: Vec<u8>,
    started: u64,
}
pub struct Decoder {
    states: Vec<State>,
    packet: Option<Packet>,
    last: Option<(u8, Vec<u8>)>,
    pub error: Option<&'static str>,
}
impl Decoder {
    pub fn new(numbers: impl Iterator<Item = u8>) -> Self {
        Self {
            states: numbers.take(4).map(State::new).collect(),
            packet: None,
            last: None,
            error: None,
        }
    }
    pub fn reset(&mut self, pts: u64) -> Vec<Change> {
        self.packet = None;
        self.last = None;
        self.states
            .iter_mut()
            .filter_map(|s| s.reset(pts))
            .collect()
    }
    fn fail(&mut self, pts: u64, error: &'static str) -> Vec<Change> {
        let changes = self.reset(pts);
        self.error = Some(error);
        changes
    }
    pub fn advance(&mut self, pts: u64) -> Vec<Change> {
        if self
            .packet
            .as_ref()
            .is_some_and(|p| pts.saturating_sub(p.started) > 5 * 90000)
        {
            return self.fail(pts, "caption_708_packet_timeout");
        }
        let mut out = vec![];
        for s in &mut self.states {
            s.advance(pts, &mut out);
        }
        out
    }
    pub fn push(&mut self, kind: u8, pair: [u8; 2], pts: u64) -> Vec<Change> {
        let mut out = self.advance(pts);
        if self.states.is_empty() {
            return out;
        }
        if kind == 3 {
            let sequence = pair[0] >> 6;
            let mut size = usize::from(pair[0] & 63);
            if size == 0 {
                size = 64;
            }
            if self.packet.is_some()
                || self
                    .last
                    .as_ref()
                    .is_some_and(|(s, _)| sequence != *s && sequence != ((*s + 1) & 3))
            {
                out.extend(self.fail(pts, "caption_708_packet_gap"));
            }
            self.packet = Some(Packet {
                sequence,
                expected: size * 2 - 1,
                bytes: vec![pair[1]],
                started: pts,
            });
        } else if kind == 2 {
            let Some(p) = &mut self.packet else {
                out.extend(self.fail(pts, "caption_708_orphan_data"));
                return out;
            };
            p.bytes.extend(pair);
        } else {
            return out;
        }
        let Some(p) = &self.packet else {
            return out;
        };
        if p.bytes.len() != p.expected {
            return out;
        }
        let p = self.packet.take().unwrap();
        if self
            .last
            .as_ref()
            .is_some_and(|(s, b)| *s == p.sequence && *b == p.bytes)
        {
            return out;
        }
        if self.last.as_ref().is_some_and(|(s, _)| *s == p.sequence) {
            out.extend(self.fail(pts, "caption_708_sequence_reuse"));
        }
        // Validate the complete framing before applying any service data.
        let mut blocks = vec![];
        let mut i = 0;
        while i < p.bytes.len() {
            let h = p.bytes[i];
            i += 1;
            let mut service = h >> 5;
            let length = usize::from(h & 31);
            if service == 0 {
                if length != 0 {
                    out.extend(self.fail(pts, "caption_708_block_size"));
                    return out;
                }
                continue;
            }
            if service == 7 {
                let Some(b) = p.bytes.get(i) else {
                    out.extend(self.fail(pts, "caption_708_extended_service"));
                    return out;
                };
                service = b & 63;
                i += 1;
                if service < 7 || b & 0xc0 != 0 {
                    out.extend(self.fail(pts, "caption_708_extended_service"));
                    return out;
                }
            }
            if length == 0 || i + length > p.bytes.len() {
                out.extend(self.fail(pts, "caption_708_block_size"));
                return out;
            }
            blocks.push((service, i, length));
            i += length;
        }
        self.last = Some((p.sequence, p.bytes.clone()));
        for (service, start, length) in blocks {
            if let Some(s) = self.states.iter_mut().find(|s| s.number == service) {
                if !s.feed(&p.bytes[start..start + length], pts, &mut out) {
                    self.error = Some("caption_708_command_limit");
                    if let Some(c) = s.reset(pts) {
                        out.push(c);
                    }
                }
            }
        }
        out
    }
}
#[derive(Clone)]
struct Window {
    cells: [[char; 42]; 16],
    rows: usize,
    cols: usize,
    row: i16,
    col: i16,
    visible: bool,
    vertical: u8,
    horizontal: u8,
    relative: bool,
    priority: u8,
    print: u8,
    scroll: u8,
    wrap: bool,
}
impl Default for Window {
    fn default() -> Self {
        Self {
            cells: [[' '; 42]; 16],
            rows: 1,
            cols: 32,
            row: 0,
            col: 0,
            visible: false,
            vertical: 0,
            horizontal: 0,
            relative: false,
            priority: 0,
            print: 0,
            scroll: 3,
            wrap: false,
        }
    }
}
impl Window {
    fn clear(&mut self) {
        self.cells = [[' '; 42]; 16];
        self.row = 0;
        self.col = 0;
    }
    fn text(&self) -> String {
        let rows: Vec<String> = self.cells[..self.rows]
            .iter()
            .map(|r| {
                r[..self.cols]
                    .iter()
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect();
        let start = rows
            .iter()
            .position(|r| !r.is_empty())
            .unwrap_or(rows.len());
        let end = rows
            .iter()
            .rposition(|r| !r.is_empty())
            .map_or(start, |r| r + 1);
        rows[start..end].join("\n")
    }
    fn bounds(&self) -> bool {
        self.row >= 0
            && self.col >= 0
            && (self.row as usize) < self.rows
            && (self.col as usize) < self.cols
    }
    fn step(&mut self, back: bool) {
        let n = if back { -1 } else { 1 };
        match self.print {
            0 => self.col += n,
            1 => self.col -= n,
            2 => self.row += n,
            _ => self.row -= n,
        }
    }
    fn put(&mut self, c: char) {
        if !self.bounds() && self.wrap {
            let mut word = Vec::new();
            if self.print == 0 && c != ' ' && self.row >= 0 && (self.row as usize) < self.rows {
                let row = &mut self.cells[self.row as usize];
                if let Some(space) = row[..self.cols].iter().rposition(|v| *v == ' ') {
                    word.extend_from_slice(&row[space + 1..self.cols]);
                    row[space..self.cols].fill(' ');
                }
            }
            self.carriage();
            for ch in word {
                self.put(ch);
            }
        }
        if self.bounds() {
            self.cells[self.row as usize][self.col as usize] = c;
            self.step(false);
        }
    }
    fn backspace(&mut self) {
        self.step(true);
        if self.bounds() {
            self.cells[self.row as usize][self.col as usize] = ' ';
        } else {
            self.step(false);
        }
    }
    fn carriage(&mut self) {
        match self.print {
            0 => self.col = 0,
            1 => self.col = self.cols as i16 - 1,
            2 => self.row = 0,
            _ => self.row = self.rows as i16 - 1,
        }
        match self.scroll {
            3 => {
                self.row += 1;
                if self.row >= self.rows as i16 {
                    for r in 1..self.rows {
                        self.cells[r - 1] = self.cells[r];
                    }
                    self.cells[self.rows - 1] = [' '; 42];
                    self.row = self.rows as i16 - 1;
                }
            }
            2 => {
                self.row -= 1;
                if self.row < 0 {
                    for r in (1..self.rows).rev() {
                        self.cells[r] = self.cells[r - 1];
                    }
                    self.cells[0] = [' '; 42];
                    self.row = 0;
                }
            }
            1 => {
                self.col += 1;
                if self.col >= self.cols as i16 {
                    for r in &mut self.cells[..self.rows] {
                        for c in 1..self.cols {
                            r[c - 1] = r[c];
                        }
                        r[self.cols - 1] = ' ';
                    }
                    self.col = self.cols as i16 - 1;
                }
            }
            _ => {
                self.col -= 1;
                if self.col < 0 {
                    for r in &mut self.cells[..self.rows] {
                        for c in (1..self.cols).rev() {
                            r[c] = r[c - 1];
                        }
                        r[0] = ' ';
                    }
                    self.col = 0;
                }
            }
        }
    }
    fn hcr(&mut self) {
        if self.row >= 0 && (self.row as usize) < self.rows {
            self.cells[self.row as usize] = [' '; 42];
        }
        self.col = if self.print == 1 {
            self.cols as i16 - 1
        } else {
            0
        };
    }
}
struct State {
    number: u8,
    windows: [Option<Window>; 8],
    current: Option<usize>,
    partial: Vec<u8>,
    queue: VecDeque<Vec<u8>>,
    queued: usize,
    delay: Option<u64>,
    visible: String,
}
impl State {
    fn new(number: u8) -> Self {
        Self {
            number,
            windows: std::array::from_fn(|_| None),
            current: None,
            partial: vec![],
            queue: VecDeque::new(),
            queued: 0,
            delay: None,
            visible: String::new(),
        }
    }
    fn reset(&mut self, pts: u64) -> Option<Change> {
        let text = std::mem::take(&mut self.visible);
        let number = self.number;
        *self = Self::new(number);
        (!text.is_empty()).then_some(Change {
            service: number,
            pts,
            text: String::new(),
        })
    }
    fn changed(&mut self, pts: u64, out: &mut Vec<Change>) {
        let mut windows: Vec<_> = self
            .windows
            .iter()
            .enumerate()
            .filter_map(|(i, w)| w.as_ref().filter(|w| w.visible).map(|w| (i, w)))
            .collect();
        windows.sort_by_key(|(i, w)| {
            (
                u16::from(w.vertical) * if w.relative { 75 } else { 50 },
                u16::from(w.horizontal) * if w.relative { 210 } else { 100 },
                w.priority,
                *i,
            )
        });
        let text = windows
            .iter()
            .map(|(_, w)| w.text())
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        if text != self.visible {
            self.visible = text.clone();
            out.push(Change {
                service: self.number,
                pts,
                text,
            });
        }
    }
    fn advance(&mut self, pts: u64, out: &mut Vec<Change>) {
        while let Some(deadline) = self.delay.filter(|t| *t <= pts) {
            self.delay = None;
            self.drain(deadline, out);
        }
    }
    fn drain(&mut self, pts: u64, out: &mut Vec<Change>) {
        while self.delay.is_none() {
            let Some(t) = self.queue.pop_front() else {
                break;
            };
            self.queued -= t.len();
            self.execute(&t, pts, out);
        }
    }
    fn feed(&mut self, bytes: &[u8], pts: u64, out: &mut Vec<Change>) -> bool {
        self.partial.extend_from_slice(bytes);
        while !self.partial.is_empty() {
            let Some(length) = token_len(&self.partial) else {
                break;
            };
            if self.partial.len() < length {
                break;
            }
            let t: Vec<_> = self.partial.drain(..length).collect();
            if t[0] == 0x8f {
                let pending = std::mem::take(&mut self.partial);
                if let Some(c) = self.reset(pts) {
                    out.push(c);
                }
                self.partial = pending;
                continue;
            }
            if t[0] == 0x8e {
                self.delay = None;
                self.drain(pts, out);
                continue;
            }
            if self.delay.is_some() {
                self.queued += t.len();
                self.queue.push_back(t);
                if self.queued > 4096 || self.queue.len() > 512 {
                    return false;
                }
            } else {
                self.execute(&t, pts, out);
            }
        }
        self.partial.len() <= 66
    }
    fn current(&mut self) -> Option<&mut Window> {
        self.current.and_then(|i| self.windows[i].as_mut())
    }
    fn execute(&mut self, t: &[u8], pts: u64, out: &mut Vec<Change>) {
        match t[0] {
            0x08 => {
                if let Some(w) = self.current() {
                    w.backspace();
                }
            }
            0x0c => {
                if let Some(w) = self.current() {
                    w.clear();
                }
            }
            0x0d => {
                if let Some(w) = self.current() {
                    w.carriage();
                }
            }
            0x0e => {
                if let Some(w) = self.current() {
                    w.hcr();
                }
            }
            0x10 => {
                let ext = t[1];
                let c = if (0x20..=0x7f).contains(&ext) {
                    Some(g2(ext))
                } else if ext >= 0xa0 {
                    Some(if ext == 0xa0 { '\u{1f16d}' } else { '_' })
                } else {
                    None
                };
                if let (Some(c), Some(w)) = (c, self.current()) {
                    w.put(c);
                }
            }
            0x18 => {
                if let Some(w) = self.current() {
                    w.put(
                        char::from_u32(u32::from(u16::from_be_bytes([t[1], t[2]])))
                            .filter(|c| !c.is_control())
                            .unwrap_or('\u{fffd}'),
                    );
                }
            }
            0x20..=0x7f | 0xa0..=0xff => {
                if let Some(w) = self.current() {
                    w.put(if t[0] == 0x7f {
                        '♪'
                    } else {
                        char::from(t[0])
                    });
                }
            }
            0x80..=0x87 => {
                let i = usize::from(t[0] & 7);
                if self.windows[i].is_some() {
                    self.current = Some(i);
                }
            }
            0x88..=0x8c => {
                for i in 0..8 {
                    if t[1] & (1 << i) != 0 {
                        if t[0] == 0x8c {
                            self.windows[i] = None;
                            if self.current == Some(i) {
                                self.current = None;
                            }
                        } else if let Some(w) = &mut self.windows[i] {
                            match t[0] {
                                0x88 => w.clear(),
                                0x89 => w.visible = true,
                                0x8a => w.visible = false,
                                _ => w.visible = !w.visible,
                            }
                        }
                    }
                }
            }
            0x8d => {
                if t[1] > 0 {
                    self.delay = Some(pts.saturating_add(u64::from(t[1]) * 9000));
                }
            }
            0x92 => {
                if let Some(w) = self.current() {
                    w.row = i16::from(t[1] & 15);
                    w.col = i16::from(t[2] & 63);
                }
            }
            0x97 => {
                if let Some(w) = self.current() {
                    w.wrap = t[3] & 0x40 != 0;
                    w.print = (t[3] >> 4) & 3;
                    w.scroll = (t[3] >> 2) & 3;
                }
            }
            0x98..=0x9f => {
                let i = usize::from(t[0] & 7);
                let existed = self.windows[i].is_some();
                let w = self.windows[i].get_or_insert_with(Window::default);
                w.visible = t[1] & 0x20 != 0;
                w.priority = t[1] & 7;
                w.relative = t[2] & 0x80 != 0;
                w.vertical = t[2] & 127;
                w.horizontal = t[3];
                w.rows = usize::from(t[4] & 15) + 1;
                w.cols = (usize::from(t[5] & 63) + 1).min(42);
                // Data outside a reduced window never reappears on later resize.
                for r in 0..16 {
                    for c in 0..42 {
                        if r >= w.rows || c >= w.cols {
                            w.cells[r][c] = ' ';
                        }
                    }
                }
                let style = (t[6] >> 3) & 7;
                if !existed || style != 0 {
                    w.print = 0;
                    w.scroll = 3;
                    w.wrap = matches!(style, 4..=6);
                }
                if !existed || t[6] & 7 != 0 {
                    w.row = 0;
                    w.col = 0;
                }
                self.current = Some(i);
            }
            _ => {}
        }
        self.changed(pts, out);
    }
}
fn token_len(t: &[u8]) -> Option<usize> {
    Some(match t[0] {
        0x10 => {
            let b = *t.get(1)?;
            match b {
                0x00..=0x07 => 2,
                0x08..=0x0f => 3,
                0x10..=0x17 => 4,
                0x18..=0x1f => 5,
                0x80..=0x87 => 6,
                0x88..=0x8f => 7,
                0x90..=0x9f => 3 + usize::from(*t.get(2)? & 63),
                _ => 2,
            }
        }
        0x11..=0x17 => 2,
        0x18..=0x1f => 3,
        0x88..=0x8d => 2,
        0x90 => 3,
        0x91 => 4,
        0x92 => 3,
        0x97 => 5,
        0x98..=0x9f => 7,
        _ => 1,
    })
}
fn g2(c: u8) -> char {
    match c {
        0x20 => ' ',
        0x21 => '\u{a0}',
        0x25 => '…',
        0x2a => 'Š',
        0x2c => 'Œ',
        0x30 => '█',
        0x31 => '‘',
        0x32 => '’',
        0x33 => '“',
        0x34 => '”',
        0x35 => '•',
        0x39 => '™',
        0x3a => 'š',
        0x3c => 'œ',
        0x3d => '℠',
        0x3f => 'Ÿ',
        0x76 => '⅛',
        0x77 => '⅜',
        0x78 => '⅝',
        0x79 => '⅞',
        0x7a => '│',
        0x7b => '┐',
        0x7c => '└',
        0x7d => '─',
        0x7e => '┘',
        0x7f => '┌',
        _ => '_',
    }
}
