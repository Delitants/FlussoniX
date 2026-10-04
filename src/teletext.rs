//! Independently implemented bounded ETSI Level 1 subtitle pages.
use std::collections::{BTreeMap, BTreeSet};
pub(crate) struct Change {
    pub page: u16,
    pub pts: u64,
    pub text: String,
}
struct Page {
    pid: Option<u16>,
    rows: [[u8; 40]; 24],
    national: u8,
    boxed: bool,
    inhibit: bool,
    sub: Option<u16>,
    pending: Option<(u64, u64)>,
    visible: String,
    blocked: bool,
}
impl Default for Page {
    fn default() -> Self {
        Self {
            pid: None,
            rows: [[0x20; 40]; 24],
            national: 0,
            boxed: true,
            inhibit: false,
            sub: None,
            pending: None,
            visible: String::new(),
            blocked: false,
        }
    }
}
impl Page {
    fn text(&self) -> String {
        if self.inhibit || self.blocked {
            return String::new();
        }
        let mut rows = vec![];
        for row in &self.rows {
            let (mut inside, mut concealed, mut graphics) = (!self.boxed, false, false);
            let mut line = String::new();
            for (i, &b) in row.iter().enumerate() {
                let c = b & 127;
                match c {
                    0..=7 => {
                        graphics = false;
                        concealed = false;
                    }
                    0x0a => inside = !self.boxed,
                    0x0b if row.get(i + 1).is_some_and(|n| n & 127 == 0x0b) => inside = true,
                    0x10..=0x17 => {
                        graphics = true;
                        concealed = false;
                    }
                    0x18 => concealed = true,
                    _ => {}
                }
                let ch =
                    if c < 32 || !inside || concealed || (graphics && !(0x40..0x60).contains(&c)) {
                        ' '
                    } else if b & 128 != 0 {
                        '�'
                    }
                    // parity replacement marker stored by push
                    else {
                        character(c, self.national)
                    };
                line.push(ch);
            }
            rows.push(line.trim().to_owned());
        }
        let first = rows
            .iter()
            .position(|r| !r.is_empty())
            .unwrap_or(rows.len());
        let end = rows
            .iter()
            .rposition(|r| !r.is_empty())
            .map_or(first, |i| i + 1);
        rows[first..end].join("\n")
    }
    fn commit(&mut self, page: u16) -> Option<Change> {
        let (pts, _) = self.pending.take()?;
        let text = self.text();
        if text == self.visible {
            return None;
        }
        self.visible = text.clone();
        Some(Change { page, pts, text })
    }
    fn clear(&mut self, page: u16, pts: u64) -> Option<Change> {
        let pid = self.pid;
        let visible = std::mem::take(&mut self.visible);
        *self = Self::default();
        self.pid = pid;
        (!visible.is_empty()).then(|| Change {
            page,
            pts,
            text: String::new(),
        })
    }
}
pub(crate) struct Decoder {
    pages: BTreeMap<u16, Page>,
    active: BTreeMap<u16, [Option<u16>; 8]>,
    serial: BTreeMap<u16, bool>,
    unsupported: BTreeSet<(u16, u8)>,
    pub error: Option<&'static str>,
}
impl Decoder {
    pub fn new(pages: impl IntoIterator<Item = u16>) -> Self {
        Self {
            pages: pages
                .into_iter()
                .take(4)
                .map(|p| (p, Page::default()))
                .collect(),
            active: BTreeMap::new(),
            serial: BTreeMap::new(),
            unsupported: BTreeSet::new(),
            error: None,
        }
    }
    pub fn pages(&self) -> Vec<u16> {
        self.pages.keys().copied().collect()
    }
    pub fn bindings(&mut self, bindings: &BTreeMap<u16, u16>, pts: u64) -> Vec<Change> {
        let mut changes = vec![];
        let mut changed = BTreeSet::new();
        for (&number, page) in &mut self.pages {
            let pid = bindings.get(&number).copied();
            if page.pid != pid {
                changed.extend(page.pid);
                changed.extend(pid);
                changes.extend(page.clear(number, pts));
                page.pid = pid;
            }
            if pid.is_none() {
                self.error = Some("teletext_page_unavailable");
            }
        }
        self.active.retain(|pid, _| !changed.contains(pid));
        self.serial.retain(|pid, _| !changed.contains(pid));
        self.unsupported.retain(|(pid, _)| !changed.contains(pid));
        changes
    }
    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!(self.pages.iter().map(|(&p,s)|serde_json::json!({"page":p,"pid":s.pid,"status":if s.pid.is_none(){"unavailable"}else if s.blocked{"unsupported"}else{"available"}})).collect::<Vec<_>>())
    }
    pub fn reset(&mut self, pts: u64) -> Vec<Change> {
        self.active.clear();
        self.serial.clear();
        self.unsupported.clear();
        self.pages
            .iter_mut()
            .filter_map(|(&n, p)| p.clear(n, pts))
            .collect()
    }
    pub fn reset_pid(&mut self, pid: u16, pts: u64) -> Vec<Change> {
        self.active.remove(&pid);
        self.serial.remove(&pid);
        self.unsupported.retain(|(p, _)| *p != pid);
        self.pages
            .iter_mut()
            .filter(|(_, p)| p.pid == Some(pid))
            .filter_map(|(&n, p)| p.clear(n, pts))
            .collect()
    }
    pub fn advance(&mut self, frontier: u64) -> Vec<Change> {
        self.pages
            .iter_mut()
            .filter(|(_, p)| {
                p.pending
                    .is_some_and(|(_, last)| last.saturating_add(9000) <= frontier)
            })
            .filter_map(|(&n, p)| p.commit(n))
            .collect()
    }
    pub fn push(&mut self, pid: u16, body: &[u8], pts: u64) -> Vec<Change> {
        // Validate all length framing before applying any part of this PES.
        let mut at = 1;
        if !body.first().is_some_and(|b| (0x10..=0x1f).contains(b)) {
            self.error = Some("teletext_data_identifier");
            return self.reset_pid(pid, pts);
        }
        while at < body.len() {
            let Some(h) = body.get(at..at + 2) else {
                self.error = Some("teletext_unit_length");
                return self.reset_pid(pid, pts);
            };
            let n = usize::from(h[1]);
            if at + 2 + n > body.len() || (matches!(h[0], 2 | 3) && n != 44) {
                self.error = Some("teletext_unit_length");
                return self.reset_pid(pid, pts);
            }
            at += 2 + n;
        }
        let mut changes = vec![];
        at = 1;
        while at < body.len() {
            let id = body[at];
            let n = usize::from(body[at + 1]);
            if matches!(id, 2 | 3) {
                changes.extend(self.unit(pid, &body[at + 2..at + 2 + n], pts));
            }
            at += 2 + n;
        }
        changes
    }
    fn unit(&mut self, pid: u16, u: &[u8], pts: u64) -> Vec<Change> {
        if u[1] != 0xe4 {
            self.error = Some("teletext_framing");
            return self.reset_pid(pid, pts);
        }
        let Some(a) = unham(u[2].reverse_bits())
            .zip(unham(u[3].reverse_bits()))
            .map(|(a, b)| a | (b << 4))
        else {
            self.error = Some("teletext_hamming");
            return self.reset_pid(pid, pts);
        };
        let mag = a & 7;
        let row = a >> 3;
        let bytes: [u8; 40] = u[4..].try_into().unwrap();
        let bytes = bytes.map(u8::reverse_bits);
        let mut out = vec![];
        if row == 0 {
            let mut h = [0; 8];
            for (i, &b) in bytes[..8].iter().enumerate() {
                let Some(n) = unham(b) else {
                    self.error = Some("teletext_hamming");
                    return self.reset_pid(pid, pts);
                };
                h[i] = n;
            }
            let serial = self.serial.get(&pid).copied().unwrap_or(false) || h[7] & 1 != 0;
            let slots = self.active.entry(pid).or_insert([None; 8]);
            for (i, slot) in slots.iter_mut().enumerate() {
                if serial || i == usize::from(mag) {
                    if let Some(n) = slot.take() {
                        if let Some(p) = self.pages.get_mut(&n) {
                            out.extend(p.commit(n));
                        }
                    }
                }
            }
            self.serial.insert(pid, h[7] & 1 != 0);
            let magazine = if mag == 0 { 8 } else { u16::from(mag) };
            let page = magazine * 100 + u16::from(h[1]) * 10 + u16::from(h[0]);
            if h[0] > 9 || h[1] > 9 {
                return out;
            }
            if let Some(p) = self.pages.get_mut(&page).filter(|p| p.pid == Some(pid)) {
                let sub = u16::from(h[2])
                    | (u16::from(h[3] & 7) << 4)
                    | (u16::from(h[4]) << 7)
                    | (u16::from(h[5] & 3) << 11);
                if h[3] & 8 != 0 || p.sub.is_some_and(|s| s != sub) {
                    p.rows = [[0x20; 40]; 24];
                }
                p.sub = Some(sub);
                p.boxed = h[5] & 12 != 0;
                p.inhibit = h[6] & 8 != 0;
                p.national = ((h[7] & 2) << 1) | (h[7] & 4) >> 1 | (h[7] & 8) >> 3;
                p.blocked = p.national == 7 || self.unsupported.contains(&(pid, mag));
                if p.blocked {
                    self.error = Some("teletext_character_set_unsupported");
                }
                p.pending = Some((pts, pts));
                self.active.get_mut(&pid).unwrap()[usize::from(mag)] = Some(page);
            }
            return out;
        }
        if row == 29 {
            self.unsupported.insert((pid, mag));
            self.error = Some("teletext_enhancement_unsupported");
            for (&n, p) in &mut self.pages {
                if p.pid == Some(pid) && ((n / 100) % 8) as u8 == mag {
                    p.blocked = true;
                    p.pending = Some((pts, pts));
                    out.extend(p.commit(n));
                }
            }
            return out;
        }
        let Some(number) = self.active.get(&pid).and_then(|s| s[usize::from(mag)]) else {
            return out;
        };
        let p = self.pages.get_mut(&number).unwrap();
        if matches!(row, 26 | 28) {
            p.blocked = true;
            self.error = Some("teletext_enhancement_unsupported");
            p.pending = Some((pts, pts));
            out.extend(p.commit(number));
            return out;
        }
        if !(1..=24).contains(&row) {
            return out;
        }
        let data = bytes.map(|b| {
            if b.count_ones() % 2 == 1 {
                b & 127
            } else {
                self.error = Some("teletext_character_parity");
                0xff
            }
        });
        if data.contains(&0x1b) {
            p.blocked = true;
            self.error = Some("teletext_character_set_unsupported");
        }
        p.rows[usize::from(row - 1)] = data;
        if let Some((_, last)) = p.pending.as_mut() {
            *last = pts;
        } else {
            p.pending = Some((pts, pts));
        }
        out
    }
}
fn unham(b: u8) -> Option<u8> {
    // ETSI Hamming8/4 codewords, nearest distance<=1 corrects a single bit.
    const C: [u8; 16] = [
        0x15, 0x02, 0x49, 0x5e, 0x64, 0x73, 0x38, 0x2f, 0xd0, 0xc7, 0x8c, 0x9b, 0xa1, 0xb6, 0xfd,
        0xea,
    ];
    C.iter()
        .position(|v| (v ^ b).count_ones() <= 1)
        .map(|n| n as u8)
}
fn character(c: u8, national: u8) -> char {
    // Level1 default Western/Central Europe national subsets (EN300706 table36).
    const POS: [u8; 13] = [
        0x23, 0x24, 0x40, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f, 0x60, 0x7b, 0x7c, 0x7d, 0x7e,
    ];
    const CHARS: [&str; 7] = [
        "£$@←½→↑#—¼‖¾÷",
        "#$§ÄÖÜ^_°äöüß",
        "#¤ÉÄÖÅÜ_éäöåü",
        "£$é°ç→↑#ùàòèì",
        "éïàëêùî#èâôûç",
        "ç$¡áéíóú¿üñèà",
        "#ůčťžýířéáěúš",
    ];
    if let Some(i) = POS.iter().position(|&p| p == c) {
        return CHARS[usize::from(national.min(6))].chars().nth(i).unwrap();
    }
    if c == 0x7f { '■' } else { char::from(c) }
}
