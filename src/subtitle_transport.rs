//! Shared bounded PSI/PES carriage for announced teletext and DVB bitmap pages.
use crate::captions::Decoder;
use std::collections::{BTreeMap, BTreeSet};
const MASK: u64 = (1 << 33) - 1;
const PES_LIMIT: usize = 65541;
#[derive(Default)]
pub(crate) struct Psi {
    bytes: Vec<u8>,
    counter: Option<u8>,
}
impl Psi {
    pub(crate) fn push(&mut self, b: &[u8], start: bool, cc: u8) -> Vec<Vec<u8>> {
        let mut out = vec![];
        if !start && self.counter.is_some_and(|old| (old + 1) & 15 != cc) {
            self.bytes.clear();
        }
        self.counter = Some(cc);
        if start {
            let Some(&pointer) = b.first() else {
                return out;
            };
            let off = 1 + usize::from(pointer);
            if off > b.len() {
                self.bytes.clear();
                return out;
            }
            if !self.bytes.is_empty() {
                self.bytes.extend(&b[1..off]);
                self.take(&mut out);
            }
            self.bytes.clear();
            self.bytes.extend(&b[off..]);
        } else if !self.bytes.is_empty() {
            self.bytes.extend(b);
        }
        self.take(&mut out);
        out
    }
    fn take(&mut self, out: &mut Vec<Vec<u8>>) {
        while self.bytes.len() >= 3 {
            if self.bytes[0] == 0xff {
                self.bytes.clear();
                break;
            }
            let n = 3 + ((usize::from(self.bytes[1] & 15) << 8) | usize::from(self.bytes[2]));
            if !(12..=4096).contains(&n) {
                self.bytes.clear();
                break;
            }
            if self.bytes.len() < n {
                break;
            }
            let section: Vec<_> = self.bytes.drain(..n).collect();
            if crc(&section) == 0 && section[5] & 1 != 0 && section[6] == 0 && section[7] == 0 {
                out.push(section);
            }
        }
        if self.bytes.len() > 4096 {
            self.bytes.clear();
        }
    }
}
#[derive(Default)]
struct Pes {
    bytes: Vec<u8>,
    counter: Option<u8>,
    last: Option<[u8; 188]>,
}
struct Event {
    pid: u16,
    raw: u64,
    body: Vec<u8>,
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Binding {
    pid: u16,
    ancillary: u16,
}
#[derive(Default)]
pub(crate) struct Transport<const DVB: bool = false> {
    pat: Psi,
    pmt: Psi,
    pmt_pid: Option<u16>,
    program: Option<u16>,
    bindings: BTreeMap<u16, Binding>,
    streams: BTreeMap<u16, Pes>,
    events: Vec<Event>,
    clock: Option<u64>,
    seen_pmt: bool,
}
impl<const DVB: bool> Transport<DVB> {
    fn reason(tt: &'static str) -> &'static str {
        if !DVB {
            return tt;
        }
        match tt {
            "teletext_transport_gap" => "dvb_transport_gap",
            "teletext_pes_truncated" => "dvb_pes_truncated",
            "teletext_pes_limit" => "dvb_pes_limit",
            "teletext_pes_header" => "dvb_pes_header",
            "teletext_pes_timestamp" => "dvb_pes_timestamp",
            "teletext_reorder_limit" => "dvb_reorder_limit",
            _ => "dvb_page_ambiguous",
        }
    }
    pub fn reset(&mut self) {
        for p in self.streams.values_mut() {
            *p = Pes::default();
        }
        self.events.clear();
        self.clock = None;
    }
    fn gap(&mut self, pid: u16, d: &mut Decoder, reason: &'static str) {
        if let Some(p) = self.streams.get_mut(&pid) {
            *p = Pes::default();
        }
        self.events.retain(|e| e.pid != pid);
        if DVB {
            d.dvb_gap(pid, d.latest_pts);
        } else {
            d.teletext_gap(pid, d.latest_pts);
        }
        d.error = Some(reason);
    }
    pub fn packet(&mut self, p: &[u8; 188], d: &mut Decoder) {
        if !d.services.iter().any(|s| {
            if DVB {
                s.channel >= 65536
            } else {
                (1124..=1923).contains(&s.channel)
            }
        }) {
            return;
        }
        let pid = (u16::from(p[1] & 31) << 8) | u16::from(p[2]);
        let start = p[1] & 0x40 != 0;
        let off = if p[3] & 0x20 != 0 {
            5 + usize::from(p[4])
        } else {
            4
        };
        if off > 188 || p[1] & 0x80 != 0 || p[3] & 0xc0 != 0 || p[3] & 0x30 == 0 {
            if self.streams.contains_key(&pid) {
                self.gap(pid, d, Self::reason("teletext_transport_gap"));
            }
            if pid == 0 {
                self.pat = Psi::default();
            } else if self.pmt_pid == Some(pid) {
                self.pmt = Psi::default();
            }
            return;
        }
        if p[3] & 0x20 != 0 && p[4] > 0 && p[5] & 0x80 != 0 {
            if self.streams.contains_key(&pid) {
                self.gap(pid, d, Self::reason("teletext_transport_gap"));
            }
            if pid == 0 {
                self.pat = Psi::default();
            } else if self.pmt_pid == Some(pid) {
                self.pmt = Psi::default();
            }
        }
        if p[3] & 0x10 == 0 || off == 188 {
            return;
        }
        let bytes = &p[off..];
        let cc = p[3] & 15;
        if pid == 0 {
            for s in self.pat.push(bytes, start, cc) {
                if s[0] != 0 {
                    continue;
                }
                let next = s[8..s.len() - 4]
                    .chunks_exact(4)
                    .find(|e| e[0] != 0 || e[1] != 0)
                    .map(|e| {
                        (
                            u16::from_be_bytes([e[0], e[1]]),
                            (u16::from(e[2] & 31) << 8) | u16::from(e[3]),
                        )
                    });
                if self.program.zip(self.pmt_pid) != next {
                    self.program = next.map(|(program, _)| program);
                    self.pmt_pid = next.map(|(_, pid)| pid);
                    self.pmt = Psi::default();
                    if self.seen_pmt {
                        self.set_bindings(BTreeMap::new(), d);
                    }
                    self.seen_pmt = false;
                }
            }
            return;
        }
        if self.pmt_pid == Some(pid) {
            for s in self.pmt.push(bytes, start, cc) {
                if s[0] == 2 && self.program == Some(u16::from_be_bytes([s[3], s[4]])) {
                    self.announcements(&s, d);
                }
            }
            return;
        }
        let Some(pes) = self.streams.get_mut(&pid) else {
            return;
        };
        if pes.counter == Some(cc) && pes.last.as_ref() == Some(p) {
            return;
        }
        let gap = pes.counter.is_some_and(|old| (old + 1) & 15 != cc);
        if gap {
            self.gap(pid, d, Self::reason("teletext_transport_gap"));
        }
        let pes = self.streams.get_mut(&pid).unwrap();
        pes.counter = Some(cc);
        pes.last = Some(*p);
        if start {
            if !pes.bytes.is_empty() {
                self.gap(pid, d, Self::reason("teletext_pes_truncated"));
                self.streams.get_mut(&pid).unwrap().counter = Some(cc);
                self.streams.get_mut(&pid).unwrap().last = Some(*p);
            }
            self.streams.get_mut(&pid).unwrap().bytes.extend(bytes);
        } else if !pes.bytes.is_empty() {
            pes.bytes.extend(bytes);
        } else {
            return;
        }
        let pes = self.streams.get_mut(&pid).unwrap();
        if pes.bytes.len() > PES_LIMIT {
            self.gap(pid, d, Self::reason("teletext_pes_limit"));
            return;
        }
        if pes.bytes.len() < 6 {
            return;
        }
        let n = 6 + ((usize::from(pes.bytes[4]) << 8) | usize::from(pes.bytes[5]));
        if n < 14 || pes.bytes[..4] != [0, 0, 1, 0xbd] {
            self.gap(pid, d, Self::reason("teletext_pes_header"));
            return;
        }
        if pes.bytes.len() < n {
            return;
        }
        let b = std::mem::take(&mut pes.bytes);
        let off = 9 + usize::from(b[8]);
        let raw = crate::caption_transport::pts(&b[9..14]);
        if b[7] & 0xc0 != 0x80 || b[8] < 5 || b[9] >> 4 != 2 || off > n || raw.is_none() {
            self.gap(pid, d, Self::reason("teletext_pes_timestamp"));
            return;
        }
        if self.events.len() >= 64
            || self.events.iter().map(|e| e.body.len()).sum::<usize>() + n - off > 256 * 1024
        {
            self.gap(pid, d, Self::reason("teletext_reorder_limit"));
            return;
        }
        self.events.push(Event {
            pid,
            raw: raw.unwrap(),
            body: b[off..n].to_vec(),
        });
        if let Some(t) = self.clock {
            self.advance(t, d);
        }
    }
    fn announcements(&mut self, s: &[u8], d: &mut Decoder) {
        if s.len() < 16 {
            return;
        }
        let selected: BTreeSet<_> = if DVB {
            d.dvb_pages()
        } else {
            d.teletext_pages()
        }
        .into_iter()
        .collect();
        let mut candidates: BTreeMap<u16, BTreeSet<Binding>> = BTreeMap::new();
        let end = s.len() - 4;
        let mut at = 12 + ((usize::from(s[10] & 15) << 8) | usize::from(s[11]));
        if at > end {
            return;
        }
        while at < end {
            let Some(h) = s.get(at..at + 5).filter(|_| at + 5 <= end) else {
                return;
            };
            let len = (usize::from(h[3] & 15) << 8) | usize::from(h[4]);
            let next = at + 5 + len;
            if next > end {
                return;
            }
            let pid = (u16::from(h[1] & 31) << 8) | u16::from(h[2]);
            if h[0] == 6 {
                let mut i = at + 5;
                while i < next {
                    if i + 2 > next {
                        return;
                    }
                    let n = usize::from(s[i + 1]);
                    if i + 2 + n > next {
                        return;
                    }
                    if DVB && s[i] == 0x59 {
                        if n % 8 != 0 {
                            return;
                        }
                        for entry in s[i + 2..i + 2 + n].chunks_exact(8) {
                            let page = u16::from_be_bytes([entry[4], entry[5]]);
                            if ((0x10..=0x16).contains(&entry[3])
                                || (0x20..=0x26).contains(&entry[3]))
                                && selected.contains(&page)
                            {
                                candidates.entry(page).or_default().insert(Binding {
                                    pid,
                                    ancillary: u16::from_be_bytes([entry[6], entry[7]]),
                                });
                            }
                        }
                    } else if !DVB && s[i] == 0x56 {
                        if n % 5 != 0 {
                            return;
                        }
                        for entry in s[i + 2..i + 2 + n].chunks_exact(5) {
                            let kind = entry[3] >> 3;
                            let mag = if entry[3] & 7 == 0 {
                                8
                            } else {
                                u16::from(entry[3] & 7)
                            };
                            let hi = entry[4] >> 4;
                            let lo = entry[4] & 15;
                            let page = mag * 100 + u16::from(hi) * 10 + u16::from(lo);
                            if matches!(kind, 2 | 5)
                                && hi < 10
                                && lo < 10
                                && selected.contains(&page)
                            {
                                candidates
                                    .entry(page)
                                    .or_default()
                                    .insert(Binding { pid, ancillary: 0 });
                            }
                        }
                    }
                    i += 2 + n;
                }
            }
            at = next;
        }
        let mut bindings = BTreeMap::new();
        for (page, pids) in candidates {
            if pids.len() == 1 {
                bindings.insert(page, *pids.first().unwrap());
            } else {
                d.error = Some(Self::reason("teletext_page_ambiguous"));
            }
        }
        if !self.seen_pmt || bindings != self.bindings {
            self.set_bindings(bindings, d);
            self.seen_pmt = true;
        }
    }
    fn set_bindings(&mut self, b: BTreeMap<u16, Binding>, d: &mut Decoder) {
        let pids: BTreeSet<_> = b.values().map(|v| v.pid).collect();
        let mut changed = BTreeSet::new();
        for page in self.bindings.keys().chain(b.keys()) {
            if self.bindings.get(page) != b.get(page) {
                changed.extend(self.bindings.get(page).map(|v| v.pid));
                changed.extend(b.get(page).map(|v| v.pid));
            }
        }
        self.streams.retain(|p, _| pids.contains(p));
        for p in &pids {
            self.streams.entry(*p).or_default();
            if changed.contains(p) {
                self.streams.insert(*p, Pes::default());
            }
        }
        self.events
            .retain(|e| pids.contains(&e.pid) && !changed.contains(&e.pid));
        if DVB {
            d.dvb_bindings(
                &b.iter()
                    .map(|(page, b)| (*page, (b.pid, b.ancillary)))
                    .collect(),
                d.latest_pts,
            );
        } else {
            d.teletext_bindings(
                &b.iter().map(|(page, b)| (*page, b.pid)).collect(),
                d.latest_pts,
            );
        }
        self.bindings = b;
    }
    pub fn advance(&mut self, frontier: u64, d: &mut Decoder) {
        self.clock = Some(frontier);
        let mut due = vec![];
        let mut pending = vec![];
        for e in self.events.drain(..) {
            let pts = unwrap(e.raw, frontier);
            if pts <= frontier {
                due.push((pts, e));
            } else {
                pending.push(e);
            }
        }
        self.events = pending;
        due.sort_by_key(|(p, _)| *p);
        for (pts, event) in due {
            if DVB {
                d.push_dvb(event.pid, &event.body, pts);
            } else {
                d.push_teletext(event.pid, &event.body, pts);
            }
        }
    }
}
fn unwrap(raw: u64, reference: u64) -> u64 {
    let delta =
        ((raw.wrapping_sub(reference & MASK).wrapping_add(1 << 32)) & MASK) as i64 - (1 << 32);
    reference.saturating_add_signed(delta)
}
fn crc(b: &[u8]) -> u32 {
    let mut c = 0xffff_ffffu32;
    for v in b {
        c ^= u32::from(*v) << 24;
        for _ in 0..8 {
            c = (c << 1) ^ if c & 0x8000_0000 != 0 { 0x04c1_1db7 } else { 0 };
        }
    }
    c
}
