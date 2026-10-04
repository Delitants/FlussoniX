//! Independent bounded DVB bitmap display reconstruction, before optional OCR.
use std::collections::{BTreeMap, BTreeSet};
#[path = "dvb_pixels.rs"]
mod pixels;
use pixels::{default_clut, paint};
const PIXELS: usize = 1024 * 1024;
type Color = [u8; 2]; // SDR luminance and opacity; text conversion flattens chroma.
type Result<T> = std::result::Result<T, &'static str>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Image {
    pub width: usize,
    pub height: usize,
    pub pixels: Vec<Color>,
}
#[derive(Clone, Debug)]
pub struct Frame {
    pub page: u16,
    pub pts: u64,
    pub expires: u64,
    pub image: Option<Image>,
}
struct Region {
    width: usize,
    height: usize,
    depth: usize,
    clut: u8,
    pixels: Vec<u8>,
    objects: Vec<(u16, usize, usize)>,
    definition: Vec<u8>,
    redraw: bool,
}
struct Page {
    binding: Option<(u16, u16)>,
    regions: BTreeMap<u8, Region>,
    objects: BTreeMap<(u16, u16), Vec<u8>>,
    cluts: BTreeMap<(u16, u8), Box<[[Color; 256]; 3]>>,
    changed: BTreeSet<(u16, u16)>,
    layout: Vec<(u8, usize, usize)>,
    display: (usize, usize),
    window: (usize, usize, usize, usize),
    pending: Option<u64>,
    timeout: u8,
    visible: Option<u64>,
    acquired: bool,
    error: Option<&'static str>,
}
impl Default for Page {
    fn default() -> Self {
        Self {
            binding: None,
            regions: BTreeMap::new(),
            objects: BTreeMap::new(),
            cluts: BTreeMap::new(),
            changed: BTreeSet::new(),
            layout: vec![],
            display: (720, 576),
            window: (0, 0, 720, 576),
            pending: None,
            timeout: 0,
            visible: None,
            acquired: false,
            error: None,
        }
    }
}
impl Page {
    fn clear(&mut self, page: u16, pts: u64) -> Frame {
        let binding = self.binding;
        *self = Self::default();
        self.binding = binding;
        Frame {
            page,
            pts,
            expires: pts,
            image: None,
        }
    }
    fn segment(
        &mut self,
        page: u16,
        kind: u8,
        owner: u16,
        b: &[u8],
        pts: u64,
    ) -> Result<Option<Frame>> {
        if self.pending.is_some_and(|p| p != pts) {
            return Err("dvb_display_truncated");
        }
        if kind == 0x80 {
            if owner != page {
                return Ok(None);
            }
            if !b.is_empty() {
                return Err("dvb_segment_header");
            }
            if self.pending.is_none() {
                return Ok(None);
            }
            if !self.acquired {
                return Err("dvb_acquisition_missing");
            }
            let image = self.render(page)?;
            self.pending = None;
            self.changed.clear();
            self.error = None;
            let expires = pts.saturating_add(u64::from(self.timeout) * 90000);
            self.visible = if image.is_some() && expires > pts {
                Some(expires)
            } else {
                None
            };
            return Ok(Some(Frame {
                page,
                pts,
                expires,
                image: if expires > pts { image } else { None },
            }));
        }
        if !matches!(kind, 0x10..=0x16) {
            return Ok(None);
        }
        self.pending = Some(pts);
        match kind {
            0x10 if owner == page => {
                if b.len() < 2 || (b.len() - 2) % 6 != 0 || b.len() > 2 + 16 * 6 {
                    return Err("dvb_page_composition");
                }
                let state = (b[1] >> 2) & 3;
                if state == 3 {
                    return Err("dvb_page_state_unsupported");
                }
                if state != 0 {
                    self.regions.clear();
                    self.changed.clear();
                    if state == 2 {
                        self.objects.clear();
                        self.cluts.clear();
                    } else {
                        self.objects.retain(|(p, _), _| *p != page);
                        self.cluts.retain(|(p, _), _| *p != page);
                    }
                    self.acquired = true;
                }
                self.timeout = b[0];
                self.layout.clear();
                for e in b[2..].chunks_exact(6) {
                    if self.layout.iter().any(|(id, _, _)| *id == e[0]) {
                        return Err("dvb_duplicate_region");
                    }
                    self.layout.push((e[0], word(e, 2)?, word(e, 4)?));
                }
            }
            0x11 if owner == page => {
                if b.len() < 10 || (b.len() - 10) % 6 != 0 {
                    return Err("dvb_region_composition");
                }
                if self.regions.get(&b[0]).is_some_and(|r| r.definition == b) {
                    return Ok(None);
                }
                let (w, h) = (word(b, 2)?, word(b, 4)?);
                let depth = usize::from((b[6] >> 2) & 7);
                if !(1..=3).contains(&depth) || !(1..=3).contains(&(b[6] >> 5)) {
                    return Err("dvb_region_depth_unsupported");
                }
                let size = w
                    .checked_mul(h)
                    .filter(|n| *n > 0 && *n <= PIXELS)
                    .ok_or("dvb_pixel_limit")?;
                let other: usize = self
                    .regions
                    .iter()
                    .filter(|(id, _)| **id != b[0])
                    .map(|(_, r)| r.pixels.len())
                    .sum();
                if other + size > PIXELS
                    || (!self.regions.contains_key(&b[0]) && self.regions.len() >= 16)
                {
                    return Err("dvb_region_limit");
                }
                let mut objects = vec![];
                for e in b[10..].chunks_exact(6) {
                    if e[2] & 0xf0 != 0 {
                        return Err("dvb_object_type_unsupported");
                    }
                    let (x, y) = (
                        word(e, 2)?,
                        (usize::from(e[4] & 15) << 8) | usize::from(e[5]),
                    );
                    if x >= w || y >= h || objects.len() >= 64 {
                        return Err("dvb_object_position");
                    }
                    objects.push((word(e, 0)? as u16, x, y));
                }
                let background = match depth {
                    1 => (b[9] >> 2) & 3,
                    2 => b[9] >> 4,
                    _ => b[8],
                };
                let old = self.regions.remove(&b[0]);
                let pixels = if b[1] & 8 == 0 {
                    old.filter(|r| r.width == w && r.height == h && r.depth == depth)
                        .map(|r| r.pixels)
                        .unwrap_or_else(|| vec![0; size])
                } else {
                    vec![background; size]
                };
                self.regions.insert(
                    b[0],
                    Region {
                        width: w,
                        height: h,
                        depth,
                        clut: b[7],
                        pixels,
                        objects,
                        definition: b.to_vec(),
                        redraw: true,
                    },
                );
            }
            0x12 => {
                if b.len() < 2 {
                    return Err("dvb_clut_header");
                }
                let id = (owner, b[0]);
                if !self.cluts.contains_key(&id) && self.cluts.len() >= 16 {
                    return Err("dvb_clut_limit");
                }
                let palette = self.cluts.entry(id).or_insert_with(default_clut);
                let mut at = 2;
                while at < b.len() {
                    let e = b.get(at..at + 2).ok_or("dvb_clut_header")?;
                    let flags = e[1];
                    let index = usize::from(e[0]);
                    let full = flags & 1 != 0;
                    let (y, t) = if full {
                        let v = b.get(at + 2..at + 6).ok_or("dvb_clut_header")?;
                        at += 6;
                        (v[0], v[3])
                    } else {
                        let v = b.get(at + 2..at + 4).ok_or("dvb_clut_header")?;
                        at += 4;
                        (v[0] & 0xfc, (v[1] & 3) << 6)
                    };
                    let value = if y == 0 {
                        [0, 0]
                    } else {
                        [
                            (((i32::from(y) - 16) * 255 + 109) / 219).clamp(0, 255) as u8,
                            255 - t,
                        ]
                    };
                    let selection = flags & 0xe0;
                    if selection.count_ones() != 1 {
                        return Err("dvb_clut_flags");
                    }
                    for (depth, mask, limit) in [(0, 0x80, 4), (1, 0x40, 16), (2, 0x20, 256)] {
                        if flags & mask != 0 {
                            if index >= limit {
                                return Err("dvb_clut_index");
                            }
                            palette[depth][index] = value;
                        }
                    }
                }
            }
            0x13 => {
                if b.len() < 7 || ((b[2] >> 2) & 3) != 0 {
                    return Err("dvb_object_coding_unsupported");
                }
                let (top, bottom) = (word(b, 3)?, word(b, 5)?);
                let end = 7 + top + bottom;
                if end > b.len() || b.len() - end > 1 || b.get(end).is_some_and(|v| *v != 0) {
                    return Err("dvb_object_truncated");
                }
                let key = (owner, word(b, 0)? as u16);
                let retained: usize = self
                    .objects
                    .iter()
                    .filter(|(k, _)| **k != key)
                    .map(|(_, b)| b.len())
                    .sum();
                if retained + b.len() > 2 * 1024 * 1024
                    || (!self.objects.contains_key(&key) && self.objects.len() >= 64)
                {
                    return Err("dvb_object_limit");
                }
                if self.objects.get(&key).is_none_or(|old| old != b) {
                    self.objects.insert(key, b.to_vec());
                    self.changed.insert(key);
                }
            }
            0x14 if owner == page => {
                if b.len() != 5 && b.len() != 13 {
                    return Err("dvb_display_definition");
                }
                let (w, h) = (word(b, 1)? + 1, word(b, 3)? + 1);
                if w > 4096 || h > 2304 {
                    return Err("dvb_display_limit");
                }
                let window = if b[0] & 8 != 0 {
                    if b.len() != 13 {
                        return Err("dvb_display_definition");
                    }
                    let (x, y, xmax, ymax) =
                        (word(b, 5)?, word(b, 9)?, word(b, 7)? + 1, word(b, 11)? + 1);
                    if x >= xmax || y >= ymax || xmax > w || ymax > h {
                        return Err("dvb_display_definition");
                    }
                    (x, y, xmax, ymax)
                } else {
                    if b.len() != 5 {
                        return Err("dvb_display_definition");
                    }
                    (0, 0, w, h)
                };
                self.display = (w, h);
                self.window = window;
            }
            0x15 | 0x16 => return Err("dvb_enhancement_unsupported"),
            _ => {}
        }
        Ok(None)
    }
    fn render(&mut self, page: u16) -> Result<Option<Image>> {
        if self.layout.is_empty() {
            return Ok(None);
        }
        let ancillary = self.binding.map_or(page, |(_, a)| a);
        let mut budget = 0;
        for &(region, _, _) in &self.layout {
            let r = self.regions.get_mut(&region).ok_or("dvb_region_missing")?;
            for index in 0..r.objects.len() {
                let (id, x, y) = r.objects[index];
                let key = if self.objects.contains_key(&(page, id)) {
                    (page, id)
                } else {
                    (ancillary, id)
                };
                let object = self.objects.get(&key).ok_or("dvb_object_missing")?;
                if !r.redraw && !self.changed.contains(&key) {
                    continue;
                }
                let top = word(object, 3)?;
                let bottom = word(object, 5)?;
                let nonmod = object[2] & 2 != 0;
                paint(r, x, y, &object[7..7 + top], nonmod, &mut budget)?;
                let bottom = if bottom == 0 {
                    &object[7..7 + top]
                } else {
                    &object[7 + top..7 + top + bottom]
                };
                if !bottom.is_empty() {
                    paint(r, x, y + 1, bottom, nonmod, &mut budget)?;
                }
            }
            r.redraw = false;
        }
        let (mut left, mut top, mut right, mut bottom) = (usize::MAX, usize::MAX, 0, 0);
        for &(id, x, y) in &self.layout {
            let r = self.regions.get(&id).ok_or("dvb_region_missing")?;
            let (x, y) = (x + self.window.0, y + self.window.1);
            if x + r.width > self.window.2 || y + r.height > self.window.3 {
                return Err("dvb_region_position");
            }
            left = left.min(x);
            top = top.min(y);
            right = right.max(x + r.width);
            bottom = bottom.max(y + r.height);
        }
        let (w, h) = (right - left, bottom - top);
        let n = w
            .checked_mul(h)
            .filter(|n| *n <= PIXELS)
            .ok_or("dvb_pixel_limit")?;
        let mut pixels = vec![[0, 0]; n];
        let defaults = default_clut();
        for &(id, x, y) in &self.layout {
            let r = &self.regions[&id];
            let palette = self
                .cluts
                .get(&(page, r.clut))
                .or_else(|| self.cluts.get(&(ancillary, r.clut)))
                .unwrap_or(&defaults);
            for row in 0..r.height {
                for col in 0..r.width {
                    let dst = (y + self.window.1 - top + row) * w + x + self.window.0 - left + col;
                    pixels[dst] = palette[r.depth - 1][usize::from(r.pixels[row * r.width + col])];
                }
            }
        }
        Ok(pixels.iter().any(|p| p[1] != 0).then_some(Image {
            width: w,
            height: h,
            pixels,
        }))
    }
}
pub struct Decoder {
    pages: BTreeMap<u16, Page>,
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
            error: None,
        }
    }
    pub fn pages(&self) -> Vec<u16> {
        self.pages.keys().copied().collect()
    }
    pub(crate) fn current_error(&self) -> Option<&'static str> {
        self.pages.values().find_map(|p| p.error)
    }
    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!(self.pages.iter().map(|(page,p)|serde_json::json!({"page":page,"announced":p.binding.is_some(),"pid":p.binding.map(|b|b.0),"ancillary_page":p.binding.map(|b|b.1),"last_error":p.error})).collect::<Vec<_>>())
    }
    pub fn bindings(&mut self, b: &BTreeMap<u16, (u16, u16)>, pts: u64) -> Vec<Frame> {
        let mut out = vec![];
        for (&page, p) in &mut self.pages {
            let next = b.get(&page).copied();
            if p.binding != next {
                out.push(p.clear(page, pts));
                p.binding = next;
            }
            if next.is_none() {
                p.error = Some("dvb_page_unannounced");
            }
        }
        out
    }
    pub fn reset_pid(&mut self, pid: u16, pts: u64) -> Vec<Frame> {
        let mut out = vec![];
        for (&page, p) in &mut self.pages {
            if p.binding.is_some_and(|b| b.0 == pid) {
                out.push(p.clear(page, pts));
            }
        }
        out
    }
    pub fn reset(&mut self, pts: u64) -> Vec<Frame> {
        self.pages
            .iter_mut()
            .map(|(&page, p)| p.clear(page, pts))
            .collect()
    }
    pub fn push(&mut self, pid: u16, body: &[u8], pts: u64) -> Vec<Frame> {
        let mut out = vec![];
        let mut bad = BTreeSet::new();
        if body.len() < 3 || body[..2] != [0x20, 0] {
            return self.fail_pid(pid, pts, "dvb_pes_payload");
        }
        let mut at = 2;
        while at < body.len() && body[at] == 0x0f {
            let Some(header) = body.get(at..at + 6) else {
                return self.fail_pid(pid, pts, "dvb_segment_truncated");
            };
            let owner = u16::from_be_bytes([header[2], header[3]]);
            let n = usize::from(u16::from_be_bytes([header[4], header[5]]));
            let Some(b) = body.get(at + 6..at + 6 + n) else {
                return self.fail_pid(pid, pts, "dvb_segment_truncated");
            };
            for (&page, p) in &mut self.pages {
                if bad.contains(&page)
                    || !p
                        .binding
                        .is_some_and(|(p, a)| p == pid && (owner == page || owner == a))
                {
                    continue;
                }
                match p.segment(page, header[1], owner, b, pts) {
                    Ok(Some(f)) => out.push(f),
                    Ok(None) => {}
                    Err(e) => {
                        out.retain(|f| f.page != page);
                        out.push(p.clear(page, pts));
                        p.error = Some(e);
                        self.error = Some(e);
                        bad.insert(page);
                    }
                }
            }
            at += 6 + n;
        }
        if body.get(at) != Some(&255) || body[at..].iter().any(|b| *b != 255) {
            return self.fail_pid(pid, pts, "dvb_pes_payload");
        }
        out
    }
    fn fail_pid(&mut self, pid: u16, pts: u64, reason: &'static str) -> Vec<Frame> {
        let out = self.reset_pid(pid, pts);
        for p in self
            .pages
            .values_mut()
            .filter(|p| p.binding.is_some_and(|b| b.0 == pid))
        {
            p.error = Some(reason);
        }
        self.error = Some(reason);
        out
    }
    pub fn advance(&mut self, pts: u64) -> Vec<Frame> {
        let mut out = vec![];
        for (&page, p) in &mut self.pages {
            if let Some(end) = p.visible.filter(|end| *end <= pts) {
                p.visible = None;
                out.push(Frame {
                    page,
                    pts: end,
                    expires: end,
                    image: None,
                });
            }
        }
        out
    }
}
fn word(b: &[u8], at: usize) -> Result<usize> {
    let bytes = b.get(at..at + 2).ok_or("dvb_segment_truncated")?;
    Ok(usize::from(u16::from_be_bytes([bytes[0], bytes[1]])))
}
