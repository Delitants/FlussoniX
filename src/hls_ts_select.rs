//! Bounded MPEG-TS first-program track selection for live HLS segments.
//! This module has no decoder or per-viewer subprocess.
use std::collections::BTreeSet;

const PACKET: usize = 188;
const MAX_SEGMENT: usize = 32 * 1024 * 1024;
const MAX_SECTION: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Selector {
    pub video: Option<usize>,
    pub audio: Option<usize>,
}

impl Selector {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        let bytes = raw.as_bytes();
        let mut at = 0;
        let mut video = None;
        let mut audio = None;
        for kind in [b'v', b'a'] {
            if bytes.get(at) != Some(&kind) {
                continue;
            }
            at += 1;
            let start = at;
            while bytes.get(at).is_some_and(u8::is_ascii_digit) {
                at += 1;
            }
            if start == at || at - start > 2 || bytes[start] == b'0' {
                return None;
            }
            let ordinal = raw[start..at].parse::<usize>().ok()?;
            if !(1..=16).contains(&ordinal) {
                return None;
            }
            if kind == b'v' {
                video = Some(ordinal);
            } else {
                audio = Some(ordinal);
            }
        }
        (at == bytes.len() && (video.is_some() || audio.is_some())).then_some(Self { video, audio })
    }

    pub(crate) fn name(self) -> String {
        let mut name = String::new();
        if let Some(n) = self.video {
            name.push_str(&format!("v{n}"));
        }
        if let Some(n) = self.audio {
            name.push_str(&format!("a{n}"));
        }
        name
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Route<'a> {
    Master {
        stream: &'a str,
    },
    Playlist {
        stream: &'a str,
        selector: Selector,
    },
    Segment {
        stream: &'a str,
        selector: Selector,
        file: &'a str,
    },
}

pub(crate) fn route(path: &str) -> Option<Route<'_>> {
    if let Some(stream) = path.strip_suffix("/video.m3u8") {
        return (!stream.is_empty()).then_some(Route::Master { stream });
    }
    let (stream, rest) = path.rsplit_once("/tracks-")?;
    let (selector, file) = rest.split_once('/')?;
    let selector = Selector::parse(selector)?;
    if stream.is_empty() || file.contains('/') {
        return None;
    }
    if file == "mono.m3u8" {
        return Some(Route::Playlist { stream, selector });
    }
    let valid_segment = file.len() < 128
        && file.starts_with('g')
        && file.ends_with(".ts")
        && !file.contains("..")
        && file
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'));
    valid_segment.then_some(Route::Segment {
        stream,
        selector,
        file,
    })
}

pub(crate) fn media_playlist(source: &[u8]) -> Result<String, &'static str> {
    if source.is_empty() || source.len() > 1024 * 1024 {
        return Err("HLS media playlist exceeds limit");
    }
    let text = std::str::from_utf8(source).map_err(|_| "invalid HLS media playlist")?;
    if text.lines().next() != Some("#EXTM3U")
        || !text.lines().any(|line| line.starts_with("#EXTINF:"))
        || text
            .lines()
            .any(|line| line.starts_with("#EXT-X-STREAM-INF:"))
    {
        return Err("HLS media playlist unavailable");
    }
    let mut segments = 0;
    for line in text
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        if line.len() >= 128
            || !line.starts_with('g')
            || !line.ends_with(".ts")
            || line.contains("..")
            || !line
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'))
        {
            return Err("unsupported HLS segment reference");
        }
        segments += 1;
    }
    if segments == 0 {
        return Err("HLS media playlist empty");
    }
    Ok(format!("{}\n", text.trim_end_matches(['\r', '\n'])))
}

pub(crate) fn first_segment(playlist: &str) -> Option<&str> {
    playlist
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
}

pub(crate) fn source_media_file(index: &[u8]) -> Result<&'static str, &'static str> {
    if index.is_empty() || index.len() > 1024 * 1024 {
        return Err("HLS index exceeds limit");
    }
    let text = std::str::from_utf8(index).map_err(|_| "invalid HLS index")?;
    if text.lines().next() != Some("#EXTM3U") {
        return Err("invalid HLS index");
    }
    if !text
        .lines()
        .any(|line| line.starts_with("#EXT-X-STREAM-INF:"))
    {
        return Ok("index.m3u8");
    }
    let children: Vec<_> = text
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    if children == ["av.m3u8"] {
        Ok("av.m3u8")
    } else {
        Err("unsupported HLS master layout")
    }
}

/// Replace the one validated child in an already rendered TS master. The
/// original master may carry converted subtitle renditions and query tokens.
pub(crate) fn master_child(
    rendered: &[u8],
    selector: Selector,
    query: &str,
) -> Result<String, &'static str> {
    if rendered.len() > 1024 * 1024 {
        return Err("HLS master exceeds limit");
    }
    let text = std::str::from_utf8(rendered).map_err(|_| "invalid HLS master")?;
    if text.lines().next() != Some("#EXTM3U") {
        return Err("invalid HLS master");
    }
    let mut output = String::new();
    let mut waiting = false;
    let mut children = 0;
    for line in text.lines() {
        if line.starts_with("#EXT-X-STREAM-INF:") {
            if waiting || children > 0 {
                return Err("multiple HLS variants unsupported");
            }
            waiting = true;
            output.push_str(line);
        } else if !line.is_empty() && !line.starts_with('#') {
            if !waiting {
                return Err("unexpected HLS master URI");
            }
            output.push_str(&format!("tracks-{}/mono.m3u8", selector.name()));
            if !query.is_empty() {
                output.push('?');
                output.push_str(query);
            }
            waiting = false;
            children += 1;
        } else {
            output.push_str(line);
        }
        output.push('\n');
    }
    if waiting || children != 1 || output.len() > 1024 * 1024 {
        return Err("HLS master child unavailable");
    }
    Ok(output)
}

fn crc(data: &[u8]) -> u32 {
    let mut c = 0xffff_ffff_u32;
    for &byte in data {
        c ^= u32::from(byte) << 24;
        for _ in 0..8 {
            c = (c << 1) ^ if c & 0x8000_0000 != 0 { 0x04c1_1db7 } else { 0 };
        }
    }
    c
}

fn pid(packet: &[u8]) -> u16 {
    (u16::from(packet[1] & 31) << 8) | u16::from(packet[2])
}

fn payload(packet: &[u8]) -> Result<Option<&[u8]>, &'static str> {
    if packet.len() != PACKET || packet[0] != 0x47 || packet[1] & 0x80 != 0 || packet[3] & 0xc0 != 0
    {
        return Err("invalid or scrambled TS packet");
    }
    if packet[3] & 0x30 == 0 {
        return Err("invalid TS adaptation control");
    }
    let offset = if packet[3] & 0x20 != 0 {
        5 + usize::from(packet[4])
    } else {
        4
    };
    if offset > PACKET {
        return Err("invalid TS adaptation length");
    }
    Ok((packet[3] & 0x10 != 0 && offset < PACKET).then(|| &packet[offset..]))
}

#[derive(Default)]
struct Sections {
    bytes: Vec<u8>,
    last_cc: Option<u8>,
}

impl Sections {
    fn push(&mut self, packet: &[u8]) -> Result<Vec<Vec<u8>>, &'static str> {
        let mut out = Vec::new();
        let Some(body) = payload(packet)? else {
            return Ok(out);
        };
        let cc = packet[3] & 15;
        let start = packet[1] & 0x40 != 0;
        if !start && self.last_cc.is_some_and(|old| (old + 1) & 15 != cc) {
            return Err("PSI continuity gap");
        }
        self.last_cc = Some(cc);
        let body = if start {
            let pointer = usize::from(*body.first().ok_or("empty PSI start")?);
            let rest = body.get(1 + pointer..).ok_or("invalid PSI pointer")?;
            if !self.bytes.is_empty() {
                return Err("incomplete PSI section at restart");
            }
            self.bytes.clear();
            rest
        } else {
            body
        };
        if !start && self.bytes.is_empty() {
            return Err("PSI continuation without start");
        }
        self.bytes.extend_from_slice(body);
        loop {
            if self.bytes.first() == Some(&0xff) {
                self.bytes.clear();
                break;
            }
            if self.bytes.len() < 3 {
                break;
            }
            if self.bytes[1] & 0xf0 != 0xb0 {
                return Err("unsupported PSI section");
            }
            let size = 3 + (usize::from(self.bytes[1] & 15) << 8) + usize::from(self.bytes[2]);
            if !(12..=MAX_SECTION).contains(&size) {
                return Err("PSI section exceeds limit");
            }
            if self.bytes.len() < size {
                break;
            }
            let section: Vec<_> = self.bytes.drain(..size).collect();
            if crc(&section) != 0 || section[5] & 1 == 0 || section[6] != 0 || section[7] != 0 {
                return Err("invalid or noncurrent PSI section");
            }
            out.push(section);
        }
        Ok(out)
    }
}

#[derive(Clone)]
struct Entry {
    pid: u16,
    bytes: Vec<u8>,
}

struct Program {
    pmt_pid: u16,
    pcr_pid: u16,
    pmt: Vec<u8>,
    video: Vec<Entry>,
    audio: Vec<Entry>,
}

fn first_program(data: &[u8]) -> Result<Program, &'static str> {
    let mut pat = Sections::default();
    let mut pmt = Sections::default();
    let mut chosen = None;
    let mut pmt_section: Option<Vec<u8>> = None;
    for packet in data.chunks_exact(PACKET) {
        let body = payload(packet)?;
        if body.is_none() {
            continue;
        }
        if pid(packet) == 0 {
            for section in pat.push(packet)? {
                if section[0] != 0 || section.len() < 16 || (section.len() - 12) % 4 != 0 {
                    return Err("invalid PAT");
                }
                let programs: Vec<_> = section[8..section.len() - 4]
                    .chunks_exact(4)
                    .filter(|entry| entry[0] != 0 || entry[1] != 0)
                    .collect();
                if programs.len() != 1 {
                    return Err("TS selector needs one program");
                }
                let entry = programs[0];
                let found = (
                    u16::from_be_bytes([entry[0], entry[1]]),
                    (u16::from(entry[2] & 31) << 8) | u16::from(entry[3]),
                );
                if chosen.is_some_and(|prior| prior != found) {
                    return Err("PAT changed within segment");
                }
                chosen = Some(found);
            }
        } else if chosen.is_some_and(|(_, pmt_pid)| pmt_pid == pid(packet)) {
            for section in pmt.push(packet)? {
                if section[0] != 2
                    || section.len() < 16
                    || chosen.map(|v| v.0) != Some(u16::from_be_bytes([section[3], section[4]]))
                {
                    return Err("invalid PMT");
                }
                if pmt_section.as_ref().is_some_and(|prior| prior != &section) {
                    return Err("PMT changed within segment");
                }
                pmt_section = Some(section);
            }
        }
    }
    let (_, pmt_pid) = chosen.ok_or("PAT absent")?;
    let pmt = pmt_section.ok_or("PMT absent")?;
    let pcr_pid = (u16::from(pmt[8] & 31) << 8) | u16::from(pmt[9]);
    if pcr_pid == 0x1fff || pcr_pid == pmt_pid {
        return Err("invalid PCR PID");
    }
    let mut at = 12 + (usize::from(pmt[10] & 15) << 8) + usize::from(pmt[11]);
    let end = pmt.len() - 4;
    if at > end {
        return Err("invalid PMT descriptors");
    }
    let mut video = Vec::new();
    let mut audio = Vec::new();
    while at < end {
        let header = pmt.get(at..at + 5).ok_or("invalid PMT entry")?;
        let size = 5 + (usize::from(header[3] & 15) << 8) + usize::from(header[4]);
        let entry = pmt.get(at..at + size).ok_or("invalid PMT descriptors")?;
        let item = Entry {
            pid: (u16::from(header[1] & 31) << 8) | u16::from(header[2]),
            bytes: entry.to_vec(),
        };
        match header[0] {
            2 | 0x10 | 0x1b | 0x24 => video.push(item),
            3 | 4 | 0x0f | 0x11 => audio.push(item),
            _ => {}
        }
        at += size;
    }
    Ok(Program {
        pmt_pid,
        pcr_pid,
        pmt,
        video,
        audio,
    })
}

pub(crate) fn available(data: &[u8]) -> Result<(usize, usize), &'static str> {
    if data.is_empty() || data.len() > MAX_SEGMENT || data.len() % PACKET != 0 {
        return Err("invalid TS segment size");
    }
    let program = first_program(data)?;
    Ok((program.video.len(), program.audio.len()))
}

fn packetized_pmt(section: &[u8], cc: &mut u8, out: &mut Vec<u8>) {
    let mut at = 0;
    while at < section.len() {
        let first = at == 0;
        let mut packet = [0xff; PACKET];
        packet[0] = 0x47;
        packet[1] = if first { 0x40 } else { 0 };
        packet[3] = 0x10 | *cc;
        *cc = (*cc + 1) & 15;
        let start = if first { 5 } else { 4 };
        if first {
            packet[4] = 0;
        }
        let size = (PACKET - start).min(section.len() - at);
        packet[start..start + size].copy_from_slice(&section[at..at + size]);
        at += size;
        out.extend_from_slice(&packet);
    }
}

pub(crate) fn filter(data: &[u8], selector: Selector) -> Result<Vec<u8>, &'static str> {
    if data.is_empty() || data.len() > MAX_SEGMENT || data.len() % PACKET != 0 {
        return Err("invalid TS segment size");
    }
    let program = first_program(data)?;
    let mut selected = Vec::new();
    if let Some(n) = selector.video {
        let index = n
            .checked_sub(1)
            .filter(|_| n <= 16)
            .ok_or("invalid video track")?;
        selected.push(program.video.get(index).ok_or("video track absent")?);
    }
    if let Some(n) = selector.audio {
        let index = n
            .checked_sub(1)
            .filter(|_| n <= 16)
            .ok_or("invalid audio track")?;
        selected.push(program.audio.get(index).ok_or("audio track absent")?);
    }
    if selected.is_empty() {
        return Err("empty TS track selection");
    }
    let mut section = program.pmt[..program.pmt.len() - 4].to_vec();
    let entries_at = 12 + (usize::from(section[10] & 15) << 8) + usize::from(section[11]);
    section.truncate(entries_at);
    let pids: BTreeSet<_> = selected.iter().map(|e| e.pid).collect();
    if pids.len() != selected.len() {
        return Err("duplicate selected PID");
    }
    for entry in selected {
        section.extend_from_slice(&entry.bytes);
    }
    let section_length = section.len() + 4 - 3;
    if section_length > 1021 {
        return Err("selected PMT exceeds limit");
    }
    section[1] = (section[1] & 0xf0) | ((section_length >> 8) as u8 & 15);
    section[2] = section_length as u8;
    section.extend_from_slice(&crc(&section).to_be_bytes());
    let mut out = Vec::with_capacity(data.len());
    let mut cc = 0;
    let mut seen_pmt = false;
    let mut seen_media = false;
    let mut seen_clock = false;
    for packet in data.chunks_exact(PACKET) {
        payload(packet)?;
        let id = pid(packet);
        if id == 0 {
            out.extend_from_slice(packet);
        } else if id == program.pmt_pid {
            if packet[1] & 0x40 != 0 {
                let begin = out.len();
                packetized_pmt(&section, &mut cc, &mut out);
                for generated in out[begin..].chunks_exact_mut(PACKET) {
                    generated[1] |= (id >> 8) as u8 & 31;
                    generated[2] = id as u8;
                }
                seen_pmt = true;
            }
        } else if pids.contains(&id) {
            out.extend_from_slice(packet);
            seen_media = true;
            if id == program.pcr_pid
                && packet[3] & 0x20 != 0
                && packet[4] >= 7
                && packet[5] & 0x10 != 0
            {
                seen_clock = true;
            }
        } else if id == program.pcr_pid
            && packet[3] & 0x20 != 0
            && packet[4] >= 7
            && packet[5] & 0x10 != 0
        {
            let mut clock = [0xff; PACKET];
            clock[0] = 0x47;
            clock[1] = packet[1] & 31;
            clock[2] = packet[2];
            clock[3] = 0x20;
            clock[4] = 183;
            clock[5] = 0x10;
            clock[6..12].copy_from_slice(&packet[6..12]);
            out.extend_from_slice(&clock);
            seen_clock = true;
        }
        if out.len() > MAX_SEGMENT + MAX_SECTION * 64 {
            return Err("selected TS exceeds limit");
        }
    }
    if !seen_pmt || !seen_media || !seen_clock {
        return Err("selected TS lacks PSI, media or clock");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectors_are_exact_and_bounded() {
        for raw in ["v1", "a1", "v1a1", "v16a16"] {
            assert!(Selector::parse(raw).is_some());
        }
        for raw in [
            "", "v0", "v17", "v01", "v1a0", "a1v1", "v1v2", "v1a1x", "V1", "v1/a1",
        ] {
            assert!(Selector::parse(raw).is_none(), "{raw}");
        }
        assert_eq!(Selector::parse("v1a2").unwrap().name(), "v1a2");
    }

    #[test]
    fn exact_routes_preserve_the_base_stream_and_reject_traversal() {
        assert_eq!(
            route("live/group/video.m3u8"),
            Some(Route::Master {
                stream: "live/group"
            })
        );
        assert_eq!(
            route("live/group/tracks-v1a2/mono.m3u8"),
            Some(Route::Playlist {
                stream: "live/group",
                selector: Selector::parse("v1a2").unwrap(),
            })
        );
        assert_eq!(
            route("live/group/tracks-a1/g123_45.ts"),
            Some(Route::Segment {
                stream: "live/group",
                selector: Selector::parse("a1").unwrap(),
                file: "g123_45.ts",
            })
        );
        for invalid in [
            "x/tracks-v0/mono.m3u8",
            "x/tracks-v1/../g1.ts",
            "x/tracks-v1/http://host/x.ts",
            "x/tracks-v1/g1.ts/extra",
            "x/tracks-v1/g..ts",
            "x/tracks-v1/x.ts",
        ] {
            assert!(route(invalid).is_none(), "{invalid}");
        }
    }

    #[test]
    fn playlist_references_only_local_generated_ts_segments() {
        let source = b"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\ng12_100.ts\n";
        assert_eq!(
            media_playlist(source).unwrap(),
            std::str::from_utf8(source).unwrap()
        );
        for bad in [
            "#EXTM3U\n#EXTINF:2,\nhttps://other.invalid/segment.ts\n",
            "#EXTM3U\n#EXTINF:2,\n../g1.ts\n",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\ng1.ts\n",
        ] {
            assert!(media_playlist(bad.as_bytes()).is_err(), "{bad}");
        }
        assert_eq!(
            first_segment(std::str::from_utf8(source).unwrap()),
            Some("g12_100.ts")
        );
    }

    #[test]
    fn master_child_replaces_only_the_one_variant_and_keeps_caption_renditions() {
        let source = b"#EXTM3U\n#EXT-X-MEDIA:TYPE=SUBTITLES,URI=\"cc1.m3u8?token=viewer\"\n#EXT-X-STREAM-INF:BANDWIDTH=2000000,SUBTITLES=\"captions\"\nav.m3u8?token=viewer\n";
        let out = master_child(source, Selector::parse("v1a1").unwrap(), "token=viewer").unwrap();
        assert!(out.contains("URI=\"cc1.m3u8?token=viewer\""));
        assert!(out.contains("tracks-v1a1/mono.m3u8?token=viewer\n"));
        assert!(!out.contains("\nav.m3u8"));
        assert!(master_child(b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\na.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2\nb.m3u8\n", Selector::parse("v1").unwrap(), "").is_err());
        assert_eq!(
            source_media_file(b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nav.m3u8\n").unwrap(),
            "av.m3u8"
        );
        assert_eq!(
            source_media_file(b"#EXTM3U\n#EXTINF:2,\ng1.ts\n").unwrap(),
            "index.m3u8"
        );
        assert!(
            source_media_file(
                b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhttps://foreign.invalid/x.m3u8\n"
            )
            .is_err()
        );
    }

    fn section(mut b: Vec<u8>) -> Vec<u8> {
        let n = b.len() + 4 - 3;
        b[1] = 0xb0 | ((n >> 8) as u8 & 15);
        b[2] = n as u8;
        b.extend_from_slice(&crc(&b).to_be_bytes());
        b
    }
    fn psi(id: u16, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut cc = 0;
        packetized_pmt(body, &mut cc, &mut out);
        for p in out.chunks_exact_mut(PACKET) {
            p[1] |= (id >> 8) as u8 & 31;
            p[2] = id as u8;
        }
        out
    }
    fn media(id: u16, pcr: bool) -> [u8; PACKET] {
        let mut p = [0xff; PACKET];
        p[0] = 0x47;
        p[1] = ((id >> 8) as u8 & 31) | 0x40;
        p[2] = id as u8;
        if pcr {
            p[3] = 0x30;
            p[4] = 7;
            p[5] = 0x10;
            p[6..12].copy_from_slice(&[0, 0, 0, 0x7e, 0, 0]);
        } else {
            p[3] = 0x10;
        }
        p
    }
    fn fixture() -> Vec<u8> {
        let pat = section(vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0, 1, 0xe1, 0]);
        let pmt = section(vec![
            2, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0xe1, 1, 0xf0, 0, 0x1b, 0xe1, 1, 0xf0, 0, 0x0f, 0xe1, 2,
            0xf0, 0, 3, 0xe1, 3, 0xf0, 0,
        ]);
        let mut out = psi(0, &pat);
        out.extend(psi(0x100, &pmt));
        out.extend(media(0x101, true));
        out.extend(media(0x102, false));
        out.extend(media(0x103, false));
        out
    }
    #[test]
    fn filters_selected_pids_and_preserves_audio_only_clock() {
        let input = fixture();
        assert_eq!(available(&input).unwrap(), (1, 2));
        let selected = filter(&input, Selector::parse("v1a2").unwrap()).unwrap();
        let ids: Vec<_> = selected.chunks_exact(PACKET).map(pid).collect();
        assert!(ids.contains(&0x101) && ids.contains(&0x103));
        assert!(!ids.contains(&0x102));
        let audio = filter(&input, Selector::parse("a1").unwrap()).unwrap();
        let ids: Vec<_> = audio.chunks_exact(PACKET).map(pid).collect();
        assert!(ids.contains(&0x102) && ids.contains(&0x101));
        assert_eq!(
            audio
                .chunks_exact(PACKET)
                .find(|p| pid(p) == 0x101)
                .unwrap()[3]
                & 0x30,
            0x20
        );
        assert!(filter(&input, Selector::parse("a3").unwrap()).is_err());
    }
    #[test]
    fn rejects_corrupt_psi_and_unaligned_segments() {
        let mut input = fixture();
        assert!(filter(&input[..input.len() - 1], Selector::parse("v1").unwrap()).is_err());
        input[20] ^= 1;
        assert!(filter(&input, Selector::parse("v1").unwrap()).is_err());
        let input = fixture();
        assert!(
            filter(
                &input,
                Selector {
                    video: Some(0),
                    audio: None
                }
            )
            .is_err()
        );
        assert!(
            filter(
                &input,
                Selector {
                    video: Some(17),
                    audio: None
                }
            )
            .is_err()
        );
    }
    #[test]
    fn multipacket_pmt_is_rewritten_to_a_single_selected_program() {
        let pat = section(vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0, 1, 0xe1, 0]);
        let mut pmt = vec![2, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0xe1, 1, 0xf0, 170];
        pmt.extend(std::iter::repeat_n(0xff, 170));
        pmt.extend([0x24, 0xe1, 1, 0xf0, 0, 0x0f, 0xe1, 2, 0xf0, 0]);
        let mut input = psi(0, &pat);
        input.extend(psi(0x100, &section(pmt)));
        input.extend(media(0x101, true));
        input.extend(media(0x102, false));
        let output = filter(&input, Selector::parse("v1a1").unwrap()).unwrap();
        assert_eq!(available(&output).unwrap(), (1, 1));
        assert!(output.len() % PACKET == 0);
        assert!(output.len() <= input.len() + MAX_SECTION);
    }

    #[test]
    fn optional_owned_live_segment_probe() {
        let Ok(path) = std::env::var("FLUSSONIX_OWNED_TS_SAMPLE") else {
            return;
        };
        let data = std::fs::read(path).unwrap();
        let (video, audio) = available(&data).unwrap();
        assert!(video > 0 && audio > 0);
        for selector in ["v1a1", "v1", "a1"] {
            let bytes = filter(&data, Selector::parse(selector).unwrap()).unwrap();
            let (actual_video, actual_audio) = available(&bytes).unwrap();
            assert_eq!(actual_video, usize::from(selector.contains('v')));
            assert_eq!(actual_audio, usize::from(selector.contains('a')));
        }
    }
}
