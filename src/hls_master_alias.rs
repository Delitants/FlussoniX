use bytes::Bytes;

const MAX_PLAYLIST: usize = 1024 * 1024;
const FALLBACK_BPS: u64 = 2_000_000;
const MAX_BPS: u64 = 100_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Alias {
    Ts,
    Fmp4,
}

impl Alias {
    pub(crate) fn from_file(file: &str) -> Option<Self> {
        match file {
            "index.ts.m3u8" => Some(Self::Ts),
            "index.fmp4.m3u8" => Some(Self::Fmp4),
            _ => None,
        }
    }

    pub(crate) fn underlying_file(self) -> &'static str {
        match self {
            Self::Ts => "index.m3u8",
            Self::Fmp4 => "fmp4/index.m3u8",
        }
    }

    fn child(self, uri: &str) -> Result<String, &'static str> {
        valid_relative_uri(uri)?;
        Ok(match self {
            Self::Ts => uri.to_owned(),
            Self::Fmp4 => format!("fmp4/{uri}"),
        })
    }
}

fn valid_relative_uri(uri: &str) -> Result<(), &'static str> {
    if uri.is_empty()
        || uri.starts_with('/')
        || uri
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || !uri
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/'))
    {
        return Err("invalid generated HLS URI");
    }
    Ok(())
}

fn rebase_attributes(alias: Alias, line: &str) -> Result<String, &'static str> {
    let Some((before, rest)) = line.split_once("URI=\"") else {
        return if line.contains("URI=") {
            Err("invalid generated HLS URI attribute")
        } else {
            Ok(line.to_owned())
        };
    };
    let (uri, after) = rest
        .split_once('"')
        .ok_or("invalid generated HLS URI attribute")?;
    if after.contains("URI=\"") {
        return Err("multiple generated HLS URI attributes");
    }
    Ok(format!("{before}URI=\"{}\"{after}", alias.child(uri)?))
}

fn bandwidth(output_rate: Option<(f64, u64)>) -> u64 {
    let Some((mbps, age_ms)) = output_rate else {
        return FALLBACK_BPS;
    };
    if age_ms > 5_000 || !mbps.is_finite() || mbps <= 0.0 {
        return FALLBACK_BPS;
    }
    (mbps * 1_200_000.0).round().clamp(1.0, MAX_BPS as f64) as u64
}

pub(crate) fn render(
    alias: Alias,
    source: &[u8],
    output_rate: Option<(f64, u64)>,
    query: &str,
) -> Result<Bytes, &'static str> {
    if source.len() > MAX_PLAYLIST {
        return Err("generated HLS playlist exceeds limit");
    }
    let source = std::str::from_utf8(source).map_err(|_| "invalid generated HLS playlist")?;
    let lines = source
        .lines()
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect::<Vec<_>>();
    if lines.first() != Some(&"#EXTM3U") {
        return Err("invalid generated HLS playlist");
    }
    let is_master = lines
        .iter()
        .any(|line| line.starts_with("#EXT-X-STREAM-INF:"));
    let body = if is_master {
        if lines
            .iter()
            .any(|line| line.starts_with("#EXTINF:") || line.starts_with("#EXT-X-TARGETDURATION:"))
        {
            return Err("mixed generated HLS playlist");
        }
        let mut output = String::new();
        let mut awaiting_child = false;
        let mut children = 0;
        for line in lines {
            if line.starts_with("#EXT-X-STREAM-INF:") {
                if awaiting_child {
                    return Err("missing generated HLS master child");
                }
                awaiting_child = true;
            }
            if !line.is_empty() && !line.starts_with('#') {
                if !awaiting_child {
                    return Err("unexpected generated HLS master child");
                }
                output.push_str(&alias.child(line)?);
                awaiting_child = false;
                children += 1;
            } else {
                output.push_str(&rebase_attributes(alias, line)?);
            }
            output.push('\n');
        }
        if awaiting_child || children == 0 {
            return Err("missing generated HLS master child");
        }
        output
    } else {
        let has_segment = lines
            .iter()
            .any(|line| !line.is_empty() && !line.starts_with('#'));
        if !has_segment || !lines.iter().any(|line| line.starts_with("#EXTINF:")) {
            return Err("generated HLS media playlist is empty");
        }
        for line in lines {
            if !line.is_empty() && !line.starts_with('#') {
                valid_relative_uri(line)?;
            } else if line.contains("URI=") {
                rebase_attributes(Alias::Ts, line)?;
            }
        }
        format!(
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH={}\n{}\n",
            bandwidth(output_rate),
            alias.underlying_file()
        )
    };
    let body = crate::server::rewrite_playlist(&body, query);
    if body.len() > MAX_PLAYLIST {
        return Err("generated HLS master exceeds limit");
    }
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::{Alias, render};

    #[test]
    fn exact_alias_names_select_existing_hls_playlists() {
        assert_eq!(Alias::from_file("index.ts.m3u8"), Some(Alias::Ts));
        assert_eq!(Alias::from_file("index.fmp4.m3u8"), Some(Alias::Fmp4));
        assert_eq!(Alias::from_file("index.m3u8"), None);
        assert_eq!(Alias::Ts.underlying_file(), "index.m3u8");
        assert_eq!(Alias::Fmp4.underlying_file(), "fmp4/index.m3u8");
    }

    #[test]
    fn media_playlist_becomes_one_master_with_bounded_bandwidth() {
        let media = b"#EXTM3U\n#EXT-X-VERSION:3\n#EXTINF:2,\ng0.ts\n";
        let ts = render(Alias::Ts, media, None, "token=viewer").unwrap();
        let ts = std::str::from_utf8(&ts).unwrap();
        assert!(ts.starts_with("#EXTM3U\n"));
        assert!(ts.contains("#EXT-X-STREAM-INF:BANDWIDTH=2000000\n"));
        assert!(ts.contains("index.m3u8?token=viewer\n"));
        assert!(!ts.contains("g0.ts"));

        let fmp4 = render(Alias::Fmp4, media, Some((3.0, 0)), "").unwrap();
        let fmp4 = std::str::from_utf8(&fmp4).unwrap();
        assert!(fmp4.contains("#EXT-X-STREAM-INF:BANDWIDTH=3600000\n"));
        assert!(fmp4.contains("fmp4/index.m3u8\n"));
        assert!(!fmp4.contains("g0.ts"));
    }

    #[test]
    fn caption_master_rebases_both_subtitles_and_av_from_fmp4_root() {
        let source = b"#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"captions\",NAME=\"English\",URI=\"cc1.m3u8\"\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"captions\",NAME=\"Deutsch\",URI=\"cc2.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=3000000,SUBTITLES=\"captions\"\nav.m3u8\n";
        let body = render(Alias::Fmp4, source, None, "token=a%2Bb").unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("URI=\"fmp4/cc1.m3u8?token=a%2Bb\""));
        assert!(body.contains("URI=\"fmp4/cc2.m3u8?token=a%2Bb\""));
        assert!(body.contains("fmp4/av.m3u8?token=a%2Bb\n"));
        assert_eq!(body.matches("#EXT-X-STREAM-INF:").count(), 1);

        let ts = render(Alias::Ts, source, None, "token=a%2Bb").unwrap();
        let ts = std::str::from_utf8(&ts).unwrap();
        assert!(ts.contains("URI=\"cc1.m3u8?token=a%2Bb\""));
        assert!(ts.contains("av.m3u8?token=a%2Bb\n"));
    }

    #[test]
    fn rejects_malformed_or_external_generated_references() {
        for source in [
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nhttps://other.invalid/av.m3u8\n",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\n../av.m3u8\n",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\n\n",
            "#EXTM3U\n#EXT-X-MEDIA:TYPE=SUBTITLES,URI=broken\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nav.m3u8\n",
            "#EXTM3U\n#EXT-X-MEDIA:TYPE=SUBTITLES,URI=\"/secret.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nav.m3u8\n",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nav.m3u8\n#EXTINF:2,\ng0.ts\n",
        ] {
            assert!(
                render(Alias::Fmp4, source.as_bytes(), None, "").is_err(),
                "{source}"
            );
        }
        let huge = format!("#EXTM3U\n#EXTINF:2,\n{}.ts\n", "a".repeat(1024 * 1024));
        assert!(render(Alias::Ts, huge.as_bytes(), None, "").is_err());
    }

    #[test]
    fn stale_nonfinite_and_extreme_rates_use_safe_bandwidth() {
        let media = b"#EXTM3U\n#EXTINF:2,\ng0.ts\n";
        for rate in [
            None,
            Some((3.0, 5_001)),
            Some((f64::NAN, 0)),
            Some((-1.0, 0)),
        ] {
            let body = render(Alias::Ts, media, rate, "").unwrap();
            assert!(
                std::str::from_utf8(&body)
                    .unwrap()
                    .contains("BANDWIDTH=2000000")
            );
        }
        let body = render(Alias::Ts, media, Some((1_000_000.0, 0)), "").unwrap();
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("BANDWIDTH=100000000000")
        );
        let low = render(Alias::Ts, media, Some((0.0005, 0)), "").unwrap();
        assert!(std::str::from_utf8(&low).unwrap().contains("BANDWIDTH=600"));
        let sub_bit = render(Alias::Ts, media, Some((f64::MIN_POSITIVE, 0)), "").unwrap();
        assert!(
            std::str::from_utf8(&sub_bit)
                .unwrap()
                .contains("BANDWIDTH=1")
        );
    }
}
