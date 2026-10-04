//! Bounded source intervals and optional independent Tesseract recognition.
use crate::{
    captions::{Cue, Service},
    dvb::{Frame, Image},
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::OnceLock,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Semaphore,
};
use tokio_util::sync::CancellationToken;
pub const BUDGET: Duration = Duration::from_millis(1500);
#[derive(Debug)]
pub struct Recognition {
    pub text: String,
    pub confidence: Option<f32>,
}
#[derive(Debug)]
pub struct Failure {
    pub reason: &'static str,
    pub confidence: Option<f32>,
}
impl Failure {
    pub fn new(reason: &'static str) -> Self {
        Self {
            reason,
            confidence: None,
        }
    }
}
pub struct Job {
    pub page: u16,
    pub token: u64,
    pub image: Image,
    pub language: String,
    pub deadline: Instant,
}
struct Interval {
    token: u64,
    start: u64,
    end: u64,
    hash: [u8; 32],
    pending: bool,
    accepted: bool,
    deadline: Instant,
    text: String,
}
struct Page {
    language: String,
    records: VecDeque<Interval>,
    latest_token: u64,
    error: Option<&'static str>,
    confidence: Option<f32>,
}
pub struct Store {
    pages: BTreeMap<u16, Page>,
    jobs: VecDeque<Job>,
    next: u64,
    changes: tokio::sync::watch::Sender<()>,
}
impl Store {
    pub fn new(services: &[Service]) -> Self {
        Self {
            pages: services
                .iter()
                .filter(|s| s.channel >= 65536)
                .map(|s| {
                    (
                        (s.channel - 65536) as u16,
                        Page {
                            language: s.ocr_language.clone().unwrap_or_else(|| "eng".into()),
                            records: VecDeque::new(),
                            latest_token: 0,
                            error: None,
                            confidence: None,
                        },
                    )
                })
                .collect(),
            jobs: VecDeque::new(),
            next: 0,
            changes: tokio::sync::watch::Sender::new(()),
        }
    }
    pub fn ingest(&mut self, f: Frame, now: Instant) {
        self.expire(now);
        let pending = self
            .pages
            .values()
            .flat_map(|p| &p.records)
            .filter(|r| r.pending)
            .count();
        let Some(p) = self.pages.get_mut(&f.page) else {
            return;
        };
        if p.records.back().is_some_and(|r| f.pts < r.start) {
            return;
        }
        let Some(image) = f.image else {
            if let Some(r) = p.records.back_mut() {
                r.end = r.end.min(f.pts.max(r.start));
            }
            return;
        };
        let mut digest = Sha256::new();
        digest.update((image.width as u64).to_be_bytes());
        digest.update((image.height as u64).to_be_bytes());
        for pixel in &image.pixels {
            digest.update(pixel)
        }
        let hash = digest.finalize().into();
        if let Some(r) = p.records.back_mut() {
            if (r.pending || r.accepted) && r.hash == hash && r.end >= f.pts {
                r.end = r.end.max(f.expires);
                return;
            }
            r.end = r.end.min(f.pts.max(r.start));
        }
        self.next = self.next.checked_add(1).expect("OCR token exhausted");
        let token = self.next;
        let deadline = now + BUDGET;
        let queued = pending < 8;
        p.latest_token = token;
        p.confidence = None;
        p.error = (!queued).then_some("dvb_ocr_queue_limit");
        p.records.push_back(Interval {
            token,
            start: f.pts,
            end: f.expires.max(f.pts),
            hash,
            pending: queued,
            accepted: false,
            deadline,
            text: String::new(),
        });
        if queued {
            self.jobs.push_back(Job {
                page: f.page,
                token,
                image,
                language: p.language.clone(),
                deadline,
            });
        }
        while p.records.len() > 64
            || p.records
                .front()
                .is_some_and(|r| r.end.saturating_add(120 * 90000) < f.pts)
        {
            p.records.pop_front();
        }
        self.prune_jobs();
        self.changes.send_replace(());
    }
    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.changes.subscribe()
    }
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.pages
            .values()
            .flat_map(|p| &p.records)
            .filter(|r| r.pending)
            .map(|r| r.deadline)
            .min()
    }
    fn prune_jobs(&mut self) {
        let pages = &self.pages;
        self.jobs.retain(|j| {
            pages
                .get(&j.page)
                .is_some_and(|p| p.records.iter().any(|r| r.token == j.token && r.pending))
        });
    }
    pub fn take_job(&mut self) -> Option<Job> {
        self.prune_jobs();
        self.jobs.pop_front()
    }
    pub fn pending(&self, page: u16, token: u64) -> bool {
        self.pages
            .get(&page)
            .is_some_and(|p| p.records.iter().any(|r| r.token == token && r.pending))
    }
    pub fn complete(&mut self, page: u16, token: u64, result: Result<Recognition, Failure>) {
        let Some(p) = self.pages.get_mut(&page) else {
            return;
        };
        let Some(r) = p.records.iter_mut().find(|r| r.token == token && r.pending) else {
            return;
        };
        r.pending = false;
        let (error, confidence) = match result {
            Ok(value) => {
                r.accepted = true;
                r.text = value.text;
                (None, value.confidence)
            }
            Err(e) => (Some(e.reason), e.confidence),
        };
        if token == p.latest_token {
            p.error = error;
            p.confidence = confidence;
        }
        self.changes.send_replace(());
    }
    pub fn reset(&mut self, pages: &[u16], pts: u64, reason: &'static str) {
        let mut changed = false;
        for page in pages {
            if let Some(p) = self.pages.get_mut(page) {
                for r in &mut p.records {
                    if r.pending {
                        changed = true;
                        r.pending = false;
                        r.text.clear();
                        r.accepted = false
                    }
                }
                if let Some(r) = p.records.back_mut() {
                    r.end = r.end.min(pts.max(r.start));
                }
                p.error = (!p.records.is_empty()).then_some(reason);
                p.confidence = None;
            }
        }
        self.prune_jobs();
        if changed {
            self.changes.send_replace(());
        }
    }
    pub fn expire(&mut self, now: Instant) {
        let mut changed = false;
        for p in self.pages.values_mut() {
            for r in &mut p.records {
                if r.pending && now >= r.deadline {
                    changed = true;
                    r.pending = false;
                    if r.token == p.latest_token {
                        p.error = Some("dvb_ocr_timeout");
                        p.confidence = None;
                    }
                }
            }
        }
        self.prune_jobs();
        if changed {
            self.changes.send_replace(());
        }
    }
    pub fn frontier(&self, latest: u64) -> u64 {
        self.pages
            .values()
            .flat_map(|p| &p.records)
            .filter(|r| r.pending)
            .map(|r| r.start)
            .min()
            .unwrap_or(latest)
            .min(latest)
    }
    pub fn snapshot(&self) -> Vec<Cue> {
        self.pages
            .iter()
            .flat_map(|(&page, p)| {
                p.records
                    .iter()
                    .filter(|r| !r.pending && !r.text.is_empty() && r.end > r.start)
                    .map(move |r| Cue {
                        channel: 65536 + u32::from(page),
                        start: r.start,
                        end: Some(r.end),
                        text: r.text.clone(),
                    })
            })
            .collect()
    }
    pub fn error(&self) -> Option<&'static str> {
        self.pages.values().find_map(|p| p.error)
    }
    pub fn stats(&self) -> serde_json::Value {
        serde_json::json!(self.pages.iter().map(|(&page,p)|serde_json::json!({"page":page,"ocr_language":p.language,"pending":p.records.iter().filter(|r|r.pending).count(),"last_error":p.error,"confidence":p.confidence})).collect::<Vec<_>>())
    }
}
pub fn parse_tsv(raw: &[u8]) -> Result<Recognition, Failure> {
    if raw.len() > 65536 {
        return Err(Failure::new("dvb_ocr_output_limit"));
    }
    let data = std::str::from_utf8(raw).map_err(|_| Failure::new("dvb_ocr_output_invalid"))?;
    let mut lines = data.lines();
    if lines.next()
        != Some(
            "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext",
        )
    {
        return Err(Failure::new("dvb_ocr_output_invalid"));
    }
    let (mut text, mut previous, mut sum, mut weight) = (String::new(), None, 0f32, 0usize);
    for line in lines.filter(|l| !l.is_empty()) {
        let cols: Vec<_> = line.splitn(12, '\t').collect();
        if cols.len() != 12 {
            return Err(Failure::new("dvb_ocr_output_invalid"));
        }
        let nums: Result<Vec<u32>, _> = cols[..10].iter().map(|v| v.parse()).collect();
        let nums = nums.map_err(|_| Failure::new("dvb_ocr_output_invalid"))?;
        if !(1..=5).contains(&nums[0]) {
            return Err(Failure::new("dvb_ocr_output_invalid"));
        }
        if nums[0] != 5 {
            continue;
        }
        let score: f32 = cols[10]
            .parse()
            .map_err(|_| Failure::new("dvb_ocr_output_invalid"))?;
        if !score.is_finite()
            || !(0.0..=100.0).contains(&score)
            || cols[11].chars().any(char::is_control)
        {
            return Err(Failure::new("dvb_ocr_output_invalid"));
        }
        let word = cols[11].trim();
        if word.is_empty() {
            continue;
        }
        let key = [nums[1], nums[2], nums[3], nums[4]];
        if !text.is_empty() {
            text.push(if previous == Some(key) { ' ' } else { '\n' })
        }
        text.push_str(word);
        if text.len() > 4096 {
            return Err(Failure::new("dvb_ocr_text_limit"));
        }
        let n = word.chars().count();
        sum += score * n as f32;
        weight += n;
        previous = Some(key);
    }
    let confidence = (weight > 0).then(|| sum / weight as f32);
    if confidence.is_some_and(|c| c < 60.0) {
        return Err(Failure {
            reason: "dvb_ocr_low_confidence",
            confidence,
        });
    }
    Ok(Recognition { text, confidence })
}
fn pgm(image: &Image) -> Result<Vec<u8>, Failure> {
    if image.width > 4096 || image.height > 2304 {
        return Err(Failure::new("dvb_ocr_image_limit"));
    }
    let n = image
        .width
        .checked_mul(image.height)
        .filter(|n| *n <= 1024 * 1024 && *n == image.pixels.len() && *n > 0)
        .ok_or_else(|| Failure::new("dvb_ocr_image_limit"))?;
    // Bright foreground on transparent/black paper is inverted; dark ink stays on white paper.
    let mean = image
        .pixels
        .iter()
        .filter(|p| p[1] > 128)
        .map(|p| u64::from(p[0]))
        .sum::<u64>();
    let count = image.pixels.iter().filter(|p| p[1] > 128).count();
    let invert = count > 0 && mean / count as u64 >= 128;
    let w = image.width + 20;
    let h = image.height + 20;
    let mut bytes = format!("P5\n{w} {h}\n255\n").into_bytes();
    let base = bytes.len();
    bytes.resize(base + w * h, 255);
    for i in 0..n {
        let [l, a] = image.pixels[i];
        let ink = if invert { 255 - l } else { l };
        let value = (u16::from(ink) * u16::from(a) + 255 * (255 - u16::from(a))) / 255;
        bytes[base + (i / image.width + 10) * w + i % image.width + 10] = value as u8;
    }
    Ok(bytes)
}
async fn limited<R: AsyncRead + Unpin>(reader: R) -> Result<Vec<u8>, Failure> {
    let mut out = vec![];
    reader
        .take(65537)
        .read_to_end(&mut out)
        .await
        .map_err(|_| Failure::new("dvb_ocr_process_failed"))?;
    if out.len() > 65536 {
        Err(Failure::new("dvb_ocr_output_limit"))
    } else {
        Ok(out)
    }
}
pub async fn recognize(
    executable: &str,
    image: &Image,
    language: &str,
    deadline: Instant,
) -> Result<Recognition, Failure> {
    recognize_cancellable(
        executable,
        image,
        language,
        deadline,
        CancellationToken::new(),
    )
    .await
}
pub async fn recognize_cancellable(
    executable: &str,
    image: &Image,
    language: &str,
    deadline: Instant,
    cancel: CancellationToken,
) -> Result<Recognition, Failure> {
    static SLOTS: OnceLock<Semaphore> = OnceLock::new();
    let deadline = tokio::time::Instant::from_std(deadline);
    let _permit = tokio::select! {biased;_=cancel.cancelled()=>return Err(Failure::new("dvb_ocr_canceled")),r=tokio::time::timeout_at(deadline,SLOTS.get_or_init(||Semaphore::new(2)).acquire())=>r.map_err(|_|Failure::new("dvb_ocr_timeout"))?.map_err(|_|Failure::new("dvb_ocr_unavailable"))?};
    let input = pgm(image)?;
    let mut child = Command::new(executable)
        .args([
            "stdin", "stdout", "--psm", "6", "-l", language, "--dpi", "150", "tsv",
        ])
        .env("OMP_THREAD_LIMIT", "1")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| Failure::new("dvb_ocr_unavailable"))?;
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let result = tokio::select! {biased;_=cancel.cancelled()=>Err(Failure::new("dvb_ocr_canceled")),r=tokio::time::timeout_at(deadline,async {
        let writer=async{let written=stdin.write_all(&input).await.is_ok();drop(stdin);Ok::<_,Failure>(written)};
        let(written,out,err)=tokio::try_join!(writer,limited(stdout),limited(stderr))?;
        let status=child.wait().await.map_err(|_|Failure::new("dvb_ocr_process_failed"))?;
        if !status.success(){let msg=String::from_utf8_lossy(&err);return Err(Failure::new(if msg.contains("Failed loading language")||msg.contains("Error opening data file"){"dvb_ocr_model_unavailable"}else{"dvb_ocr_process_failed"}))}
        if !written{return Err(Failure::new("dvb_ocr_process_failed"))}
        parse_tsv(&out)
    })=>r.unwrap_or_else(|_|Err(Failure::new("dvb_ocr_timeout")))};
    if result.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    result
}
