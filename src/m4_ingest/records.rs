//! Shared native record processing. Transport framing is validated before this boundary.
use super::*;

pub(super) struct State<'a> {
    pub tracks: &'a mut Vec<Track>,
    pub declared: &'a mut Option<Vec<Track>>,
    pub muxer: &'a mut Option<Muxer>,
    pub metadata: &'a mut Option<oneshot::Sender<Vec<Track>>>,
}
pub(super) async fn events<W: AsyncWrite + Unpin>(
    stdin: &mut W,
    state: State<'_>,
    hub: Option<&Hub>,
    policy: SubtitlePolicy<'_>,
    events: Vec<Event>,
) -> Result<(), String> {
    let State {
        tracks,
        declared,
        muxer,
        metadata,
    } = state;
    let SubtitlePolicy {
        authority,
        preserve,
        conversion,
        ..
    } = policy;
    for event in events {
        check_authority(authority)?;
        match event {
            Event::Info { tracks: new, wire } => {
                configs(
                    stdin,
                    tracks,
                    declared,
                    muxer,
                    new.clone(),
                    metadata,
                    policy,
                )
                .await?;
                if let Some(h) = hub {
                    Diagnostics::phase(policy.diagnostic, Phase::Relay);
                    check_authority(authority)?;
                    let text = new.iter().any(|t| t.codec == "subtitle");
                    let output = if !preserve && text {
                        crate::native_subtitles::info(&wire)?
                    } else {
                        wire
                    };
                    h.relay_info(
                        new.iter()
                            .filter(|t| preserve || t.codec != "subtitle")
                            .cloned()
                            .collect(),
                        output,
                    )?;
                }
            }
            Event::Frame {
                track_id,
                dts,
                pts_offset,
                key,
                body,
                wire,
            } => {
                let f = crate::m4f::Frame {
                    track_id,
                    dts,
                    pts_offset,
                    key,
                    body,
                };
                if let Some(state) = conversion {
                    state.native_frame(tracks, &f);
                }
                write_frame(stdin, muxer, &f, policy.diagnostic).await?;
                if let Some(h) = hub {
                    Diagnostics::phase(policy.diagnostic, Phase::Relay);
                    check_authority(authority)?;
                    if preserve
                        || tracks
                            .iter()
                            .any(|t| t.id == f.track_id && t.codec != "subtitle")
                    {
                        h.relay_frame(f.clone(), wire)?;
                    }
                }
            }
            Event::Gop {
                gop,
                tracks: new,
                frames,
                wire,
            } => {
                configs(
                    stdin,
                    tracks,
                    declared,
                    muxer,
                    new.clone(),
                    metadata,
                    SubtitlePolicy {
                        sparse: true,
                        ..policy
                    },
                )
                .await?;
                for f in &frames {
                    check_authority(authority)?;
                    if let Some(state) = conversion {
                        state.native_frame(&new, f);
                    }
                    write_frame(stdin, muxer, f, policy.diagnostic).await?;
                }
                if let Some(h) = hub {
                    Diagnostics::phase(policy.diagnostic, Phase::Relay);
                    check_authority(authority)?;
                    if !preserve && new.iter().any(|t| t.codec == "subtitle") {
                        let body = crate::native_subtitles::segment(&gop.body)?;
                        let filtered = PackedGop { body, ..gop };
                        let wire = crate::m4s::encode_gop(&filtered)?;
                        h.relay_gop(
                            filtered,
                            new.iter()
                                .filter(|t| t.codec != "subtitle")
                                .cloned()
                                .collect(),
                            wire,
                        )?;
                    } else {
                        h.relay_gop(gop, new.clone(), wire)?;
                    }
                }
            }
            Event::Other { wire } => {
                if let Some(h) = hub {
                    Diagnostics::phase(policy.diagnostic, Phase::Relay);
                    check_authority(authority)?;
                    h.m4s.send(wire)?;
                }
            }
        }
    }
    Ok(())
}

/// Keep transport retirement active while native processing awaits an output
/// write. Dropping the consumer also releases its owned subscription stream.
pub(crate) async fn consume<W: AsyncWrite + Unpin>(
    stream: crate::fxlink::RecordStream,
    output: &mut W,
    hub: Option<&Hub>,
    metadata: oneshot::Sender<Vec<Track>>,
    subtitles: PullOptions<'_>,
    retirement: tokio_util::sync::CancellationToken,
) -> Result<(), String> {
    tokio::select! {biased;
        _=retirement.cancelled()=>Err("FXLink subscription retired".into()),
        result=consume_active(stream, output, hub, metadata, subtitles)=>result,
    }
}
/// Consume only intact, ordered records from an already authenticated transport.
async fn consume_active<W: AsyncWrite + Unpin>(
    mut stream: std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, String>> + Send>>,
    output: &mut W,
    hub: Option<&Hub>,
    metadata: oneshot::Sender<Vec<Track>>,
    subtitles: PullOptions<'_>,
) -> Result<(), String> {
    let mut decoder = Decoder::default();
    let mut bootstrap = super::bootstrap::Bootstrap::default();
    let bootstrap_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let mut tracks = Vec::new();
    let mut declared = None;
    let mut muxer = None;
    let mut metadata = Some(metadata);
    let policy = SubtitlePolicy {
        preserve: subtitles.preserve,
        copy: hub.is_some(),
        sparse: false,
        detected: subtitles.detected,
        conversion: subtitles.conversion,
        authority: None,
        failure: subtitles.failure,
        diagnostic: subtitles.diagnostic,
    };
    loop {
        let read_deadline = if bootstrap.ready() {
            tokio::time::Instant::now() + Duration::from_secs(15)
        } else {
            bootstrap_deadline
        };
        let wire = tokio::time::timeout_at(read_deadline, stream.next())
            .await
            .map_err(|_| "FXLink bootstrap or media deadline exceeded")?
            .ok_or("FXLink source closed")??;
        // Defense in depth: no parser state may span purported complete records.
        if wire.len() < 12
            || wire.len() > 16 * 1024 * 1024 + 4
            || u32::from_be_bytes(wire[..4].try_into().unwrap()) as usize != wire.len() - 4
        {
            return Err("invalid FXLink native record length".into());
        }
        let parsed = decoder.push(&wire)?;
        if parsed.len() != 1 {
            return Err("invalid FXLink native record boundary".into());
        }
        let parsed = bootstrap.events(parsed)?;
        events(
            output,
            State {
                tracks: &mut tracks,
                declared: &mut declared,
                muxer: &mut muxer,
                metadata: &mut metadata,
            },
            hub,
            policy,
            parsed,
        )
        .await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
    };
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    struct BlockedWriter {
        accepted: usize,
        before_block: usize,
        entered: Arc<Notify>,
    }
    impl AsyncWrite for BlockedWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.accepted < self.before_block {
                self.accepted += 1;
                return Poll::Ready(Ok(bytes.len()));
            }
            self.entered.notify_one();
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    struct StreamOwner(Arc<AtomicBool>);
    impl Drop for StreamOwner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    async fn blocked_consumer(before_block: usize) {
        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let retired = CancellationToken::new();
        let track = Track {
            id: 1,
            codec: "m2a".into(),
            config: vec![],
        };
        let frame = crate::m4f::Frame {
            track_id: 1,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body: include_bytes!("../../tests/fixtures/codecs/mp2.bin").to_vec(),
        };
        let records = VecDeque::from([
            Bytes::from(crate::wire::encode_info(std::slice::from_ref(&track)).unwrap()),
            Bytes::from(crate::wire::encode_frame(&track, &frame).unwrap()),
        ]);
        let stream = futures_util::stream::unfold(
            (records, StreamOwner(dropped.clone())),
            |(mut records, owner)| async move {
                match records.pop_front() {
                    Some(bytes) => Some((Ok(bytes), (records, owner))),
                    None => std::future::pending().await,
                }
            },
        );
        let writer_entered = entered.clone();
        let (metadata, _metadata_receiver) = oneshot::channel();
        let retirement = retired.clone();
        let mut task = tokio::spawn(async move {
            let mut writer = BlockedWriter {
                accepted: 0,
                before_block,
                entered: writer_entered,
            };
            consume(
                Box::pin(stream),
                &mut writer,
                None,
                metadata,
                PullOptions::default(),
                retirement,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("valid native record must reach blocked writer");
        retired.cancel();
        let result = tokio::time::timeout(Duration::from_millis(500), &mut task).await;
        let returned = match result {
            Ok(result) => Some(result.unwrap()),
            Err(_) => {
                task.abort();
                let _ = task.await;
                None
            }
        };
        assert!(
            returned.is_some(),
            "remote retirement failed to interrupt blocked native output write"
        );
        assert_eq!(
            returned.unwrap().unwrap_err(),
            "FXLink subscription retired"
        );
        assert!(
            dropped.load(Ordering::Acquire),
            "retired consumer retained its subscription stream"
        );
    }
    #[tokio::test]
    async fn remote_retirement_interrupts_blocked_metadata_write() {
        blocked_consumer(0).await;
    }
    #[tokio::test]
    async fn remote_retirement_interrupts_blocked_frame_write() {
        blocked_consumer(1).await;
    }
}
