//! Keep authenticated M4F control reads independent of the paced TS feed.
use super::*;
use crate::m4_ingest::flussonic::InputFailure;
use tokio::io::AsyncWriteExt;

async fn drain_required(worker: &Worker) -> Result<(), SharedFailure> {
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        worker.current()?;
        if worker.drain.written()
            >= worker
                .required_issued
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(());
        }
        tokio::select! { biased;
            _=worker.cancel.cancelled()=>return Err(SharedFailure::Closed),
            _=worker.changed.notified()=>{},
            _=tick.tick()=>{},
        }
    }
}

async fn drain_all(
    worker: &Worker,
    copy: &mut (impl std::future::Future<Output = Result<(), SharedFailure>> + Unpin),
    deadline: tokio::time::Instant,
) -> Result<Result<(), SharedFailure>, tokio::time::error::Elapsed> {
    tokio::time::timeout_at(deadline, async {
        copy.await?;
        drain_required(worker).await
    })
    .await
}

fn writer_stopped(worker: &Worker, result: Result<(), SharedFailure>) -> Result<(), SharedFailure> {
    worker.current()?;
    worker.input_failed(InputFailure::Local, false)?;
    Err(result.err().unwrap_or(SharedFailure::Input))
}

fn terminal_failure(
    worker: &Worker,
    source: InputFailure,
    drained: Result<Result<(), SharedFailure>, tokio::time::error::Elapsed>,
) -> Result<(), SharedFailure> {
    worker.current()?;
    let successful = matches!(&drained, Ok(Ok(())));
    let failure = match drained {
        Ok(Ok(())) => source,
        Ok(Err(_)) | Err(_) if matches!(source, InputFailure::Rejected | InputFailure::Invalid) => {
            source
        }
        Ok(Err(_)) | Err(_) => InputFailure::Local,
    };
    if successful {
        worker.drain.complete();
    }
    worker.input_failed(failure, false)?;
    Err(SharedFailure::Input)
}

impl SharedInput {
    pub(super) async fn run_m4f_buffered(&self) -> Result<(), SharedFailure> {
        let worker = &self.worker;
        worker.current()?;
        let required = worker
            .required
            .lock()
            .unwrap()
            .clone()
            .map(tokio_util::sync::PollSender::new);
        let mut output = QueueWriter {
            channel: &worker.ts,
            required,
            required_issued: &worker.required_issued,
        };
        let authority = || worker.current().is_ok();
        let mut output = crate::m4_ingest::authority::Writer {
            inner: &mut output,
            authority: Some(&authority),
        };
        let (mut buffered, mut receiver) = bridge::bounded();
        let copy = async {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let chunk = tokio::select! { biased;
                    _=worker.cancel.cancelled()=>return Err(SharedFailure::Closed),
                    _=worker.changed.notified()=>{worker.current()?;continue},
                    _=tick.tick()=>{worker.current()?;continue},
                    next=receiver.recv()=>next,
                };
                let Some(chunk) = chunk else { return Ok(()) };
                worker.current()?;
                let write = output.write_all(&chunk);
                tokio::pin!(write);
                loop {
                    tokio::select! { biased;
                        _=worker.cancel.cancelled()=>return Err(SharedFailure::Closed),
                        _=worker.changed.notified()=>worker.current()?,
                        _=tick.tick()=>worker.current()?,
                        result=&mut write=>{result.map_err(|_|SharedFailure::Input)?;break},
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::pin!(copy);
        let (tx, mut metadata) = oneshot::channel();
        let mut sender = Some(tx);
        let mut m4f = crate::m4_ingest::m4f_session::M4fSession::default();
        let mut rapid_eofs = VecDeque::new();
        let mut last_eof_segments = 0;
        let mut metadata_pending = true;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let result = {
                let options = crate::m4_ingest::PullOptions {
                    preserve: worker.options.preserve_subtitles,
                    detected: Some(&worker.detected),
                    conversion: worker.options.conversion.as_deref(),
                    ..Default::default()
                };
                let pull = self.input.pull_m4f_session_guarded(
                    &mut buffered,
                    Some(&worker.hub),
                    &mut m4f,
                    sender.take(),
                    options,
                    &authority,
                );
                tokio::pin!(pull);
                loop {
                    tokio::select! { biased;
                        _=worker.cancel.cancelled()=>return Err(SharedFailure::Closed),
                        _=worker.changed.notified()=>worker.current()?,
                        _=tick.tick()=>worker.current()?,
                        tracks=&mut metadata, if metadata_pending=>{
                            metadata_pending=false;
                            if let Ok(tracks)=tracks {
                                worker.current()?;
                                worker.at_source_route(|_| {
                                    let commit=|| {
                                        if worker.active_viewer().is_none(){return Err(AdmissionFailure::Revoked);}
                                        *worker.tracks.lock().unwrap()=Some(tracks);
                                        worker.metadata_ready.notify_waiters();Ok(())
                                    };
                                    match &worker.scope {Some(scope)=>scope.accept_private(commit),None=>commit()}
                                })?;
                            }
                        },
                        stopped=&mut copy=>return writer_stopped(worker,stopped),
                        result=&mut pull=>break result,
                    }
                }
            };
            worker.current()?;
            match result {
                Err(InputFailure::CleanControlEof) => {
                    let now = std::time::Instant::now();
                    if !allow_clean_eof_reopen(
                        &mut rapid_eofs,
                        now,
                        m4f.accepted_segments() > last_eof_segments,
                    ) {
                        let deadline = worker.drain.start();
                        worker.hub.finish();
                        drop(buffered);
                        let drained = drain_all(worker, &mut copy, deadline).await;
                        return terminal_failure(worker, InputFailure::Transient, drained);
                    }
                    last_eof_segments = m4f.accepted_segments();
                    let renewal = worker.renewal.upgrade().ok_or(SharedFailure::Closed)?;
                    let refresh = renewal.renew_after_eof(&worker.handle, Duration::from_secs(3));
                    tokio::pin!(refresh);
                    loop {
                        tokio::select! { biased;
                            _=worker.cancel.cancelled()=>return Err(SharedFailure::Closed),
                            _=worker.changed.notified()=>worker.current()?,
                            _=tick.tick()=>worker.current()?,
                            stopped=&mut copy=>return writer_stopped(worker,stopped),
                            result=&mut refresh=>{result?;break},
                        }
                    }
                    worker.current()?;
                }
                Err(kind) => {
                    if matches!(kind, InputFailure::Rejected | InputFailure::Invalid) {
                        worker.input_failed(kind, false)?;
                        worker.hub.finish();
                        return Err(SharedFailure::Input);
                    }
                    let deadline = worker.drain.start();
                    worker.hub.finish();
                    drop(buffered);
                    let drained = drain_all(worker, &mut copy, deadline).await;
                    return terminal_failure(worker, kind, drained);
                }
                Ok(()) => {
                    let deadline = worker.drain.start();
                    worker.hub.finish();
                    drop(buffered);
                    return match drain_all(worker, &mut copy, deadline).await {
                        Ok(Ok(())) => {
                            worker.drain.complete();
                            Ok(())
                        }
                        Ok(Err(error)) => writer_stopped(worker, Err(error)),
                        Err(_) => writer_stopped(worker, Err(SharedFailure::Input)),
                    };
                }
            }
        }
    }
}
