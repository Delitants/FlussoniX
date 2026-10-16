# Direct subprocess cleanup after a daemon crash

Approved scope: reduce the observed 15-second orphan cleanup delay while
preserving normal shutdown and the accepted preview. Execution is inline.

1. Add a Linux daemon regression using a controlled worker that ignores
   SIGTERM. Kill only its daemon; require worker exit within three seconds,
   sibling daemon/worker survival, and successful normal sibling shutdown.
   Observe the orphan failure before changing production code.
2. Introduce one private Tokio command constructor. On Linux, install
   PR_SET_PDEATHSIG(SIGKILL) in pre_exec, then check the parent PID captured
   before fork. Fail closed on setup failure. Apply it to media packaging,
   GPU probes, SRT push and OCR; preserve existing cancellation/kill_on_drop.
3. Run the regression, affected integration checks, formatting and strict
   Clippy. Build an isolated release and package it with the pinned UI.
4. Repeat the reviewed real non-root service crash/recovery and graceful-stop
   profile, checking saved state, decoded media, UI, cgroup/PID cleanup and
   accepted-preview preservation. Review and publish the source/draft PR.

Review focus: pre_exec must allocate/log nothing; parent-death setup races;
creator-thread lifetime; sibling isolation; normal shutdown; direct children
versus arbitrary wrapper descendants. Linux follows the creating thread and
clears the setting for forked children or credential-changing execution.
Systemd cgroup cleanup remains the fallback; other platforms retain their
existing lifecycle behavior. The internal FFmpeg SIGTERM stall is unproven.
