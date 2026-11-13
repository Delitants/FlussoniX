//! Direct subprocess ownership, including fatal Linux daemon termination.
use std::ffi::OsStr;
use tokio::process::Command;

pub(crate) fn command(program: impl AsRef<OsStr>) -> Command {
    let command = Command::new(program);
    #[cfg(target_os = "linux")]
    {
        let mut command = command;
        let parent = std::process::id() as libc::pid_t;
        // Linux associates this signal with the creating thread. These commands
        // are spawned on the daemon's persistent async runtime threads, not on
        // retiring blocking-pool threads. Forked wrapper descendants and
        // credential-changing execs can lose this protection; a service cgroup
        // remains the containment backstop. Existing graceful cancellation and
        // kill_on_drop still handle ordinary shutdown.
        // SAFETY: After fork, use only async-signal-safe syscalls and OS error
        // values: no allocation, logging, locks or Rust runtime operations.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(
                    libc::PR_SET_PDEATHSIG,
                    libc::SIGKILL as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                ) == -1
                {
                    return Err(std::io::Error::last_os_error());
                }
                // The daemon could have died before the signal was installed.
                // Reject execution if the child has already been reparented.
                if libc::getppid() != parent {
                    return Err(std::io::Error::from_raw_os_error(libc::ECHILD));
                }
                Ok(())
            });
        }
        command
    }
    #[cfg(not(target_os = "linux"))]
    command
}
