use super::{
    CurrentSetup, DependencyReport, InitOptions, InitOutcome, InitRequest, Layout, NodeRole,
    Secret, SetupError, SystemdManager, apply, preflight, recover,
};
use ring::rand::{SecureRandom, SystemRandom};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::AsRawFd;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const INPUT_LIMIT: usize = 4096;

#[derive(Clone, Debug)]
pub struct ExplicitInitOptions {
    pub role: NodeRole,
    pub node_name: String,
    pub listen: SocketAddr,
}

pub(crate) trait PromptIo {
    fn write(&mut self, text: &str) -> Result<(), io::Error>;
    fn read_line(&mut self, prompt: &str) -> Result<String, io::Error>;
    fn read_hidden(&mut self, prompt: &str) -> Result<String, io::Error>;
}

pub(crate) trait RandomSource {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), SetupError>;
}

struct SystemRandomSource(SystemRandom);

impl Default for SystemRandomSource {
    fn default() -> Self {
        Self(SystemRandom::new())
    }
}

impl RandomSource for SystemRandomSource {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), SetupError> {
        self.0
            .fill(output)
            .map_err(|_| SetupError::invalid("secure peer-key generation failed"))
    }
}

static SETUP_INTERRUPTED: AtomicBool = AtomicBool::new(false);

pub(crate) struct TtyPrompt {
    tty: File,
}

impl TtyPrompt {
    fn open() -> Result<Self, SetupError> {
        let tty = OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")
            .map_err(|error| SetupError::io("open /dev/tty for setup", error))?;
        Ok(Self { tty })
    }

    #[cfg(test)]
    pub(crate) fn from_file_for_test(tty: File) -> Self {
        Self { tty }
    }

    pub(crate) fn read_raw_line(&mut self) -> Result<String, io::Error> {
        let mut bytes = Vec::new();
        loop {
            if SETUP_INTERRUPTED.load(Ordering::SeqCst) {
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            let mut descriptor = libc::pollfd {
                fd: self.tty.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: descriptor points to one initialized pollfd for the duration of this call.
            let ready = unsafe { libc::poll(&mut descriptor, 1, 100) };
            if ready == 0 {
                continue;
            }
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            let mut byte = [0_u8; 1];
            match self.tty.read(&mut byte) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "setup terminal closed",
                    ));
                }
                Ok(_) if byte[0] == b'\n' => break,
                Ok(_) if byte[0] == b'\r' => {}
                Ok(_) if bytes.len() < INPUT_LIMIT => bytes.push(byte[0]),
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "setup input exceeded its limit",
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "setup input was not UTF-8"))
    }
}

extern "C" fn setup_interrupt_handler(_signal: libc::c_int) {
    SETUP_INTERRUPTED.store(true, Ordering::SeqCst);
}

pub(crate) struct InterruptGuard {
    previous: libc::sigaction,
}

impl InterruptGuard {
    pub(crate) fn install() -> Result<Self, SetupError> {
        SETUP_INTERRUPTED.store(false, Ordering::SeqCst);
        // SAFETY: zero is a valid starting representation for sigaction before its fields are set.
        let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        action.sa_sigaction = setup_interrupt_handler as *const () as usize;
        action.sa_flags = 0;
        // SAFETY: action owns a valid signal set and sigemptyset initializes it.
        if unsafe { libc::sigemptyset(&mut action.sa_mask) } != 0 {
            return Err(SetupError::io(
                "prepare setup interrupt handler",
                io::Error::last_os_error(),
            ));
        }
        let mut previous = std::mem::MaybeUninit::<libc::sigaction>::uninit();
        // SAFETY: both action and previous remain live for the sigaction call.
        if unsafe { libc::sigaction(libc::SIGINT, &action, previous.as_mut_ptr()) } != 0 {
            return Err(SetupError::io(
                "install setup interrupt handler",
                io::Error::last_os_error(),
            ));
        }
        // SAFETY: sigaction succeeded and populated previous.
        Ok(Self {
            previous: unsafe { previous.assume_init() },
        })
    }
}

impl Drop for InterruptGuard {
    fn drop(&mut self) {
        // SAFETY: previous was returned by sigaction for SIGINT in this process.
        let _ = unsafe { libc::sigaction(libc::SIGINT, &self.previous, std::ptr::null_mut()) };
        SETUP_INTERRUPTED.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
pub(crate) fn mark_interrupted_for_test() {
    setup_interrupt_handler(libc::SIGINT);
}

impl PromptIo for TtyPrompt {
    fn write(&mut self, text: &str) -> Result<(), io::Error> {
        self.tty.write_all(text.as_bytes())?;
        self.tty.flush()
    }

    fn read_line(&mut self, prompt: &str) -> Result<String, io::Error> {
        self.write(prompt)?;
        self.read_raw_line()
    }

    fn read_hidden(&mut self, prompt: &str) -> Result<String, io::Error> {
        self.write(prompt)?;
        let descriptor = self.tty.as_raw_fd();
        let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
        // SAFETY: descriptor is an open terminal and original points to writable termios storage.
        if unsafe { libc::tcgetattr(descriptor, original.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: tcgetattr succeeded and initialized original.
        let original = unsafe { original.assume_init() };
        let mut hidden = original;
        hidden.c_lflag &= !libc::ECHO;
        // SAFETY: descriptor remains open and hidden contains a valid termios value.
        if unsafe { libc::tcsetattr(descriptor, libc::TCSAFLUSH, &hidden) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let guard = EchoGuard {
            descriptor,
            original,
        };
        let result = self.read_raw_line();
        drop(guard);
        self.write("\n")?;
        result
    }
}

struct EchoGuard {
    descriptor: i32,
    original: libc::termios,
}

impl Drop for EchoGuard {
    fn drop(&mut self) {
        // SAFETY: descriptor belongs to the live TtyPrompt and original came from tcgetattr.
        let _ = unsafe { libc::tcsetattr(self.descriptor, libc::TCSANOW, &self.original) };
    }
}

struct StdoutPrompt;

impl PromptIo for StdoutPrompt {
    fn write(&mut self, text: &str) -> Result<(), io::Error> {
        let mut stdout = io::stdout().lock();
        stdout.write_all(text.as_bytes())?;
        stdout.flush()
    }

    fn read_line(&mut self, _prompt: &str) -> Result<String, io::Error> {
        Err(io::Error::other("non-interactive setup cannot read input"))
    }

    fn read_hidden(&mut self, _prompt: &str) -> Result<String, io::Error> {
        Err(io::Error::other(
            "non-interactive setup cannot read hidden input",
        ))
    }
}

pub async fn run(
    options: InitOptions,
    explicit: ExplicitInitOptions,
) -> Result<InitOutcome, SetupError> {
    let layout = Layout::system();
    let mut report = preflight(&layout, None).await?;
    let mut output: Box<dyn PromptIo> = if options.non_interactive {
        Box::new(StdoutPrompt)
    } else {
        Box::new(TtyPrompt::open()?)
    };
    write_dependency_report(output.as_mut(), &report.dependencies)?;
    if !report.dependencies.required_ready() {
        return Err(SetupError::invalid(
            "required media dependencies are unavailable; repair the reported packages and retry",
        ));
    }

    if super::files::journal_present(&layout)? {
        let mut service = SystemdManager::system();
        recover(&layout, &mut service)?;
        report = preflight(&layout, None).await?;
    }
    if let Some(current) = report.current.as_ref() {
        if let Some(outcome) = report_existing(output.as_mut(), current, options.reconfigure)? {
            return Ok(outcome);
        }
    } else if options.reconfigure {
        return Err(SetupError::invalid(
            "FlussoniX is not initialized; omit --reconfigure for the first setup",
        ));
    }

    let request = if options.non_interactive {
        collect_unattended(&explicit, options.no_start, &mut |name| {
            std::env::var(name).ok()
        })?
    } else {
        let _interrupt = InterruptGuard::install()?;
        collect_interactive(
            output.as_mut(),
            &explicit,
            options.no_start,
            &mut super::listener_is_available,
            &mut SystemRandomSource::default(),
        )?
    };
    if options.non_interactive && !super::listener_is_available(request.listen)? {
        return Err(SetupError::invalid(format!(
            "selected listener {} is already in use",
            request.listen
        )));
    }

    let mut service = SystemdManager::system();
    let pending = apply(&layout, &request, &options, &mut service)?;
    if options.no_start || !request.enable_now {
        return pending.complete();
    }
    match wait_for_admin_health(&request).await {
        Ok(()) => pending.complete(),
        Err(error) => {
            pending.fail_health(&mut service)?;
            Err(error)
        }
    }
}

pub(crate) fn collect_interactive(
    prompt: &mut dyn PromptIo,
    explicit: &ExplicitInitOptions,
    no_start: bool,
    listener_available: &mut dyn FnMut(SocketAddr) -> Result<bool, SetupError>,
    random: &mut dyn RandomSource,
) -> Result<InitRequest, SetupError> {
    let admin_user = loop {
        let value = defaulted(read_line(prompt, "Admin username [admin]: ")?, "admin");
        match validate_identifier("admin username", &value) {
            Ok(()) => break value,
            Err(error) => prompt_write(prompt, &format!("{error}\n"))?,
        }
    };
    let admin_password = loop {
        let first = read_hidden(prompt, "Admin password: ")?;
        validate_secret("admin password", &first)?;
        let second = read_hidden(prompt, "Repeat admin password: ")?;
        if first == second {
            break first;
        }
        prompt_write(prompt, "Passwords did not match; try again.\n")?;
    };
    let role = loop {
        let raw = read_line(
            prompt,
            &format!("Node role (standalone/source/cdn/lb) [{}]: ", explicit.role),
        )?;
        let raw = defaulted(raw, &explicit.role.to_string());
        match NodeRole::from_str(&raw) {
            Ok(role) => break role,
            Err(error) => prompt_write(prompt, &format!("{error}\n"))?,
        }
    };
    let node_name = loop {
        let value = defaulted(
            read_line(prompt, &format!("Node name [{}]: ", explicit.node_name))?,
            &explicit.node_name,
        );
        match validate_identifier("node name", &value) {
            Ok(()) => break value,
            Err(error) => prompt_write(prompt, &format!("{error}\n"))?,
        }
    };
    let peer_key = if read_yes_no(
        prompt,
        "Join an existing native cluster and enter its peer key? [y/N]: ",
        false,
    )? {
        loop {
            let value = read_hidden(prompt, "Existing cluster peer key: ")?;
            match validate_secret("peer key", &value) {
                Ok(()) => break value,
                Err(error) => prompt_write(prompt, &format!("{error}\n"))?,
            }
        }
    } else {
        generate_peer_key(random)?
    };
    prompt_write(
        prompt,
        "Remote administration requires HTTPS or a trusted reverse proxy.\n",
    )?;
    let listen = loop {
        let raw = defaulted(
            read_line(prompt, &format!("HTTP listener [{}]: ", explicit.listen))?,
            &explicit.listen.to_string(),
        );
        let address = match raw.parse::<SocketAddr>() {
            Ok(address) if address.port() != 0 => address,
            _ => {
                prompt_write(prompt, "Enter a valid IP address and non-zero port.\n")?;
                continue;
            }
        };
        if listener_available(address)? {
            break address;
        }
        prompt_write(prompt, "That listener is already in use; choose another.\n")?;
    };
    prompt_write(
        prompt,
        &format!(
            "\nSetup summary\n  Admin user: {admin_user}\n  Role: {role}\n  Node: {node_name}\n  HTTP: {listen}\n  Secrets: stored privately (not shown)\n",
        ),
    )?;
    let enable_now = if no_start {
        false
    } else {
        read_yes_no(
            prompt,
            "Enable and start FlussoniX at boot now? [Y/n]: ",
            true,
        )?
    };
    Ok(InitRequest {
        admin_user,
        admin_password: Secret::new(admin_password),
        peer_key: Secret::new(peer_key),
        role,
        node_name,
        listen,
        enable_now,
    })
}

pub(crate) fn collect_unattended(
    explicit: &ExplicitInitOptions,
    no_start: bool,
    environment: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<InitRequest, SetupError> {
    let admin_user = environment("FLUSSONIX_ADMIN_USER");
    let admin_password = environment("FLUSSONIX_ADMIN_PASSWORD");
    let peer_key = environment("FLUSSONIX_PEER_KEY");
    let admin_user = admin_user.ok_or_else(|| {
        SetupError::invalid("non-interactive setup requires FLUSSONIX_ADMIN_USER")
    })?;
    let admin_password = admin_password.ok_or_else(|| {
        SetupError::invalid("non-interactive setup requires FLUSSONIX_ADMIN_PASSWORD")
    })?;
    let peer_key = peer_key
        .ok_or_else(|| SetupError::invalid("non-interactive setup requires FLUSSONIX_PEER_KEY"))?;
    validate_identifier("admin username", &admin_user)?;
    validate_identifier("node name", &explicit.node_name)?;
    validate_secret("admin password", &admin_password)?;
    validate_secret("peer key", &peer_key)?;
    if explicit.listen.port() == 0 {
        return Err(SetupError::invalid(
            "setup listener must use a non-zero port",
        ));
    }
    Ok(InitRequest {
        admin_user,
        admin_password: Secret::new(admin_password),
        peer_key: Secret::new(peer_key),
        role: explicit.role,
        node_name: explicit.node_name.clone(),
        listen: explicit.listen,
        enable_now: !no_start,
    })
}

pub(crate) fn report_existing(
    output: &mut dyn PromptIo,
    current: &CurrentSetup,
    reconfigure: bool,
) -> Result<Option<InitOutcome>, SetupError> {
    if reconfigure {
        return Ok(None);
    }
    prompt_write(
        output,
        &format!(
            "FlussoniX is already initialized.\n  Admin user: {}\n  Role: {}\n  Node: {}\n  HTTP: {}\n  Active: {}\n  Enabled at boot: {}\nUse --init --reconfigure to change this setup.\n",
            current.admin_user,
            current.role,
            current.node_name,
            current.listen,
            current.service.active,
            current.service.enabled,
        ),
    )?;
    Ok(Some(InitOutcome {
        configured: true,
        active: current.service.active,
        enabled: current.service.enabled,
        admin_url: format!("http://{}/admin/", current.listen),
    }))
}

fn write_dependency_report(
    output: &mut dyn PromptIo,
    dependencies: &DependencyReport,
) -> Result<(), SetupError> {
    for (name, status) in [
        ("FFmpeg", &dependencies.ffmpeg),
        ("FFprobe", &dependencies.ffprobe),
        ("H.264/AAC", &dependencies.h264_aac_profile),
        ("SRT", &dependencies.srt),
        ("SRTP", &dependencies.srtp),
    ] {
        prompt_write(
            output,
            &format!(
                "{name}: {} ({})\n",
                if status.available {
                    "available"
                } else {
                    "unavailable"
                },
                status.detail
            ),
        )?;
    }
    for status in &dependencies.gpu {
        prompt_write(
            output,
            &format!(
                "GPU {}: {} ({})\n",
                status.name,
                if status.available {
                    "available"
                } else {
                    "unavailable"
                },
                status.detail
            ),
        )?;
    }
    Ok(())
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), SetupError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(SetupError::invalid(format!(
            "{field} must use 1-128 letters, digits, dots, underscores or dashes"
        )));
    }
    Ok(())
}

fn validate_secret(field: &'static str, value: &str) -> Result<(), SetupError> {
    if value.is_empty() || value.len() > INPUT_LIMIT || value.chars().any(char::is_control) {
        return Err(SetupError::invalid(format!(
            "{field} must be non-empty, at most {INPUT_LIMIT} bytes and contain no control characters"
        )));
    }
    Ok(())
}

fn read_yes_no(
    prompt: &mut dyn PromptIo,
    message: &str,
    default: bool,
) -> Result<bool, SetupError> {
    loop {
        let value = read_line(prompt, message)?.trim().to_ascii_lowercase();
        match value.as_str() {
            "" => return Ok(default),
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => prompt_write(prompt, "Enter yes or no.\n")?,
        }
    }
}

fn read_line(prompt: &mut dyn PromptIo, message: &str) -> Result<String, SetupError> {
    prompt
        .read_line(message)
        .map_err(|error| prompt_error("read setup input", error))
}

fn read_hidden(prompt: &mut dyn PromptIo, message: &str) -> Result<String, SetupError> {
    prompt
        .read_hidden(message)
        .map_err(|error| prompt_error("read hidden setup input", error))
}

fn prompt_write(prompt: &mut dyn PromptIo, text: &str) -> Result<(), SetupError> {
    prompt
        .write(text)
        .map_err(|error| prompt_error("write setup prompt", error))
}

fn prompt_error(action: &'static str, error: io::Error) -> SetupError {
    if error.kind() == io::ErrorKind::Interrupted {
        SetupError::invalid("setup cancelled before configuration was written")
    } else {
        SetupError::io(action, error)
    }
}

fn defaulted(value: String, default: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        default.to_owned()
    } else {
        value.to_owned()
    }
}

fn generate_peer_key(random: &mut dyn RandomSource) -> Result<String, SetupError> {
    let mut bytes = [0_u8; 32];
    random.fill(&mut bytes)?;
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("write into String");
    }
    Ok(encoded)
}

async fn wait_for_admin_health(request: &InitRequest) -> Result<(), SetupError> {
    let address = match request.listen.ip() {
        IpAddr::V4(address) if address.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), request.listen.port())
        }
        IpAddr::V6(address) if address.is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), request.listen.port())
        }
        _ => request.listen,
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|_| SetupError::service("could not create local setup health client"))?;
    let url = format!("http://{address}/admin/");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let result = client
            .get(&url)
            .basic_auth(&request.admin_user, Some(request.admin_password.expose()))
            .send()
            .await;
        if result.as_ref().is_ok_and(|response| {
            response.status().is_success() || response.status().is_redirection()
        }) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(SetupError::service(
                "FlussoniX did not become healthy; inspect systemctl status flussonix.service and journalctl -u flussonix.service",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
