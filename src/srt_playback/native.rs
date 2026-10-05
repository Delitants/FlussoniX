//! Narrow Linux libsrt 1.5 C ABI. Declarations and option numbers are pinned to
//! https://github.com/Haivision/srt/blob/v1.5.4/srtcore/srt.h .
//! A process-lifetime singleton keeps the library loaded and starts it once;
//! each Socket uniquely owns its SRT descriptor. There are no Rust callbacks
//! into listener admission and no pointers retained by socket operations.
use super::Settings;
use libc::{c_char, c_int, c_void, sockaddr, sockaddr_storage};
use libloading::Library;
use std::{
    io, mem,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
    path::Path,
    sync::{Arc, OnceLock},
};

type AddressFn = unsafe extern "C" fn(i32, *mut sockaddr, *mut c_int) -> c_int;
type LogFn =
    unsafe extern "C" fn(*mut c_void, c_int, *const c_char, c_int, *const c_char, *const c_char);
struct Api {
    _library: Library,
    create: unsafe extern "C" fn() -> i32,
    close: unsafe extern "C" fn(i32) -> c_int,
    bind: unsafe extern "C" fn(i32, *const sockaddr, c_int) -> c_int,
    listen: unsafe extern "C" fn(i32, c_int) -> c_int,
    accept: AddressFn,
    address: AddressFn,
    set: unsafe extern "C" fn(i32, c_int, *const c_void, c_int) -> c_int,
    get: unsafe extern "C" fn(i32, c_int, *mut c_void, *mut c_int) -> c_int,
    send: unsafe extern "C" fn(i32, *const c_char, c_int, c_int, c_int) -> c_int,
    state: unsafe extern "C" fn(i32) -> c_int,
    error: unsafe extern "C" fn(*mut c_int) -> c_int,
}
fn failure(message: &'static str) -> io::Error {
    io::Error::other(message)
}
unsafe extern "C" fn discard_log(
    _: *mut c_void,
    _: c_int,
    _: *const c_char,
    _: c_int,
    _: *const c_char,
    _: *const c_char,
) {
}
unsafe fn symbol<T: Copy>(library: &Library, name: &[u8]) -> Result<T, &'static str> {
    // SAFETY: Only the fixed, audited C signatures below call this helper.
    unsafe { library.get::<T>(name) }
        .map(|s| *s)
        .map_err(|_| "incompatible system libsrt")
}
fn system_library() -> Result<Library, &'static str> {
    let triplet = match std::env::consts::ARCH {
        "x86_64" => "x86_64-linux-gnu",
        "aarch64" => "aarch64-linux-gnu",
        "arm" => "arm-linux-gnueabihf",
        _ => "",
    };
    for directory in [
        format!("/lib/{triplet}"),
        format!("/usr/lib/{triplet}"),
        "/usr/lib64".into(),
        "/lib64".into(),
        "/usr/lib".into(),
        "/lib".into(),
    ] {
        for name in [
            "libsrt-gnutls.so.1.5",
            "libsrt.so.1.5",
            "libsrt-gnutls.so",
            "libsrt.so",
        ] {
            let Ok(path) = Path::new(&directory).join(name).canonicalize() else {
                continue;
            };
            if !path.starts_with("/usr/lib") && !path.starts_with("/lib") {
                continue;
            }
            // SAFETY: Load only an absolute canonical system library path. Do
            // not resolve a bare soname via a vendor/working-directory path.
            if let Ok(library) = unsafe { Library::new(path) } {
                return Ok(library);
            }
        }
    }
    Err("system libsrt 1.5 is unavailable")
}
impl Api {
    fn load() -> Result<Self, &'static str> {
        if !cfg!(target_os = "linux") {
            return Err("SRT playback requires Linux");
        }
        let library = system_library()?;
        // SAFETY: These exact signatures are the public libsrt 1.5 C API;
        // the Library is moved into Api and retained for the process lifetime.
        unsafe {
            let version: unsafe extern "C" fn() -> u32 = symbol(&library, b"srt_getversion\0")?;
            if !(0x010500..0x010600).contains(&version()) {
                return Err("system libsrt 1.5 is required");
            }
            let log: unsafe extern "C" fn(*mut c_void, Option<LogFn>) =
                symbol(&library, b"srt_setloghandler\0")?;
            log(std::ptr::null_mut(), Some(discard_log));
            let startup: unsafe extern "C" fn() -> c_int = symbol(&library, b"srt_startup\0")?;
            let api = Self {
                create: symbol(&library, b"srt_create_socket\0")?,
                close: symbol(&library, b"srt_close\0")?,
                bind: symbol(&library, b"srt_bind\0")?,
                listen: symbol(&library, b"srt_listen\0")?,
                accept: symbol(&library, b"srt_accept\0")?,
                address: symbol(&library, b"srt_getsockname\0")?,
                set: symbol(&library, b"srt_setsockflag\0")?,
                get: symbol(&library, b"srt_getsockflag\0")?,
                send: symbol(&library, b"srt_sendmsg\0")?,
                state: symbol(&library, b"srt_getsockstate\0")?,
                error: symbol(&library, b"srt_getlasterror\0")?,
                _library: library,
            };
            if startup() != 0 {
                return Err("system libsrt initialization failed");
            }
            Ok(api)
        }
    }
    fn shared() -> io::Result<Arc<Self>> {
        static API: OnceLock<Result<Arc<Api>, &'static str>> = OnceLock::new();
        API.get_or_init(|| Self::load().map(Arc::new))
            .as_ref()
            .map(Arc::clone)
            .map_err(|message| failure(message))
    }
}
pub struct Socket {
    api: Arc<Api>,
    id: i32,
}
impl Drop for Socket {
    fn drop(&mut self) {
        // SAFETY: This uniquely owned descriptor is not concurrently closed;
        // socket operations retain Api and never outlive this object.
        unsafe {
            (self.api.close)(self.id);
        }
    }
}
impl Socket {
    fn create() -> io::Result<Self> {
        let api = Api::shared()?;
        // SAFETY: Initialized, process-lifetime libsrt function pointer.
        let id = unsafe { (api.create)() };
        if id < 0 {
            return Err(failure("SRT socket creation failed"));
        }
        Ok(Self { api, id })
    }
    fn option<T>(&self, flag: c_int, value: &T) -> io::Result<()> {
        // SAFETY: Call sites below provide the documented C scalar/POD type
        // and its exact size; libsrt copies it during this synchronous call.
        let result = unsafe {
            (self.api.set)(
                self.id,
                flag,
                std::ptr::from_ref(value).cast(),
                mem::size_of::<T>() as c_int,
            )
        };
        if result != 0 {
            Err(failure("SRT option setup failed"))
        } else {
            Ok(())
        }
    }
    fn nonblocking(&self) -> io::Result<()> {
        self.option(1, &0u8)?; // SRTO_SNDSYN; C++ bool ABI is one byte on Linux.
        self.option(2, &0u8)?; // SRTO_RCVSYN.
        self.option(
            7,
            &libc::linger {
                l_onoff: 0,
                l_linger: 0,
            },
        )
    }
    pub fn stream_id(&self) -> io::Result<String> {
        let mut bytes = [0u8; 513];
        let mut len = bytes.len() as c_int;
        // SAFETY: Writable buffer and length both live through the call;
        // SRTO_STREAMID copies at most the supplied number of bytes.
        let result = unsafe { (self.api.get)(self.id, 46, bytes.as_mut_ptr().cast(), &mut len) };
        if result != 0 || !(0..=513).contains(&len) {
            return Err(failure("invalid SRT stream ID"));
        }
        let mut len = len as usize;
        if len > 0 && bytes[len - 1] == 0 {
            len -= 1;
        }
        if len > 512 {
            return Err(failure("invalid SRT stream ID"));
        }
        String::from_utf8(bytes[..len].to_vec()).map_err(|_| failure("invalid SRT stream ID"))
    }
    /// True means the entire message was accepted by libsrt, not remotely ACKed.
    pub fn try_send(&self, data: &[u8]) -> io::Result<bool> {
        if data.is_empty() || data.len() > 1316 {
            return Err(failure("invalid SRT message size"));
        }
        // SAFETY: Read-only slice lives through the call; message send copies
        // its bytes and does not retain the pointer. Nonblocking is mandatory.
        let result =
            unsafe { (self.api.send)(self.id, data.as_ptr().cast(), data.len() as c_int, -1, 1) };
        if result == data.len() as c_int {
            return Ok(true);
        }
        // SAFETY: Thread-local numeric error getter accepts a null OS-error output.
        if result < 0 && unsafe { (self.api.error)(std::ptr::null_mut()) } == 6001 {
            return Ok(false);
        }
        Err(failure("SRT output failed"))
    }
    pub fn is_connected(&self) -> bool {
        // SAFETY: Valid owned descriptor and process-lifetime function pointer.
        unsafe { (self.api.state)(self.id) == 5 }
    }
}
pub struct Listener {
    socket: Socket,
    address: SocketAddr,
    settings: Settings,
}
impl Listener {
    pub fn bind(address: SocketAddr, settings: Settings) -> io::Result<Self> {
        let socket = Socket::create()?;
        socket.option(50, &0i32)?; // SRTO_TRANSTYPE = SRTT_LIVE.
        socket.option(21, &1u8)?; // SRTO_SENDER.
        socket.option(15, &0u8)?; // SRTO_REUSEADDR: no sharing an occupied UDP port.
        socket.option(49, &1316i32)?; // SRTO_PAYLOADSIZE.
        socket.option(23, &(settings.latency_millis as i32))?;
        socket.option(5, &(1316i32 * 128))?; // Finite application send buffer.
        socket.option(55, &5000i32)?; // SRTO_PEERIDLETIMEO.
        socket.option(53, &1u8)?; // SRTO_ENFORCEDENCRYPTION.
        if !settings.passphrase.is_empty() {
            // SAFETY: Validated ASCII string, length10..79, copied by libsrt.
            let result = unsafe {
                (socket.api.set)(
                    socket.id,
                    26,
                    settings.passphrase.as_ptr().cast(),
                    settings.passphrase.len() as c_int,
                )
            };
            if result != 0 {
                return Err(failure("SRT encryption setup failed"));
            }
            socket.option(27, &16i32)?;
        }
        socket.nonblocking()?;
        let (storage, len) = encode_address(address);
        // SAFETY: Properly aligned sockaddr_storage contains the selected
        // Linux IPv4/IPv6 structure and exact initialized structure length.
        let result =
            unsafe { (socket.api.bind)(socket.id, std::ptr::from_ref(&storage).cast(), len) };
        if result != 0 {
            return Err(failure("SRT listener bind failed"));
        }
        // SAFETY: Valid bound descriptor; backlog bounds the completed queue.
        if unsafe { (socket.api.listen)(socket.id, 32) } != 0 {
            return Err(failure("SRT listener setup failed"));
        }
        // SAFETY: Zeroed C integer/POD buffer is valid sockaddr_storage.
        let mut storage: sockaddr_storage = unsafe { mem::zeroed() };
        let mut len = mem::size_of_val(&storage) as c_int;
        // SAFETY: libsrt fills this aligned buffer, bounded by the supplied length.
        if unsafe {
            (socket.api.address)(socket.id, std::ptr::from_mut(&mut storage).cast(), &mut len)
        } != 0
        {
            return Err(failure("SRT listener address unavailable"));
        }
        Ok(Self {
            socket,
            address: decode_address(&storage, len)?,
            settings,
        })
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    pub fn settings(&self) -> &Settings {
        &self.settings
    }
    pub fn accept(&self) -> io::Result<Option<(Socket, SocketAddr)>> {
        // SAFETY: Zeroed C integer/POD buffer is valid sockaddr_storage.
        let mut storage: sockaddr_storage = unsafe { mem::zeroed() };
        let mut len = mem::size_of_val(&storage) as c_int;
        // SAFETY: Owned listener with nonblocking accept; writable address
        // buffer and length live through the call and are not retained.
        let id = unsafe {
            (self.socket.api.accept)(
                self.socket.id,
                std::ptr::from_mut(&mut storage).cast(),
                &mut len,
            )
        };
        if id < 0 {
            // SAFETY: Thread-local numeric error; optional output is null.
            if unsafe { (self.socket.api.error)(std::ptr::null_mut()) } == 6002 {
                return Ok(None);
            }
            return Err(failure("SRT accept failed"));
        }
        let socket = Socket {
            api: Arc::clone(&self.socket.api),
            id,
        };
        socket.nonblocking()?;
        Ok(Some((socket, decode_address(&storage, len)?)))
    }
}
fn encode_address(address: SocketAddr) -> (sockaddr_storage, c_int) {
    // SAFETY: These Linux C address structures contain only integer/POD data;
    // sockaddr_storage has sufficient size/alignment for both alternatives.
    unsafe {
        let mut storage: sockaddr_storage = mem::zeroed();
        match address {
            SocketAddr::V4(a) => {
                let value = libc::sockaddr_in {
                    sin_family: libc::AF_INET as _,
                    sin_port: a.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(a.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                std::ptr::write(
                    std::ptr::from_mut(&mut storage).cast::<libc::sockaddr_in>(),
                    value,
                );
                (storage, mem::size_of::<libc::sockaddr_in>() as c_int)
            }
            SocketAddr::V6(a) => {
                let value = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as _,
                    sin6_port: a.port().to_be(),
                    sin6_flowinfo: a.flowinfo().to_be(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: a.ip().octets(),
                    },
                    sin6_scope_id: a.scope_id(),
                };
                std::ptr::write(
                    std::ptr::from_mut(&mut storage).cast::<libc::sockaddr_in6>(),
                    value,
                );
                (storage, mem::size_of::<libc::sockaddr_in6>() as c_int)
            }
        }
    }
}
fn decode_address(storage: &sockaddr_storage, len: c_int) -> io::Result<SocketAddr> {
    if len < 0 || len as usize > mem::size_of_val(storage) {
        return Err(failure("invalid SRT peer address"));
    }
    // SAFETY: Family and supplied length are checked before reading the
    // corresponding POD value from the fully initialized storage buffer.
    unsafe {
        match storage.ss_family as c_int {
            libc::AF_INET if len as usize >= mem::size_of::<libc::sockaddr_in>() => {
                let a = std::ptr::read(std::ptr::from_ref(storage).cast::<libc::sockaddr_in>());
                Ok(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::from(a.sin_addr.s_addr.to_ne_bytes())),
                    u16::from_be(a.sin_port),
                ))
            }
            libc::AF_INET6 if len as usize >= mem::size_of::<libc::sockaddr_in6>() => {
                let a = std::ptr::read(std::ptr::from_ref(storage).cast::<libc::sockaddr_in6>());
                Ok(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(a.sin6_addr.s6_addr),
                    u16::from_be(a.sin6_port),
                    u32::from_be(a.sin6_flowinfo),
                    a.sin6_scope_id,
                )))
            }
            _ => Err(failure("invalid SRT peer address")),
        }
    }
}
