//! Narrow libSRTP 2.x ABI. No custom cryptography or vendor media component.
use std::{
    ffi::{c_int, c_ulong, c_void},
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    sync::OnceLock,
};
#[repr(C)]
#[derive(Default)]
struct CryptoPolicy {
    cipher_type: u32,
    cipher_key_len: c_int,
    auth_type: u32,
    auth_key_len: c_int,
    auth_tag_len: c_int,
    sec_serv: c_int,
}
#[repr(C)]
#[derive(Default)]
struct Ssrc {
    kind: c_int,
    value: u32,
}
#[repr(C)]
#[derive(Default)]
struct Policy {
    ssrc: Ssrc,
    rtp: CryptoPolicy,
    rtcp: CryptoPolicy,
    key: *mut u8,
    keys: *mut *mut c_void,
    num_master_keys: c_ulong,
    deprecated_ekt: *mut c_void,
    window_size: c_ulong,
    allow_repeat_tx: c_int,
    enc_xtn_hdr: *mut c_int,
    enc_xtn_hdr_count: c_int,
    next: *mut Policy,
}
type Transform = unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_int) -> c_int;
struct Api {
    _library: libloading::Library,
    create: unsafe extern "C" fn(*mut *mut c_void, *const Policy) -> c_int,
    dealloc: unsafe extern "C" fn(*mut c_void) -> c_int,
    remove: unsafe extern "C" fn(*mut c_void, u32) -> c_int,
    rtp_default: unsafe extern "C" fn(*mut CryptoPolicy),
    protect: Transform,
    unprotect: Transform,
    protect_rtcp: Transform,
    unprotect_rtcp: Transform,
}
impl Api {
    unsafe fn load() -> Result<Self, &'static str> {
        // Fixed SONAME: never search any proprietary directory or a caller-provided path.
        let lib = unsafe { libloading::Library::new("libsrtp2.so.1") }
            .map_err(|_| "system libsrtp2 is unavailable")?;
        unsafe fn get<T: Copy>(lib: &libloading::Library, name: &[u8]) -> Result<T, &'static str> {
            Ok(*unsafe { lib.get::<T>(name) }.map_err(|_| "system libsrtp2 ABI is unsupported")?)
        }
        let init: unsafe extern "C" fn() -> c_int = unsafe { get(&lib, b"srtp_init\0") }?;
        let api = Self {
            create: unsafe { get(&lib, b"srtp_create\0") }?,
            dealloc: unsafe { get(&lib, b"srtp_dealloc\0") }?,
            remove: unsafe { get(&lib, b"srtp_remove_stream\0") }?,
            rtp_default: unsafe { get(&lib, b"srtp_crypto_policy_set_rtp_default\0") }?,
            protect: unsafe { get(&lib, b"srtp_protect\0") }?,
            unprotect: unsafe { get(&lib, b"srtp_unprotect\0") }?,
            protect_rtcp: unsafe { get(&lib, b"srtp_protect_rtcp\0") }?,
            unprotect_rtcp: unsafe { get(&lib, b"srtp_unprotect_rtcp\0") }?,
            _library: lib,
        };
        // OnceLock calls initialization once before any session creation. Library stays loaded.
        if unsafe { init() } != 0 {
            return Err("system libsrtp2 initialization failed");
        }
        Ok(api)
    }
}
static API: OnceLock<Result<Api, &'static str>> = OnceLock::new();
fn api() -> Result<&'static Api, &'static str> {
    API.get_or_init(|| unsafe { Api::load() })
        .as_ref()
        .map_err(|e| *e)
}
pub fn availability() -> bool {
    api().is_ok()
}
fn erase(bytes: &mut [u8]) {
    for b in bytes {
        unsafe { std::ptr::write_volatile(b, 0) }
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
}
struct Secret([u8; 30]);
impl Drop for Secret {
    fn drop(&mut self) {
        erase(&mut self.0)
    }
}
struct Encoded(Vec<u8>);
impl Drop for Encoded {
    fn drop(&mut self) {
        erase(&mut self.0)
    }
}
/// Read through the validated opened descriptor. Errors never include path or key data.
pub fn read_key(path: &Path) -> Result<[u8; 30], &'static str> {
    if !path.is_absolute() {
        return Err("SRTP key file must be absolute");
    }
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOCTTY)
        .open(path)
        .map_err(|_| "SRTP key file cannot be opened")?;
    let metadata = file
        .metadata()
        .map_err(|_| "SRTP key file metadata unavailable")?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
        || metadata.len() > 128
    {
        return Err("SRTP key file must be bounded, regular and owner-only");
    }
    let mut bytes = Encoded(vec![0; 129]);
    let mut count = 0;
    while count < 129 {
        let n = file
            .read(&mut bytes.0[count..])
            .map_err(|_| "SRTP key file cannot be read")?;
        if n == 0 {
            break;
        }
        count += n;
    }
    if count > 128 {
        return Err("SRTP key file exceeds size limit");
    }
    use base64::Engine;
    let trimmed = bytes.0[..count].trim_ascii();
    let mut decoded = Secret([0; 30]);
    let size = base64::engine::general_purpose::STANDARD
        .decode_slice(trimmed, &mut decoded.0)
        .map_err(|_| "SRTP key file must contain a 30-byte base64 key and salt")?;
    if size != 30 {
        return Err("SRTP key and salt must contain exactly 30 bytes");
    }
    Ok(decoded.0)
}

pub struct Session {
    ctx: *mut c_void,
    api: &'static Api,
    sender: bool,
    pinned: Option<u32>,
    candidate: Option<u32>,
}
// The opaque context is used exclusively via &mut Session; sessions are never shared
// or cloned. libSRTP owns per-session state and the global library stays initialized.
unsafe impl Send for Session {}
impl Drop for Session {
    fn drop(&mut self) {
        unsafe { (self.api.dealloc)(self.ctx) };
    }
}
impl Session {
    pub fn new(key: [u8; 30], sender: Option<u32>) -> Result<Self, &'static str> {
        let mut key = Secret(key);
        Self::create(&mut key, sender, sender.is_some())
    }
    fn create(key: &mut Secret, sender: Option<u32>, outbound: bool) -> Result<Self, &'static str> {
        let api = api()?;
        let mut policy = Policy {
            ssrc: Ssrc {
                kind: if sender.is_some() {
                    1
                } else if outbound {
                    3
                } else {
                    2
                },
                value: sender.unwrap_or(0),
            },
            key: key.0.as_mut_ptr(),
            window_size: 128,
            ..Default::default()
        };
        // libSRTP fills exactly the repr(C) crypto policy. This is AES_CM_128_HMAC_SHA1_80
        // for both RTP and RTCP (RTCP gets its own index and replay state).
        unsafe {
            (api.rtp_default)(&mut policy.rtp);
            (api.rtp_default)(&mut policy.rtcp);
        }
        if policy.rtp.cipher_key_len != 30
            || policy.rtp.auth_tag_len != 10
            || policy.rtp.sec_serv != 3
            || policy.rtcp.cipher_key_len != 30
            || policy.rtcp.auth_tag_len != 10
            || policy.rtcp.sec_serv != 3
        {
            return Err("system libsrtp2 protection profile is unsupported");
        }
        let mut ctx = std::ptr::null_mut();
        let result = unsafe { (api.create)(&mut ctx, &policy) };
        if result != 0 || ctx.is_null() {
            return Err("SRTP session initialization failed");
        }
        Ok(Self {
            ctx,
            api,
            sender: outbound,
            pinned: sender,
            candidate: None,
        })
    }
    /// Keep the authenticated candidate's replay/ROC state until another SSRC
    /// arrives, but do not let malformed plaintext own this endpoint. The caller
    /// may use this only before its first structurally valid media/control peer.
    pub(crate) fn discard_candidate(&mut self) -> Result<(), &'static str> {
        if self.sender {
            return Err("SRTP session direction mismatch");
        }
        self.candidate = self.pinned.take();
        Ok(())
    }
    pub fn protect(&mut self, packet: &mut Vec<u8>, rtcp: bool) -> Result<(), &'static str> {
        if !self.sender {
            return Err("SRTP session direction mismatch");
        }
        self.transform(packet, rtcp, true)
    }
    pub fn unprotect(&mut self, packet: &mut Vec<u8>, rtcp: bool) -> Result<(), &'static str> {
        if self.sender {
            return Err("SRTP session direction mismatch");
        }
        self.transform(packet, rtcp, false)
    }
    fn transform(
        &mut self,
        packet: &mut Vec<u8>,
        rtcp: bool,
        protect: bool,
    ) -> Result<(), &'static str> {
        let min = if rtcp { 8 } else { 12 };
        if packet.len() < min || packet.len() > 2048 || packet[0] >> 6 != 2 {
            return Err("SRTP packet framing rejected");
        }
        let offset = if rtcp { 4 } else { 8 };
        let ssrc = u32::from_be_bytes(packet[offset..offset + 4].try_into().unwrap());
        if self.pinned.is_some_and(|s| s != ssrc) {
            return Err("SRTP source rejected");
        }
        if !protect && self.pinned.is_none() && self.candidate.is_some_and(|prior| prior != ssrc) {
            // Public API takes network byte order. Retire only the uncommitted
            // candidate before authenticating a new SSRC; at most one stream
            // context exists, and the generation's wildcard key stays loaded.
            let prior = self.candidate.take().unwrap();
            if unsafe { (self.api.remove)(self.ctx, prior.to_be()) } != 0 {
                return Err("SRTP candidate state could not be retired");
            }
        }
        // libSRTP requires word-aligned storage and enough writable trailer space.
        // 576 initialized u32s hold 2048 bytes plus the maximum 148-byte library trailer.
        let mut words = [0u32; 576];
        let buffer = unsafe {
            std::slice::from_raw_parts_mut(
                words.as_mut_ptr().cast::<u8>(),
                std::mem::size_of_val(&words),
            )
        };
        buffer[..packet.len()].copy_from_slice(packet);
        let mut length = packet.len() as c_int;
        let operation = match (protect, rtcp) {
            (true, false) => self.api.protect,
            (false, false) => self.api.unprotect,
            (true, true) => self.api.protect_rtcp,
            (false, true) => self.api.unprotect_rtcp,
        };
        let code = unsafe { operation(self.ctx, buffer.as_mut_ptr().cast(), &mut length) };
        let result = if code == 0 && length >= min as c_int && length as usize <= buffer.len() {
            packet.clear();
            packet.extend_from_slice(&buffer[..length as usize]);
            self.pinned.get_or_insert(ssrc);
            self.candidate = None;
            Ok(())
        } else {
            Err("SRTP authentication, replay or protection rejected")
        };
        erase(buffer);
        result
    }
}
/// Both directions load one generation's key, then erase all application buffers.
pub fn sessions(path: &Path, sender: u32) -> Result<(Session, Session), &'static str> {
    let mut key = Secret(read_key(path)?);
    let receive = Session::create(&mut key, None, false)?;
    let transmit = Session::create(&mut key, Some(sender), true)?;
    Ok((receive, transmit))
}
/// One immutable key snapshot for all bounded lanes in a transport generation.
/// A wildcard transmitter is only for trusted private decoder feedback; after
/// its first valid report Session pins that SSRC, just like specific senders.
pub(crate) fn track_sessions(
    path: &Path,
    senders: &[Option<u32>],
) -> Result<Vec<(Session, Session)>, &'static str> {
    if senders.is_empty() || senders.len() > 8 {
        return Err("SRTP requires 1..8 track contexts");
    }
    let mut key = Secret(read_key(path)?);
    senders
        .iter()
        .map(|sender| {
            Ok((
                Session::create(&mut key, None, false)?,
                Session::create(&mut key, *sender, true)?,
            ))
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_candidate_preserves_rollover_and_replay_until_valid_media() {
        let mut ts = vec![0xff; 188];
        ts[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
        let mut tx = Session::new([0x31; 30], Some(42)).unwrap();
        let mut rx = Session::new([0x31; 30], None).unwrap();
        let mut wrong = crate::direct_rtp::packet::packet(65535, 0, 42, &ts);
        wrong[1] = 96;
        tx.protect(&mut wrong, false).unwrap();
        let mut received = wrong.clone();
        rx.unprotect(&mut received, false).unwrap();
        rx.discard_candidate().unwrap();
        assert!(rx.unprotect(&mut wrong, false).is_err());
        let plain = crate::direct_rtp::packet::packet(0, 0, 42, &ts);
        let mut cipher = plain.clone();
        tx.protect(&mut cipher, false).unwrap();
        rx.unprotect(&mut cipher, false).unwrap();
        assert_eq!(cipher, plain);
    }
    #[test]
    fn libsrtp2_policy_matches_verified_public_c_abi() {
        assert_eq!(std::mem::size_of::<CryptoPolicy>(), 24);
        assert_eq!(std::mem::size_of::<Ssrc>(), 8);
        assert_eq!(std::mem::offset_of!(Policy, rtp), 8);
        assert_eq!(std::mem::offset_of!(Policy, rtcp), 32);
        assert_eq!(std::mem::offset_of!(Policy, key), 56);
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(std::mem::size_of::<Policy>(), 128);
            assert_eq!(std::mem::offset_of!(Policy, window_size), 88);
            assert_eq!(std::mem::offset_of!(Policy, allow_repeat_tx), 96);
            assert_eq!(std::mem::offset_of!(Policy, enc_xtn_hdr), 104);
            assert_eq!(std::mem::offset_of!(Policy, next), 120);
        }
    }
}
