//! Bounded, clock-injected FXLink v1 packet/FEC engine. Authentication belongs to QUIC.
pub mod budget;
pub mod config;
pub mod fec;
pub mod packet;
pub mod receiver;
pub use budget::{Budget, Reservation};
pub use config::{Address, Capabilities, Features, Settings};
pub use fec::{EncodedRecord, Encoder};
pub use packet::Layout;
pub use receiver::{ReceivedRecord, Receiver, RepairRequest, Stats};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidSettings(&'static str),
    InvalidAddress(&'static str),
    InvalidPacket(&'static str),
    InvalidRecord(&'static str),
    BudgetExceeded,
    Stale,
    Retired,
    Expired { record_id: u64 },
    Corrupt,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FXLink: {self:?}")
    }
}
impl std::error::Error for Error {}
pub const MAX_RECORD_LEN: usize = 16 * 1024 * 1024 + 4;
pub const MAX_BLOCKS_PER_RECORD: usize = 2048;
pub(crate) fn validate_record(record: &[u8], limit: usize) -> Result<(), Error> {
    if record.len() < 12
        || record.len() > limit
        || u32::from_be_bytes(record[..4].try_into().unwrap()) as usize != record.len() - 4
    {
        return Err(Error::InvalidRecord("native length prefix"));
    }
    Ok(())
}

mod bootstrap;
mod bootstrap_source;
mod client;
mod control;
mod demand;
mod drain_wake;
mod feedback;
mod feedback_copy;
#[cfg(test)]
mod feedback_copy_tests;
mod flow;
#[cfg(test)]
mod flow_tests;
#[cfg(test)]
mod gap_tests;
mod geometry;
#[cfg(test)]
mod jitter_tests;
mod listener;
#[cfg(test)]
mod middle_gap_tests;
mod packet_queue;
mod parity;
#[cfg(test)]
mod parity_transport_tests;
mod repair;
mod repair_cache;
#[cfg(test)]
mod scheduling_tests;
mod service_timing;
mod service_trace;
mod transport;
pub use client::{ClientPool, Subscription};
pub use listener::Listener;
#[cfg(test)]
pub(crate) use transport::test_lock;
pub use transport::{Provider, RecordStream, Request, Source, Telemetry};

#[cfg(test)]
mod compact_tests;

#[cfg(test)]
mod compact_transport_tests;

mod short_fec;

#[cfg(test)]
mod trimmed_tests;

#[cfg(test)]
mod trimmed_transport_tests;

#[cfg(test)]
mod parity_repair_tests;

#[cfg(test)]
mod parity_repair_transport_tests;
