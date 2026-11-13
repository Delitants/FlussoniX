//! Optional authenticated record-shape metadata. Never a media/timing authority.
mod diagnostics;
mod plan;
#[cfg(test)]
mod pressure_tests;
pub(super) use plan::plan;
#[cfg(test)]
mod plan_tests;
mod quota;
#[cfg(test)]
mod quota_tests;
pub(super) mod receive;
#[cfg(test)]
mod receive_tests;
pub(super) mod send;
#[cfg(test)]
mod send_tests;
pub(super) mod wire;
#[cfg(test)]
mod wire_tests;
