//! Explicit owner-scheduled refresh; no timer, retry, route activation or failover.
use super::super::super::{CoordinatedInventory, CoordinationFailure, SourceHandle};
use super::*;

pub struct SharedRenewal {
    pub inventory: CoordinatedInventory,
    pub renewed: usize,
    pub retired: usize,
}
impl SharedInputs {
    /// Preserve current demand while observing complete inventory. Equivalent
    /// routes renew atomically before the old deadline; failed/cancelled attempts
    /// never extend it. The caller owns/schedules this future. No lease resurrection.
    pub async fn renew(
        &self,
        handle: &SourceHandle,
        lifetime: Duration,
    ) -> Result<SharedRenewal, SharedFailure> {
        let (epoch, pending) = {
            let registry = self
                .workers
                .lock()
                .map_err(|_| SharedFailure::Unavailable)?;
            let anchor = registry
                .workers
                .values()
                .filter_map(Weak::upgrade)
                .find(|w| same_source(&w.handle, handle) && w.current().is_ok())
                .ok_or(SharedFailure::Unavailable)?;
            let route = copy_route(&anchor.route.lock().unwrap());
            let pending = self
                .owner
                .at_handle(handle, || {
                    handle
                        .authority
                        .begin_modern_renewal(&route.candidate, lifetime)
                        .map_err(CoordinationFailure::Lease)
                })
                .map_err(|e| SharedFailure::Authority(AdmissionFailure::Coordination(e)))?;
            (registry.epoch.clone(), pending)
        };
        let observed = pending.observe(&handle.stamp.identity.ids).await;
        let registry = self
            .workers
            .lock()
            .map_err(|_| SharedFailure::Unavailable)?;
        if !Arc::ptr_eq(&epoch, &registry.epoch) {
            return Err(SharedFailure::Closed);
        }
        let (inventory, (renewed, retired)) = self
            .owner
            .at_handle(handle, || {
                handle
                    .authority
                    .commit_modern_renewal(pending, observed, |inventory| {
                        let mut updates = Vec::new();
                        let (mut renewed, mut retired) = (0, 0);
                        for worker in registry
                            .workers
                            .values()
                            .filter_map(Weak::upgrade)
                            .filter(|w| same_source(&w.handle, handle) && !w.cancel.is_cancelled())
                        {
                            let old = worker.route.lock().unwrap();
                            let next = inventory
                                .candidate(&worker.name)
                                .ok()
                                .filter(|c| old.candidate.equivalent_route(c));
                            drop(old);
                            if next.is_some() {
                                renewed += 1
                            } else {
                                retired += 1
                            }
                            updates.push((worker, next));
                        }
                        ((renewed, retired), move || {
                            for (worker, next) in updates {
                                if let Some(candidate) = next {
                                    *worker.route.lock().unwrap() =
                                        super::super::super::CoordinatedCandidate {
                                            handle: worker.handle.clone(),
                                            candidate,
                                        };
                                } else {
                                    worker.close();
                                }
                            }
                        })
                    })
                    .map_err(CoordinationFailure::Lease)
            })
            .map_err(|e| SharedFailure::Authority(AdmissionFailure::Coordination(e)))?;
        Ok(SharedRenewal {
            inventory: CoordinatedInventory {
                handle: handle.clone(),
                inventory,
            },
            renewed,
            retired,
        })
    }
}
