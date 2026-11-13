//! Explicit owner-scheduled refresh; no timer, retry, route activation or failover.
use super::super::super::{CoordinatedInventory, CoordinationFailure, SourceHandle};
use super::*;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

pub(super) struct RenewalContext {
    owner: Arc<ModernSourceCoordinator>,
    workers: Arc<Mutex<Registry>>,
    gates: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
    eof_slots: Semaphore,
}
impl RenewalContext {
    pub(super) fn new(owner: Arc<ModernSourceCoordinator>, workers: Arc<Mutex<Registry>>) -> Self {
        Self {
            owner,
            workers,
            gates: Mutex::new(HashMap::new()),
            eof_slots: Semaphore::new(4),
        }
    }

    fn gate(&self, handle: &SourceHandle) -> Result<Arc<AsyncMutex<()>>, SharedFailure> {
        let mut gates = self.gates.lock().map_err(|_| SharedFailure::Unavailable)?;
        gates.retain(|_, gate| gate.strong_count() > 0);
        Ok(gates
            .entry(handle.stamp.key.clone())
            .or_default()
            .upgrade()
            .unwrap_or_else(|| {
                let gate = Arc::new(AsyncMutex::new(()));
                gates.insert(handle.stamp.key.clone(), Arc::downgrade(&gate));
                gate
            }))
    }

    fn remaining(&self, handle: &SourceHandle) -> Result<Duration, SharedFailure> {
        let registry = self
            .workers
            .lock()
            .map_err(|_| SharedFailure::Unavailable)?;
        let anchor = registry
            .workers
            .values()
            .filter_map(Weak::upgrade)
            .find(|worker| same_source(&worker.handle, handle) && worker.current().is_ok())
            .ok_or(SharedFailure::Unavailable)?;
        Ok(anchor
            .route
            .lock()
            .unwrap()
            .expires_at()
            .saturating_duration_since(std::time::Instant::now()))
    }

    async fn renew(
        &self,
        handle: &SourceHandle,
        lifetime: Duration,
    ) -> Result<SharedRenewal, SharedFailure> {
        let gate = self.gate(handle)?;
        let _turn = gate.lock().await;
        self.renew_inner(handle, lifetime).await
    }

    pub(super) async fn renew_after_eof(
        &self,
        handle: &SourceHandle,
        lifetime: Duration,
    ) -> Result<SharedRenewal, SharedFailure> {
        let deadline = self.remaining(handle)?.min(Duration::from_secs(5));
        if deadline.is_zero() {
            return Err(SharedFailure::Unavailable);
        }
        tokio::time::timeout(deadline, async {
            let _slot = self
                .eof_slots
                .acquire()
                .await
                .map_err(|_| SharedFailure::Closed)?;
            let gate = self.gate(handle)?;
            let _turn = gate.lock().await;
            self.renew_inner(handle, lifetime).await
        })
        .await
        .unwrap_or(Err(SharedFailure::Unavailable))
    }
}

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
        self.renewal.renew(handle, lifetime).await
    }

    pub async fn renew_after_eof(
        &self,
        handle: &SourceHandle,
        lifetime: Duration,
    ) -> Result<SharedRenewal, SharedFailure> {
        self.renewal.renew_after_eof(handle, lifetime).await
    }
}
impl RenewalContext {
    async fn renew_inner(
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
                    .commit_modern_renewal(
                        pending,
                        observed,
                        |error| {
                            for worker in registry
                                .workers
                                .values()
                                .filter_map(Weak::upgrade)
                                .filter(|w| {
                                    same_source(&w.handle, handle) && !w.cancel.is_cancelled()
                                })
                            {
                                if worker
                                    .scope
                                    .as_ref()
                                    .is_some_and(|scope| scope.reject_inventory(error))
                                {
                                    worker.close();
                                }
                            }
                        },
                        |inventory, current| {
                            let mut updates = Vec::new();
                            let (mut renewed, mut retired) = (0, 0);
                            for worker in registry
                                .workers
                                .values()
                                .filter_map(Weak::upgrade)
                                .filter(|w| {
                                    same_source(&w.handle, handle) && !w.cancel.is_cancelled()
                                })
                            {
                                let old = worker.route.lock().unwrap();
                                let valid = current(&old.candidate);
                                let projected = inventory.candidate(&worker.name);
                                let rejected = if valid {
                                    projected.as_ref().err().copied()
                                } else {
                                    None
                                };
                                let next = projected
                                    .ok()
                                    .filter(|c| valid && old.candidate.equivalent_route(c));
                                drop(old);
                                if next.is_some() {
                                    renewed += 1
                                } else {
                                    retired += 1
                                }
                                updates.push((worker, next, rejected));
                            }
                            ((renewed, retired), move || {
                                for (worker, next, rejected) in updates {
                                    if let Some(candidate) = next {
                                        *worker.route.lock().unwrap() =
                                            super::super::super::CoordinatedCandidate {
                                                handle: worker.handle.clone(),
                                                candidate,
                                            };
                                    } else {
                                        if let (Some(scope), Some(error)) =
                                            (&worker.scope, rejected)
                                        {
                                            scope.reject_inventory(error);
                                        }
                                        worker.close();
                                    }
                                }
                            })
                        },
                    )
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
