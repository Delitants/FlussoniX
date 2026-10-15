//! Control-to-playback ownership with bounded segmented session credits.
use super::*;
use crate::admission_credit::OutputCredit;
use std::sync::{Mutex as StdMutex, Weak};
struct Handoff {
    grant: Grant,
    expires: Instant,
}
struct Segmented {
    credit: Arc<OutputCredit>,
    cancel: tokio_util::sync::CancellationToken,
    expires: Instant,
}
struct TrackedCredit {
    shared: bool,
    identity: Option<String>,
    credit: Weak<OutputCredit>,
    cancel: tokio_util::sync::CancellationToken,
}
#[derive(Default)]
struct State {
    handoffs: HashMap<String, Handoff>,
    segmented: HashMap<String, Segmented>,
    credits: Vec<TrackedCredit>,
}
impl State {
    fn prune(&mut self) {
        self.handoffs
            .retain(|_, h| h.expires > Instant::now() && !h.grant.is_cancelled());
        self.segmented
            .retain(|_, s| s.expires > Instant::now() && !s.cancel.is_cancelled());
        self.credits
            .retain(|tracked| tracked.credit.strong_count() > 0 && !tracked.cancel.is_cancelled());
    }
}
#[derive(Default)]
pub(super) struct Registry(StdMutex<State>);
impl Registry {
    pub(super) fn insert(&self, ticket: String, mut grant: Grant, mbps: f64) {
        grant.release_capacity_reservation();
        let mut state = self.0.lock().unwrap();
        state.prune();
        let credit = Arc::new(OutputCredit::new(mbps));
        state.credits.push(TrackedCredit {
            shared: false,
            identity: grant.session_identity().map(str::to_owned),
            credit: Arc::downgrade(&credit),
            cancel: grant.cancellation_token(),
        });
        grant.set_output_credit(Some(credit));
        state.handoffs.insert(
            ticket,
            Handoff {
                grant,
                expires: Instant::now() + Duration::from_secs(5),
            },
        );
    }
    pub(super) fn take(&self, grant: &Grant, segmented: bool) -> Option<Arc<OutputCredit>> {
        let identity = grant.session_identity()?;
        let mut state = self.0.lock().unwrap();
        state.prune();
        if let Some(s) = state.segmented.get_mut(identity) {
            s.expires = Instant::now() + Duration::from_secs(30);
            return Some(s.credit.clone());
        }
        let ticket = state
            .handoffs
            .iter()
            .find(|(_, h)| h.grant.session_identity() == Some(identity))
            .map(|(t, _)| t.clone());
        let credit = if let Some(ticket) = ticket {
            state.handoffs.remove(&ticket)?.grant.take_output_credit()?
        } else {
            // A live M4F signal response still owns the credit after a long
            // segment pause. Its later finite media must remain attributable.
            state
                .credits
                .iter()
                .filter(|tracked| tracked.identity.as_deref() == Some(identity))
                .find_map(|tracked| tracked.credit.upgrade())?
        };
        if segmented {
            for tracked in &mut state.credits {
                if Weak::ptr_eq(&tracked.credit, &Arc::downgrade(&credit)) {
                    tracked.shared = true;
                }
            }
            state.segmented.insert(
                identity.into(),
                Segmented {
                    credit: credit.clone(),
                    cancel: grant.cancellation_token(),
                    expires: Instant::now() + Duration::from_secs(30),
                },
            );
        }
        Some(credit)
    }
    pub(super) fn prune(&self) {
        self.0.lock().unwrap().prune();
    }
    pub(super) fn sample(&self, from: Option<Instant>, to: Instant) {
        let mut state = self.0.lock().unwrap();
        state.prune();
        for credit in state
            .credits
            .iter()
            .filter_map(|tracked| tracked.credit.upgrade())
        {
            credit.sample(from, to);
        }
    }
    pub(super) fn unmeasured(
        &self,
        window: Option<(Instant, Instant)>,
        http_mbps: Option<f64>,
    ) -> f64 {
        let mut state = self.0.lock().unwrap();
        state.prune();
        let credits = state
            .credits
            .iter()
            .filter_map(|tracked| tracked.credit.upgrade().map(|credit| (tracked, credit)))
            .collect::<Vec<_>>();
        let measured: f64 = credits.iter().map(|(_, c)| c.measured(window)).sum();
        // Counter reads can straddle a send. Never discount more HTTP bytes
        // than the transport sampler actually represents in this interval.
        let factor = if measured > 0.0 {
            (http_mbps.unwrap_or(0.0) / measured).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let mut outstanding = 0.0;
        let mut shared: HashMap<&str, (f64, f64)> = HashMap::new();
        for (tracked, credit) in &credits {
            let measured = credit.measured(window) * factor;
            if tracked.shared
                && let Some(identity) = tracked.identity.as_deref()
            {
                // Signals may live separately, but finite HLS/M4F media has
                // one authorized session identity. Aggregate activated output
                // before clamping; pending tickets never join this discount.
                let group = shared.entry(identity).or_default();
                group.0 += credit.estimate();
                group.1 += measured;
            } else {
                outstanding += (credit.estimate() - measured).max(0.0);
            }
        }
        outstanding
            + shared
                .values()
                .map(|(estimate, measured)| (estimate - measured).max(0.0))
                .sum::<f64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn viewer(
        auth: &PlaybackAuth,
        policy: PolicySnapshot,
        token: &str,
        playback: bool,
    ) -> Grant {
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            token: token.into(),
            ..Default::default()
        };
        let outcome = if playback {
            auth.authorize(policy, request).await
        } else {
            auth.authorize_control(policy, request).await
        };
        let AuthOutcome::Allowed(grant) = outcome else {
            panic!("owned viewer denied")
        };
        grant
    }
    #[tokio::test]
    async fn measured_output_does_not_discount_another_viewers_pending_bandwidth() {
        let auth = PlaybackAuth::new(10);
        let policy = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let registry = Registry::default();
        let from = Instant::now();
        for token in ["a", "b"] {
            registry.insert(
                token.into(),
                viewer(&auth, policy.clone(), token, false).await,
                40.0,
            );
        }
        let mut a = viewer(&auth, policy.clone(), "a", true).await;
        a.set_output_credit(registry.take(&a, false));
        a.add_bytes(5_000_000); // 40 Mbps for the first viewer only.
        let to = from + Duration::from_secs(1);
        registry.sample(Some(from), to);
        assert_eq!(registry.unmeasured(Some((from, to)), Some(40.0)), 40.0);
        assert_eq!(
            registry.unmeasured(Some((from, to)), Some(20.0)),
            60.0,
            "credit cannot discount bytes absent from transport telemetry"
        );
        assert_eq!(registry.unmeasured(None, None), 80.0);
    }
    #[tokio::test]
    async fn segmented_credit_survives_finite_grants_and_stops_on_revocation_or_expiry() {
        let auth = PlaybackAuth::new(10);
        let policy = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let registry = Registry::default();
        registry.insert(
            "a".into(),
            viewer(&auth, policy.clone(), "a", false).await,
            40.0,
        );
        let mut a = viewer(&auth, policy.clone(), "a", true).await;
        let identity = a.session_identity().unwrap().to_owned();
        a.set_output_credit(registry.take(&a, true));
        drop(a); // A finite manifest has returned; no response grant remains.
        assert_eq!(registry.unmeasured(None, None), 40.0);
        let mut segment = viewer(&auth, policy.clone(), "a", true).await;
        segment.set_output_credit(registry.take(&segment, true));
        segment.add_bytes(188);
        drop(segment);
        assert!(auth.revoke(&identity));
        assert_eq!(registry.unmeasured(None, None), 0.0);
        registry.insert(
            "b".into(),
            viewer(&auth, policy.clone(), "b", false).await,
            40.0,
        );
        let mut b = viewer(&auth, policy, "b", true).await;
        let identity = b.session_identity().unwrap().to_owned();
        b.set_output_credit(registry.take(&b, true));
        drop(b);
        registry
            .0
            .lock()
            .unwrap()
            .segmented
            .get_mut(&identity)
            .unwrap()
            .expires = Instant::now() - Duration::from_secs(1);
        registry.prune();
        assert_eq!(registry.unmeasured(None, None), 0.0);
    }
    #[tokio::test]
    async fn live_signal_credit_remains_discoverable_after_segment_grace_expires() {
        let auth = PlaybackAuth::new(10);
        let policy = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let registry = Registry::default();
        registry.insert(
            "a".into(),
            viewer(&auth, policy.clone(), "a", false).await,
            40.0,
        );
        let mut signal = viewer(&auth, policy.clone(), "a", true).await;
        let identity = signal.session_identity().unwrap().to_owned();
        let original = registry.take(&signal, true).unwrap();
        signal.set_output_credit(Some(original.clone()));
        registry.insert(
            "abandoned-duplicate".into(),
            viewer(&auth, policy.clone(), "a", false).await,
            40.0,
        );
        registry
            .0
            .lock()
            .unwrap()
            .handoffs
            .get_mut("abandoned-duplicate")
            .unwrap()
            .expires = Instant::now() - Duration::from_secs(1);
        registry.prune();
        registry
            .0
            .lock()
            .unwrap()
            .segmented
            .get_mut(&identity)
            .unwrap()
            .expires = Instant::now() - Duration::from_secs(1);
        registry.prune();
        let segment = viewer(&auth, policy, "a", true).await;
        let rediscovered = registry
            .take(&segment, true)
            .expect("live signal's credit was forgotten during segment pause");
        assert!(Arc::ptr_eq(&original, &rediscovered));
    }
    #[tokio::test]
    async fn either_of_two_pending_handoffs_can_own_the_live_signal_credit() {
        let auth = PlaybackAuth::new(10);
        let policy = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let registry = Registry::default();
        for ticket in ["a", "b"] {
            registry.insert(
                ticket.into(),
                viewer(&auth, policy.clone(), "same", false).await,
                40.0,
            );
        }
        // Keep A's unused control credit alive while clean playback selects B.
        let abandoned = registry.0.lock().unwrap().handoffs.remove("a").unwrap();
        let mut signal = viewer(&auth, policy.clone(), "same", true).await;
        let identity = signal.session_identity().unwrap().to_owned();
        let selected = registry.take(&signal, true).unwrap();
        signal.set_output_credit(Some(selected.clone()));
        drop(abandoned);
        registry.prune();
        registry
            .0
            .lock()
            .unwrap()
            .segmented
            .get_mut(&identity)
            .unwrap()
            .expires = Instant::now() - Duration::from_secs(1);
        registry.prune();
        let segment = viewer(&auth, policy, "same", true).await;
        assert!(Arc::ptr_eq(
            &selected,
            &registry
                .take(&segment, true)
                .expect("selected live credit lost after other pending ticket expired")
        ));
    }
    #[tokio::test]
    async fn same_session_segmented_media_represents_all_activated_outputs_but_not_pending_tickets()
    {
        let auth = PlaybackAuth::new(10);
        let policy = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let registry = Registry::default();
        let from = Instant::now();
        registry.insert(
            "a".into(),
            viewer(&auth, policy.clone(), "same", false).await,
            40.0,
        );
        let mut a = viewer(&auth, policy.clone(), "same", true).await;
        let identity = a.session_identity().unwrap().to_owned();
        a.set_output_credit(registry.take(&a, true));
        registry.insert(
            "b".into(),
            viewer(&auth, policy.clone(), "same", false).await,
            40.0,
        );
        a.add_bytes(5_000_000);
        let to = from + Duration::from_secs(1);
        registry.sample(Some(from), to);
        assert_eq!(
            registry.unmeasured(Some((from, to)), Some(40.0)),
            40.0,
            "already measured output must not discount an unstarted ticket"
        );
        registry
            .0
            .lock()
            .unwrap()
            .segmented
            .get_mut(&identity)
            .unwrap()
            .expires = Instant::now() - Duration::from_secs(1);
        registry.prune();
        let mut b = viewer(&auth, policy, "same", true).await;
        b.set_output_credit(registry.take(&b, true));
        b.add_bytes(10_000_000); // Both clients' finite media is attributed to the shared active credit.
        let end = to + Duration::from_secs(1);
        registry.sample(Some(to), end);
        assert_eq!(
            registry.unmeasured(Some((to, end)), Some(80.0)),
            0.0,
            "same-session finite output was charged twice"
        );
    }
}
