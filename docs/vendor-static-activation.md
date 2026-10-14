# Vendor static input ownership

This library building block prepares always-on vendor streaming. Automatic
vendor catalog polling and daemon startup scheduling are still pending; adding
a source does not start new vendor pulls without viewer demand yet.

A current, enabled vendor static stream selected by the source's activation
settings can receive an `ActivatedCandidate` through the modern source
coordinator, or through its selected replica. Activation uses the configured
private origin and Cluster credentials. It creates no viewer session, does not
call viewer authentication, and does not consume viewer limits. Every network
viewer still needs its own normal authorization.

`SharedInputs::attach_activation` and `Engine::package_shared` let an embedding
owner keep one shared input and packager active. Independently authorized
viewers join the same generation when source, processing and replica selection
match. Direct and replica-group attachments cannot mix. Static ownership counts
as zero viewers and its reads do not renew playback activity. Current static
demand protects the worker from idle retirement. Cancelling its `ActivationControl`
stops that owner's demand; an authorized viewer can keep the shared worker
alive. Revoking a viewer does not cancel static ownership.

Static ownership follows the current source configuration, identity, lease and
selected replica. Safe renewal preserves an equivalent generation. Expiry,
removal, disable, demotion to on-demand, policy changes or replica replacement
retire stale media. Local configured streams retain precedence, including
disabled ones. The library starts no detached retry or maintenance tasks;
embeddings own renewal, packaging and shutdown.

Owned HTTP tests exercise M4S/M4F pulls, independent HLS decoding, sharing,
cancellation, callbacks, viewer limits, expiry, renewal and replica changes.
These tests do not establish production vendor interoperability or capacity.
