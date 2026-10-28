use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{debug, info};

use super::multi_venue_finalizer::{apply_progress, derive_outcome, set_meta_v2};
use crate::{
    finalizers::{
        finalizer::FinalizerError,
        multi_venue::{VenueLegCheckpoint, VenueLegProgress},
    },
    persistance::{LiqMetaWrapper, LiqResultRecord, MultiVenueExecutionState, WalStore},
    utils::now_ts,
    wal::encode_meta,
};

/// Durable snapshot owned by one invocation of a venue leg.
///
/// The complete envelope is retained here because every checkpoint must write
/// the selected leg together with all sibling legs and the parent execution
/// outcome. This prevents a venue-local transition from erasing concurrent or
/// previously committed state belonging to another venue.
pub(super) struct ParentLegCheckpointState {
    pub(super) row: LiqResultRecord,
    pub(super) wrapper: LiqMetaWrapper,
    pub(super) execution: MultiVenueExecutionState,
    /// A checkpoint that can never succeed. The adapter only passes text back,
    /// so the parent reads the decision here.
    pub(super) permanent_failure: Option<FinalizerError>,
}

/// Invocation-scoped persistence capability for exactly one committed leg.
///
/// A venue receives this object while advancing but never receives the parent
/// WAL row directly. Calling `checkpoint` replaces only `leg_index`, derives
/// the parent outcome, validates the complete `meta_v2` envelope, and commits
/// it atomically before the venue submits its next external side effect.
///
/// The checkpoint is deliberately created per finalizer invocation rather
/// than stored on an adapter: its WAL row and leg index are request-specific,
/// and keeping them invocation-scoped makes shared adapters concurrency-safe.
pub(super) struct ParentLegCheckpoint<'a> {
    wal: &'a dyn WalStore,
    leg_index: usize,
    inner: Mutex<ParentLegCheckpointState>,
}

impl<'a> ParentLegCheckpoint<'a> {
    pub(super) fn new(
        wal: &'a dyn WalStore,
        leg_index: usize,
        row: &LiqResultRecord,
        wrapper: &LiqMetaWrapper,
        execution: &MultiVenueExecutionState,
    ) -> Self {
        Self {
            wal,
            leg_index,
            inner: Mutex::new(ParentLegCheckpointState {
                row: row.clone(),
                wrapper: wrapper.clone(),
                execution: execution.clone(),
                permanent_failure: None,
            }),
        }
    }

    /// Returns the most recently committed state so the parent can continue
    /// from venue checkpoints even when the adapter returns an operational
    /// error after one or more successful transitions.
    pub(super) fn into_state(self) -> ParentLegCheckpointState {
        self.inner.into_inner()
    }
}

#[async_trait]
impl VenueLegCheckpoint for ParentLegCheckpoint<'_> {
    async fn checkpoint(&self, progress: VenueLegProgress) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        let previous = inner.execution.legs[self.leg_index].clone();
        let applied = apply_progress(&mut inner.execution.legs[self.leg_index], progress)
            .map_err(FinalizerError::Permanent)
            .and_then(|()| {
                inner.execution.outcome = derive_outcome(&inner.execution.legs);
                let execution = inner.execution.clone();
                set_meta_v2(&mut inner.wrapper, execution)
            });
        if let Err(failure) = applied {
            let message = failure.to_string();
            inner.permanent_failure = Some(failure);
            return Err(message);
        }
        inner.row.updated_at = now_ts();
        let wrapper = inner.wrapper.clone();
        encode_meta(&mut inner.row, &wrapper)?;
        self.wal
            .upsert_result(inner.row.clone())
            .await
            .map_err(|error| format!("failed to checkpoint venue leg {}: {error}", previous.leg_id))?;
        let current = &inner.execution.legs[self.leg_index];
        if previous.status != current.status {
            info!(
                event = "multi_venue_leg_checkpoint",
                liquidation_id = %inner.row.id,
                strategy_id = %inner.execution.plan.strategy_id,
                leg_id = %current.leg_id,
                venue_id = %current.venue_id,
                previous_status = ?previous.status,
                status = ?current.status,
                "Checkpointed venue status transition"
            );
        } else {
            debug!(
                event = "multi_venue_leg_checkpoint",
                liquidation_id = %inner.row.id,
                strategy_id = %inner.execution.plan.strategy_id,
                leg_id = %current.leg_id,
                venue_id = %current.venue_id,
                status = ?current.status,
                "Checkpointed venue-local state"
            );
        }
        Ok(())
    }
}
