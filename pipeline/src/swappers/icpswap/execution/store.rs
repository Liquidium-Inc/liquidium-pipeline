use async_trait::async_trait;

use crate::{
    persistance::{VenueExecutionState, WalStore},
    wal::{decode_receipt_wrapper, encode_meta},
};

use super::types::IcpswapExecutionState;
use crate::swappers::icpswap::VENUE_ID;

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait IcpswapExecutionStateStore: Send + Sync {
    async fn load(&self, liquidation_id: &str) -> Result<Option<IcpswapExecutionState>, String>;
    async fn persist(&self, liquidation_id: &str, state: &IcpswapExecutionState) -> Result<(), String>;
}

pub struct WalIcpswapExecutionStateStore<'a> {
    wal: &'a dyn WalStore,
}

impl<'a> WalIcpswapExecutionStateStore<'a> {
    pub fn new(wal: &'a dyn WalStore) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl IcpswapExecutionStateStore for WalIcpswapExecutionStateStore<'_> {
    async fn load(&self, liquidation_id: &str) -> Result<Option<IcpswapExecutionState>, String> {
        let Some(row) = self
            .wal
            .get_result(liquidation_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Err(format!("missing WAL row for liquidation {liquidation_id}"));
        };
        let wrapper = decode_receipt_wrapper(&row)?;
        wrapper
            .and_then(|wrapper| wrapper.venue_execution)
            .map(|record| record.decode(VENUE_ID))
            .transpose()
            .map(Option::flatten)
    }

    async fn persist(&self, liquidation_id: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        let mut row = self
            .wal
            .get_result(liquidation_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("missing WAL row for liquidation {liquidation_id}"))?;
        let mut wrapper = decode_receipt_wrapper(&row)?
            .ok_or_else(|| format!("missing receipt metadata for liquidation {liquidation_id}"))?;

        if state.execution_id != liquidation_id {
            return Err(format!(
                "ICPSwap execution ID {} does not match key {liquidation_id}",
                state.execution_id
            ));
        }

        if let Some(record) = &wrapper.venue_execution {
            let existing = record
                .decode::<IcpswapExecutionState>(VENUE_ID)?
                .ok_or_else(|| format!("liquidation {liquidation_id} belongs to venue {}", record.venue))?;
            if existing.plan != state.plan {
                return Err(format!(
                    "refusing to replace persisted ICPSwap plan for liquidation {liquidation_id}"
                ));
            }
            // Waiting states re-persist identical bytes on every poll, and the
            // store runs `synchronous=FULL` with `fullfsync=ON`, so each of those
            // is a real disk sync. The decode above already gives us the
            // comparison for free.
            if existing == *state {
                return Ok(());
            }
        }

        wrapper.venue_execution = Some(VenueExecutionState::new(VENUE_ID, state)?);
        encode_meta(&mut row, &wrapper)?;
        self.wal.upsert_result(row).await.map_err(|error| error.to_string())
    }
}
