use liquidium_pipeline_core::{account::model::ChainAccount, tokens::chain_token_amount::ChainTokenAmount};
use serde::{Deserialize, Serialize};

/// Durable progress for collateral that cannot meet any venue's execution
/// minimum. The exact transfer is persisted as `ReadyToSubmit` before the
/// trader account submits it, so a restart never blindly repeats an ambiguous
/// transfer: only the invocation that wrote that state may submit it, and any
/// later process that loads it parks the sweep instead of guessing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoverySweepStatus {
    ReadyToSubmit,
    Completed,
    OperatorRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecoverySweepState {
    pub liquidation_id: String,
    pub reason: String,
    pub amount: ChainTokenAmount,
    pub destination: ChainAccount,
    pub status: RecoverySweepStatus,
    #[serde(default)]
    pub txid: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
}

impl RecoverySweepState {
    pub fn validate(&self) -> Result<(), String> {
        self.liquidation_id
            .parse::<u128>()
            .map_err(|error| format!("invalid recovery liquidation ID `{}`: {error}", self.liquidation_id))?;

        match self.status {
            RecoverySweepStatus::ReadyToSubmit => {
                if self.txid.is_some() {
                    return Err("ready recovery sweep cannot already have a transaction ID".to_string());
                }
            }
            RecoverySweepStatus::Completed => {
                if self.amount.value != 0u8 && self.txid.as_deref().is_none_or(str::is_empty) {
                    return Err("completed non-zero recovery sweep requires a transaction ID".to_string());
                }
            }
            RecoverySweepStatus::OperatorRequired => {
                if self.last_error.as_deref().is_none_or(str::is_empty) {
                    return Err("operator-required recovery sweep must explain the ambiguity".to_string());
                }
            }
        }
        Ok(())
    }
}
