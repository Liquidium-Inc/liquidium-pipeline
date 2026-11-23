//! The state of collateral that a liquidation paid as supply shares, until the bot withdraws the tokens.
use candid::{Nat, Principal};
use serde::{Deserialize, Serialize};

/// A withdraw that is not finished.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OpenWithdraw {
    /// The bot saves this before the call. If it stays after the call, the bot does not know the outcome.
    Submitting {
        /// The amount that the call asks for.
        amount: Nat,
        /// The share count of the position before the call. The same count later proves that the call did not run.
        shares_before: Nat,
        after_index: u64,
        at: i64,
    },
    /// The canister accepted the withdraw. The payout comes later.
    Accepted {
        outflow_id: String,
        amount: Nat,
        /// The newest ledger transaction of the destination before the call.
        after_index: u64,
        at: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShareHolding {
    /// The lending profile that holds the shares.
    pub profile: Principal,
    pub pool: Principal,
    /// The value of the shares in native units when the liquidation paid them.
    pub seized: Nat,
    /// The amount that accepted withdraws took out of the position.
    pub withdrawn: Nat,
    /// The amount that arrived at the destination, after the ledger fees of the pool.
    pub arrived: Nat,
    #[serde(default)]
    pub open: Option<OpenWithdraw>,
    pub held_since: i64,
    #[serde(default)]
    pub released_at: Option<i64>,
    /// The bot tries no withdraw before this time.
    #[serde(default)]
    pub next_try_at: i64,
    #[serde(default)]
    pub last_note: Option<String>,
    /// The amount that an operator declared absent from the position. The row does not wait for it.
    #[serde(default)]
    pub written_off: Nat,
}

impl ShareHolding {
    pub fn new(profile: Principal, pool: Principal, seized: Nat, now: i64) -> Self {
        Self {
            profile,
            pool,
            seized,
            withdrawn: Nat::from(0u8),
            arrived: Nat::from(0u8),
            open: None,
            held_since: now,
            released_at: None,
            next_try_at: 0,
            last_note: None,
            written_off: Nat::from(0u8),
        }
    }

    /// Returns the amount that the row still has in the position.
    pub fn remaining(&self) -> Nat {
        let gone = self.withdrawn.clone() + self.written_off.clone();
        if self.seized > gone {
            self.seized.clone() - gone
        } else {
            Nat::from(0u8)
        }
    }

    /// Returns the supply interest: the amount that the withdraws took above the seized amount.
    pub fn interest(&self) -> Nat {
        if self.withdrawn > self.seized {
            self.withdrawn.clone() - self.seized.clone()
        } else {
            Nat::from(0u8)
        }
    }

    /// Returns the seconds between the liquidation and the release to the swap, or until `now` while it is held.
    pub fn held_secs(&self, now: i64) -> i64 {
        (self.released_at.unwrap_or(now) - self.held_since).max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_and_interest_follow_the_withdrawn_amount() {
        let mut holding = ShareHolding::new(Principal::anonymous(), Principal::anonymous(), Nat::from(1_000u64), 10);
        assert_eq!(holding.remaining(), Nat::from(1_000u64));
        holding.withdrawn = Nat::from(400u64);
        assert_eq!(holding.remaining(), Nat::from(600u64));
        assert_eq!(holding.interest(), Nat::from(0u8));
        // The last withdraw takes the whole position, which includes the interest.
        holding.withdrawn = Nat::from(1_007u64);
        assert_eq!(holding.remaining(), Nat::from(0u8));
        assert_eq!(holding.interest(), Nat::from(7u64));
    }

    #[test]
    fn a_written_off_amount_is_not_remaining() {
        let mut holding = ShareHolding::new(Principal::anonymous(), Principal::anonymous(), Nat::from(1_000u64), 10);
        holding.withdrawn = Nat::from(400u64);
        holding.written_off = Nat::from(600u64);
        assert_eq!(holding.remaining(), Nat::from(0u8));
        assert_eq!(holding.interest(), Nat::from(0u8));
    }

    #[test]
    fn a_holding_saved_without_the_later_fields_still_reads() {
        let holding = ShareHolding::new(Principal::anonymous(), Principal::anonymous(), Nat::from(5u8), 3);
        let mut json = serde_json::to_value(&holding).unwrap();
        for field in ["open", "released_at", "next_try_at", "last_note", "written_off"] {
            json.as_object_mut().unwrap().remove(field);
        }
        assert_eq!(serde_json::from_value::<ShareHolding>(json).unwrap(), holding);
    }

    /// A row from before the share payout has no request flag and no result field.
    #[test]
    fn a_request_and_a_result_saved_before_the_share_payout_still_read() {
        use liquidium_pipeline_core::types::protocol_types::{
            AssetType, LiquidationAmounts, LiquidationRequest, LiquidationResult, LiquidationStatus, TransferStatus,
            TxStatus,
        };
        let request = LiquidationRequest {
            borrower: Principal::anonymous(),
            debt_pool_id: Principal::anonymous(),
            collateral_pool_id: Principal::anonymous(),
            debt_amount: Nat::from(7u8),
            receiver_address: Principal::anonymous(),
            buy_bad_debt: false,
            collateral_share_receiver: Some(Principal::anonymous()),
        };
        let mut json = serde_json::to_value(&request).unwrap();
        json.as_object_mut().unwrap().remove("collateral_share_receiver");
        let read: LiquidationRequest = serde_json::from_value(json).unwrap();
        assert_eq!(read.collateral_share_receiver, None);

        let tx = || TxStatus {
            tx_id: None,
            status: TransferStatus::Success,
        };
        let result = LiquidationResult {
            amounts: LiquidationAmounts {
                collateral_received: Nat::from(5u8),
                debt_repaid: Nat::from(4u8),
            },
            collateral_asset: AssetType::Unknown,
            debt_asset: AssetType::Unknown,
            status: LiquidationStatus::Success,
            timestamp: 1,
            change_tx: tx(),
            collateral_tx: tx(),
            id: 9,
            collateral_share_receiver: Some(Principal::anonymous()),
        };
        let mut json = serde_json::to_value(&result).unwrap();
        json.as_object_mut().unwrap().remove("collateral_share_receiver");
        let read: LiquidationResult = serde_json::from_value(json).unwrap();
        assert_eq!(read.collateral_share_receiver, None);
    }
}
