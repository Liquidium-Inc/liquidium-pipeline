//! Calls for collateral that the bot holds as supply shares: pool liquidity, the position, the withdraw, and the ledger check for the payout.
use std::sync::Arc;

use async_trait::async_trait;
use candid::{Decode, Encode, Nat, Principal};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_connectors::backend::ledger_history_backend::{LedgerHistoryBackend, LedgerTransfer};
use liquidium_pipeline_connectors::pipeline_agent::{PipelineAgent, UpdateFailure};
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::types::protocol_types::{
    IcpCallerAuth, LendingOutflow, LendingPositionView, LendingSignature, LendingWithdrawRequest, ProtocolError,
    SignedLendingWithdraw, WithdrawDestination,
};

use crate::liquidation::ledger_party_utils::{has_subaccount, is_default_account, is_owned_by};
use crate::utils::now_ts;

/// How many of the newest transactions of the destination one arrival check reads.
const ARRIVAL_SCAN_DEPTH: u64 = 100;
/// The canister accepts an expiry of at most 300 s ahead.
const WITHDRAW_EXPIRY_SECS: i64 = 60;
/// The canister truncates the memo of a payout to this many bytes.
const PAYOUT_MEMO_BYTES: usize = 32;

/// A withdraw that the canister accepted. The tokens arrive later, in a transfer that the pool sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithdrawAccepted {
    pub outflow_id: String,
    /// The amount that left the position. The destination receives this amount minus the ledger fees of the pool.
    pub amount: Nat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WithdrawFailure {
    /// The canister rejected the call. The position did not change, for example when the pool has no liquidity.
    NotExecuted(String),
    /// The canister answered with an error before it changed the position.
    Refused(ProtocolError),
    /// The call possibly ran. The bot reads the position before a new withdraw.
    Ambiguous(String),
}

/// What an arrival check looks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayoutQuery {
    pub token: ChainToken,
    pub pool: Principal,
    pub destination: Principal,
    pub outflow_id: String,
    /// The newest transaction of the destination before the withdraw. Only later transactions count for a two-step payout.
    pub after_index: u64,
    /// The smallest amount that counts as this payout. A smaller transfer belongs to another withdraw.
    pub min_amount: Nat,
    /// The largest amount that counts as this payout: the amount that left the position.
    pub max_amount: Nat,
}

/// A supply position: its value now, and its share count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharePosition {
    pub native: Nat,
    /// Only a deposit, a share payout or a withdraw changes this number. Interest does not change it.
    pub shares: Nat,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SharePositions: Send + Sync {
    /// Returns the tokens that the pool can pay now.
    async fn available_liquidity(&self, pool: Principal) -> Result<Nat, String>;
    /// Returns the supply position, or zeros when there is none.
    async fn position(&self, profile: Principal, pool: Principal) -> Result<SharePosition, String>;
    /// Withdraws `amount` to `destination`. The canister pays at most the whole position.
    async fn withdraw(
        &self,
        profile: Principal,
        pool: Principal,
        amount: Nat,
        destination: Principal,
    ) -> Result<WithdrawAccepted, WithdrawFailure>;
    /// Returns the index of the newest transaction of `destination` on the token's ledger, or 0 when it has none.
    async fn newest_index(&self, token: ChainToken, destination: Principal) -> Result<u64, String>;
    /// Returns the amount that the payout put into the destination, or `None` while it has not arrived.
    async fn arrived(&self, query: PayoutQuery) -> Result<Option<Nat>, String>;
}

pub struct LendingSharePositions<A: PipelineAgent> {
    agent: Arc<A>,
    history: Arc<dyn LedgerHistoryBackend>,
    lending_canister: Principal,
    /// The principal that signs the calls. The canister requires it as the signer of a withdraw.
    caller: Principal,
}

impl<A: PipelineAgent> LendingSharePositions<A> {
    pub fn new(
        agent: Arc<A>,
        history: Arc<dyn LedgerHistoryBackend>,
        lending_canister: Principal,
        caller: Principal,
    ) -> Self {
        Self {
            agent,
            history,
            lending_canister,
            caller,
        }
    }
}

fn ledger_of(token: &ChainToken) -> Result<Principal, String> {
    match token {
        ChainToken::Icp { ledger, .. } => Ok(*ledger),
        other => Err(format!("{} is not on an ICP ledger", other.symbol())),
    }
}

/// Returns the memo that the pool writes on the payout of `outflow_id`.
fn payout_memo(outflow_id: &str) -> Vec<u8> {
    outflow_id.as_bytes().iter().take(PAYOUT_MEMO_BYTES).copied().collect()
}

/// Returns the amount of a transfer when it is the payout that `query` looks for.
/// The payout has the withdraw id as memo, or it comes from a pool subaccount after the withdraw.
fn payout_amount(query: &PayoutQuery, transfer: &LedgerTransfer) -> Option<Nat> {
    if !is_default_account(&transfer.to, query.destination) || !is_owned_by(&transfer.from, query.pool) {
        return None;
    }
    let memo = payout_memo(&query.outflow_id);
    let direct = transfer.memo.as_deref() == Some(memo.as_slice());
    let two_step = has_subaccount(&transfer.from)
        && transfer.index > query.after_index
        && transfer.amount >= query.min_amount
        && transfer.amount <= query.max_amount;
    (direct || two_step).then(|| transfer.amount.clone())
}

#[async_trait]
impl<A: PipelineAgent> SharePositions for LendingSharePositions<A> {
    async fn available_liquidity(&self, pool: Principal) -> Result<Nat, String> {
        let arg = Encode!(&pool).map_err(|error| error.to_string())?;
        let liquidity: Option<Nat> = self
            .agent
            .call_query(&self.lending_canister, "available_liquidity", arg)
            .await?;
        liquidity.ok_or_else(|| format!("the lending canister does not know pool {pool}"))
    }

    async fn position(&self, profile: Principal, pool: Principal) -> Result<SharePosition, String> {
        let arg = Encode!(&profile, &pool).map_err(|error| error.to_string())?;
        let position: Option<LendingPositionView> = self
            .agent
            .call_query(&self.lending_canister, "get_position", arg)
            .await?;
        Ok(match position {
            Some(view) => SharePosition {
                native: view.deposited_native_now,
                shares: view.deposit_scaled,
            },
            None => SharePosition {
                native: Nat::from(0u8),
                shares: Nat::from(0u8),
            },
        })
    }

    async fn withdraw(
        &self,
        profile: Principal,
        pool: Principal,
        amount: Nat,
        destination: Principal,
    ) -> Result<WithdrawAccepted, WithdrawFailure> {
        let request = SignedLendingWithdraw {
            data: LendingWithdrawRequest {
                expiry_timestamp: (now_ts() + WITHDRAW_EXPIRY_SECS) as u64,
                account: WithdrawDestination::Native(destination),
                pool_id: pool,
                amount,
            },
            signature_info: LendingSignature::IcpCaller(IcpCallerAuth {
                principal: self.caller,
                subaccount: None,
            }),
        };
        // The bot sends nothing when it cannot encode the request.
        let arg = Encode!(&profile, &request).map_err(|error| WithdrawFailure::NotExecuted(error.to_string()))?;
        let reply = self
            .agent
            .call_update_classified(&self.lending_canister, "withdraw", arg)
            .await
            .map_err(|failure| match failure {
                UpdateFailure::Rejected(message) => WithdrawFailure::NotExecuted(message),
                UpdateFailure::Ambiguous(message) => WithdrawFailure::Ambiguous(message),
            })?;
        // The call ran. A reply that the bot cannot read does not tell if the canister burned shares.
        match Decode!(&reply, Result<LendingOutflow, ProtocolError>) {
            Ok(Ok(outflow)) => Ok(WithdrawAccepted {
                outflow_id: outflow.id,
                amount: outflow.amount,
            }),
            Ok(Err(error)) => Err(WithdrawFailure::Refused(error)),
            Err(error) => Err(WithdrawFailure::Ambiguous(format!(
                "withdraw reply not readable: {error}"
            ))),
        }
    }

    async fn newest_index(&self, token: ChainToken, destination: Principal) -> Result<u64, String> {
        self.history
            .newest_index(ledger_of(&token)?, default_account(destination))
            .await
    }

    async fn arrived(&self, query: PayoutQuery) -> Result<Option<Nat>, String> {
        let transfers = self
            .history
            .recent_transfers(
                ledger_of(&query.token)?,
                default_account(query.destination),
                ARRIVAL_SCAN_DEPTH,
            )
            .await?;
        Ok(transfers.iter().find_map(|transfer| payout_amount(&query, transfer)))
    }
}

fn default_account(owner: Principal) -> Account {
    Account {
        owner,
        subaccount: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liquidium_pipeline_connectors::backend::ledger_history_backend::{LedgerParty, MockLedgerHistoryBackend};
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;

    use crate::liquidation::ledger_party_utils::default_account_id_hex;

    fn p(text: &str) -> Principal {
        Principal::from_text(text).unwrap()
    }

    fn token() -> ChainToken {
        ChainToken::Icp {
            ledger: p("mxzaz-hqaaa-aaaar-qaada-cai"),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(10u8),
        }
    }

    fn query() -> PayoutQuery {
        PayoutQuery {
            token: token(),
            pool: p("42svn-2yaaa-aaaae-qfcsq-cai"),
            destination: p("q4iil-7u3vj-sta62-rv37o-mquq5-bdnbh-lyw22-laizm-tirnp-2p7yo-dae"),
            outflow_id: "withdraw_7".to_string(),
            after_index: 500,
            min_amount: Nat::from(9_970u64),
            max_amount: Nat::from(10_000u64),
        }
    }

    fn transfer(index: u64, from_subaccount: Option<[u8; 32]>, memo: Option<&str>, amount: u64) -> LedgerTransfer {
        let query = query();
        LedgerTransfer {
            index,
            from: LedgerParty::Account(Account {
                owner: query.pool,
                subaccount: from_subaccount,
            }),
            to: LedgerParty::Account(default_account(query.destination)),
            amount: Nat::from(amount),
            memo: memo.map(|text| text.as_bytes().to_vec()),
            timestamp_nanos: None,
        }
    }

    #[test]
    fn a_direct_payout_is_found_by_its_memo() {
        let found = payout_amount(&query(), &transfer(400, None, Some("withdraw_7"), 9_990));
        assert_eq!(found, Some(Nat::from(9_990u64)));
    }

    #[test]
    fn a_collateral_payout_of_a_normal_liquidation_is_not_the_withdraw() {
        // The pool and the destination are the same. Only the memo is different.
        assert_eq!(
            payout_amount(&query(), &transfer(600, None, Some("liq_55"), 9_990)),
            None
        );
    }

    #[test]
    fn a_two_step_payout_is_found_after_the_withdraw() {
        let found = payout_amount(&query(), &transfer(501, Some([7; 32]), None, 9_980));
        assert_eq!(found, Some(Nat::from(9_980u64)));
    }

    #[test]
    fn a_two_step_payout_from_before_the_withdraw_does_not_count() {
        assert_eq!(
            payout_amount(&query(), &transfer(500, Some([7; 32]), None, 9_980)),
            None
        );
    }

    #[test]
    fn a_small_transfer_from_another_withdraw_does_not_count() {
        assert_eq!(payout_amount(&query(), &transfer(501, Some([7; 32]), None, 12)), None);
    }

    /// The bot does not take a late payout of a larger, earlier withdraw for this one.
    #[test]
    fn a_two_step_transfer_above_the_withdrawn_amount_does_not_count() {
        assert_eq!(
            payout_amount(&query(), &transfer(501, Some([7; 32]), None, 99_980)),
            None
        );
    }

    #[test]
    fn a_transfer_from_another_sender_does_not_count() {
        let mut other = transfer(600, None, Some("withdraw_7"), 9_990);
        other.from = LedgerParty::Account(default_account(Principal::anonymous()));
        assert_eq!(payout_amount(&query(), &other), None);
    }

    /// The ICP index gives account identifiers. The bot finds the direct payout of the ICP pool by its memo.
    #[test]
    fn a_direct_payout_on_the_icp_ledger_is_found_by_its_memo() {
        let query = query();
        let mut icp = transfer(600, None, Some("withdraw_7"), 9_990);
        icp.from = LedgerParty::AccountId(default_account_id_hex(query.pool));
        icp.to = LedgerParty::AccountId(default_account_id_hex(query.destination));
        assert_eq!(payout_amount(&query, &icp), Some(Nat::from(9_990u64)));
        icp.to = LedgerParty::AccountId(default_account_id_hex(Principal::anonymous()));
        assert_eq!(payout_amount(&query, &icp), None);
    }

    fn positions(agent: MockPipelineAgent) -> LendingSharePositions<MockPipelineAgent> {
        LendingSharePositions::new(
            Arc::new(agent),
            Arc::new(MockLedgerHistoryBackend::new()),
            p("nja4y-2yaaa-aaaae-qddxa-cai"),
            Principal::anonymous(),
        )
    }

    #[tokio::test]
    async fn a_rejected_withdraw_is_known_not_to_have_run() {
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update_classified()
            .returning(|_, _, _| Err(UpdateFailure::Rejected("canister trapped".to_string())));
        let failure = positions(agent)
            .withdraw(
                Principal::anonymous(),
                query().pool,
                Nat::from(1u8),
                query().destination,
            )
            .await
            .unwrap_err();
        assert!(matches!(failure, WithdrawFailure::NotExecuted(_)));
    }

    #[tokio::test]
    async fn a_transport_error_leaves_the_withdraw_ambiguous() {
        let mut agent = MockPipelineAgent::new();
        agent
            .expect_call_update_classified()
            .returning(|_, _, _| Err(UpdateFailure::Ambiguous("timeout".to_string())));
        let failure = positions(agent)
            .withdraw(
                Principal::anonymous(),
                query().pool,
                Nat::from(1u8),
                query().destination,
            )
            .await
            .unwrap_err();
        assert!(matches!(failure, WithdrawFailure::Ambiguous(_)));
    }

    #[tokio::test]
    async fn the_withdraw_request_names_the_caller_as_signer_and_the_destination() {
        let mut agent = MockPipelineAgent::new();
        agent.expect_call_update_classified().returning(|_, method, arg| {
            assert_eq!(method, "withdraw");
            let (profile, request) = Decode!(&arg, Principal, SignedLendingWithdraw).unwrap();
            assert_eq!(profile, Principal::management_canister());
            assert_eq!(request.data.account, WithdrawDestination::Native(query().destination));
            assert_eq!(request.data.amount, Nat::from(0u8));
            assert_eq!(
                request.signature_info,
                LendingSignature::IcpCaller(IcpCallerAuth {
                    principal: Principal::anonymous(),
                    subaccount: None
                })
            );
            let reply: Result<LendingOutflow, ProtocolError> = Ok(LendingOutflow {
                id: "withdraw_7".to_string(),
                amount: Nat::from(10_000u64),
            });
            Ok(Encode!(&reply).unwrap())
        });
        let accepted = positions(agent)
            .withdraw(
                Principal::management_canister(),
                query().pool,
                Nat::from(0u8),
                query().destination,
            )
            .await
            .unwrap();
        assert_eq!(accepted.outflow_id, "withdraw_7");
        assert_eq!(accepted.amount, Nat::from(10_000u64));
    }

    #[tokio::test]
    async fn a_typed_error_is_a_refusal() {
        let mut agent = MockPipelineAgent::new();
        agent.expect_call_update_classified().returning(|_, _, _| {
            let reply: Result<LendingOutflow, ProtocolError> = Err(ProtocolError::PoolFrozen);
            Ok(Encode!(&reply).unwrap())
        });
        let failure = positions(agent)
            .withdraw(
                Principal::anonymous(),
                query().pool,
                Nat::from(1u8),
                query().destination,
            )
            .await
            .unwrap_err();
        assert_eq!(failure, WithdrawFailure::Refused(ProtocolError::PoolFrozen));
    }
}
