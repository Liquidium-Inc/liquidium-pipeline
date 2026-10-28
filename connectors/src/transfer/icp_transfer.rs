use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use icrc_ledger_types::icrc1::account::Account;
use icrc_ledger_types::icrc1::transfer::TransferArg;
use icrc_ledger_types::icrc2::approve::ApproveArgs;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::{TransferActions, TransferFailure};
use log::info;

use crate::backend::icp_backend::IcpBackend;

pub struct IcpTransferAdapter<B: IcpBackend> {
    backend: Arc<B>,
    account: Account,
}

impl<B: IcpBackend> IcpTransferAdapter<B> {
    pub fn new(backend: Arc<B>, account: Account) -> Self {
        Self { backend, account }
    }
}

impl<B: IcpBackend + Send + Sync> IcpTransferAdapter<B> {
    /// One sender for both trait methods. `Some(created_at)` lets the ledger
    /// answer a repeat of the same arguments with the first block.
    async fn send(
        &self,
        token: &ChainToken,
        to: &ChainAccount,
        amount_native: Nat,
        created_at_time_nanos: Option<u64>,
    ) -> Result<String, TransferFailure> {
        let from_owner = self.account.owner.to_text();
        let from_subaccount = if self.account.subaccount.is_some() {
            "some"
        } else {
            "none"
        };
        let to_desc = match to {
            ChainAccount::IcpLedger(hex) => format!("icp_ledger:{hex}"),
            ChainAccount::Icp(account) => {
                let sub = if account.subaccount.is_some() { "some" } else { "none" };
                format!("icp:{} subaccount={}", account.owner.to_text(), sub)
            }
            other => format!("{:?}", other),
        };

        match (token, to) {
            // Native ICP via ICP ledger: destination is an AccountIdentifier hex string
            (ChainToken::Icp { ledger, .. }, ChainAccount::IcpLedger(to_account_id_hex)) => {
                info!(
                    "[transfer] icp from={} subaccount={} to={} amount={} ledger={}",
                    from_owner,
                    from_subaccount,
                    to_desc,
                    amount_native,
                    ledger.to_text()
                );
                // amount_native is in e8s for ICP
                let block_index = self
                    .backend
                    .icp_transfer(*ledger, to_account_id_hex, amount_native, created_at_time_nanos)
                    .await?;

                Ok(block_index.to_string())
            }

            // ICRC-1 style transfers that still use an ICRC `Account` as destination
            (ChainToken::Icp { ledger, .. }, ChainAccount::Icp(to_account)) => {
                info!(
                    "[transfer] icp from={} subaccount={} to={} amount={} ledger={}",
                    from_owner,
                    from_subaccount,
                    to_desc,
                    amount_native,
                    ledger.to_text()
                );
                let amount = amount_native;
                let block_index = match created_at_time_nanos {
                    None => self.backend.icrc1_transfer(*ledger, &self.account, to_account, amount).await?,
                    Some(created_at) => self
                        .backend
                        .icrc1_transfer_with_args(
                            *ledger,
                            TransferArg {
                                from_subaccount: self.account.subaccount,
                                to: *to_account,
                                amount,
                                fee: None,
                                memo: None,
                                created_at_time: Some(created_at),
                            },
                        )
                        .await
                        .map_err(TransferFailure::from)?,
                };

                Ok(block_index.to_string())
            }

            // Routing refusals never reach a ledger, so nothing moved.
            (ChainToken::Icp { .. }, _) => Err(TransferFailure::Rejected(
                "IcpTransferAdapter: destination chain must be ICP".to_string(),
            )),
            _ => Err(TransferFailure::Rejected(
                "IcpTransferAdapter only supports ChainToken::Icp".to_string(),
            )),
        }
    }
}

#[async_trait]
impl<B: IcpBackend + Send + Sync> TransferActions for IcpTransferAdapter<B> {
    async fn transfer(
        &self,
        token: &ChainToken,
        to: &ChainAccount,
        amount_native: Nat,
    ) -> Result<String, TransferFailure> {
        self.send(token, to, amount_native, None).await
    }

    async fn transfer_deduplicated(
        &self,
        token: &ChainToken,
        to: &ChainAccount,
        amount_native: Nat,
        created_at_time_nanos: u64,
    ) -> Result<String, TransferFailure> {
        self.send(token, to, amount_native, Some(created_at_time_nanos)).await
    }

    async fn approve(&self, token: &ChainToken, spender: &ChainAccount, amount_native: Nat) -> Result<String, String> {
        match (token, spender) {
            (ChainToken::Icp { ledger, .. }, ChainAccount::Icp(spender_account)) => {
                let args = ApproveArgs {
                    from_subaccount: self.account.subaccount,
                    spender: *spender_account,
                    amount: amount_native,
                    expected_allowance: None,
                    expires_at: None,
                    fee: None,
                    memo: None,
                    created_at_time: None,
                };

                let block_index = self.backend.icrc2_approve(*ledger, args).await?;
                Ok(block_index.to_string())
            }
            (ChainToken::Icp { .. }, _) => Err("IcpTransferAdapter: spender must be an ICP account".to_string()),
            _ => Err("IcpTransferAdapter only supports ChainToken::Icp".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::icp_backend::MockIcpBackend;
    use candid::Principal;

    fn token(ledger: Principal) -> ChainToken {
        ChainToken::Icp {
            ledger,
            symbol: "ckUSDC".into(),
            decimals: 6,
            fee: Nat::from(10_000u64),
        }
    }

    #[tokio::test]
    async fn a_deduplicated_transfer_sends_its_created_at_time_on_both_ledger_kinds() {
        let ledger = Principal::from_slice(&[1]);
        let mut backend = MockIcpBackend::new();
        backend
            .expect_icrc1_transfer_with_args()
            .times(1)
            .withf(|_, args| args.created_at_time == Some(42) && args.fee.is_none() && args.memo.is_none())
            .returning(|_, _| Ok(Nat::from(5u64)));
        backend
            .expect_icp_transfer()
            .times(1)
            .withf(|_, _, _, created_at| *created_at == Some(42))
            .returning(|_, _, _, _| Ok(6));
        backend.expect_icrc1_transfer().times(0);
        let adapter = IcpTransferAdapter::new(
            Arc::new(backend),
            Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
        );
        let to = ChainAccount::Icp(Account {
            owner: Principal::from_slice(&[2]),
            subaccount: None,
        });

        assert_eq!(
            adapter.transfer_deduplicated(&token(ledger), &to, Nat::from(100u64), 42).await,
            Ok("5".to_string())
        );
        assert_eq!(
            adapter
                .transfer_deduplicated(&token(ledger), &ChainAccount::IcpLedger("00".into()), Nat::from(100u64), 42)
                .await,
            Ok("6".to_string())
        );
    }
}
