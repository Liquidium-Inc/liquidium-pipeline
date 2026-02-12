use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use icrc_ledger_types::icrc1::account::Account;
use icrc_ledger_types::icrc2::approve::ApproveArgs;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::error::TransferError;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::TransferActions;
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

#[async_trait]
impl<B: IcpBackend + Send + Sync> TransferActions for IcpTransferAdapter<B> {
    async fn transfer(
        &self,
        token: &ChainToken,
        to: &ChainAccount,
        amount_native: Nat,
    ) -> Result<String, TransferError> {
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
                    .icp_transfer(*ledger, to_account_id_hex, amount_native)
                    .await
                    .map_err(TransferError::backend)?;

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
                let block_index = self
                    .backend
                    .icrc1_transfer(*ledger, &self.account, to_account, amount)
                    .await
                    .map_err(TransferError::backend)?;

                Ok(block_index.to_string())
            }

            (ChainToken::Icp { .. }, _) => Err(TransferError::UnsupportedDestination {
                details: "destination chain must be ICP".to_string(),
            }),
            _ => Err(TransferError::UnsupportedToken { token: token.clone() }),
        }
    }

    async fn approve(
        &self,
        token: &ChainToken,
        spender: &ChainAccount,
        amount_native: Nat,
    ) -> Result<String, TransferError> {
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

                let block_index = self
                    .backend
                    .icrc2_approve(*ledger, args)
                    .await
                    .map_err(TransferError::backend)?;
                Ok(block_index.to_string())
            }
            (ChainToken::Icp { .. }, _) => Err(TransferError::UnsupportedDestination {
                details: "spender must be an ICP account".to_string(),
            }),
            _ => Err(TransferError::UnsupportedToken { token: token.clone() }),
        }
    }
}
