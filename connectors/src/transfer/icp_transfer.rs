use std::sync::Arc;

use async_trait::async_trait;
use candid::Nat;
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_core::account::model::ChainAccount;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use liquidium_pipeline_core::transfer::actions::TransferActions;

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
    ) -> Result<String, String> {
        match (token, to) {
            (
                ChainToken::Icp { ledger, .. },
                ChainAccount::Icp(to_account),
            ) => {
                let amount = Nat::from(amount_native);
                let block_index = self
                    .backend
                    .icrc1_transfer(*ledger, &self.account, to_account, amount)
                    .await?;

                Ok(block_index.to_string())
            }
            (ChainToken::Icp { .. }, _) => {
                Err("IcpTransferAdapter: destination chain must be ICP".to_string())
            }
            _ => Err("IcpTransferAdapter only supports ChainToken::Icp".to_string()),
        }
    }
}
