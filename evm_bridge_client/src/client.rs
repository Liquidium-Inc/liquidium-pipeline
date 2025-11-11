// Core EVM client with transaction management

use crate::{config::*, errors::EvmError, types::*};
use alloy::{
    network::EthereumWallet,
    providers::{DynProvider, ProviderBuilder},
};

#[derive(Clone)]
pub struct EvmClient {
    pub provider: DynProvider,
    pub from: Address,
    pub chain_id: u64,
    pub policy: TxPolicyConfig,
    pub wallet: EthereumWallet,
}

impl EvmClient {
    pub async fn new(
        rpc: RpcConfig,
        signer: PrivateKeySigner,
        from: Address,
        policy: TxPolicyConfig,
    ) -> Result<Self, EvmError> {
        let wallet = EthereumWallet::from(signer);
        let provider = ProviderBuilder::new()
            .wallet(wallet.clone())
            .connect_http(rpc.rpc_url.parse().map_err(|e| EvmError::Other(format!("{}", e)))?);

        Ok(Self {
            provider: provider.erased(),
            from,
            chain_id: rpc.chain_id,
            policy,
            wallet,
        })
    }
}
