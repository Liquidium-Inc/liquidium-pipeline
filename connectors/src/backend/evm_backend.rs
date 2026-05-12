use alloy::{
    network::{AnyNetwork, NetworkWallet, TransactionBuilder},
    primitives::{Address, TxHash, U256},
    providers::{Provider, WalletProvider},
    rpc::types::TransactionRequest,
    sol,
};
use async_trait::async_trait;
use candid::Nat;
use tokio::time::{Duration, sleep};

const ERC20_APPROVE_GAS_LIMIT: u64 = 120_000;
const ERC20_APPROVE_GAS_BUFFER_BPS: u64 = 2_000; // +20%
const NONCE_RETRY_MAX_ATTEMPTS: u8 = 3;
const NONCE_RETRY_BASE_DELAY_MS: u64 = 200;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address owner) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function transfer(address to, uint256 amount) external returns (bool);
        function decimals() external view returns (uint8);
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EvmBackend: Send + Sync {
    // ERC20 balance for the pipeline wallet on a given chain.
    async fn erc20_balance(&self, chain: &str, token_address: &str) -> Result<Nat, String>;

    // Native coin balance (ETH, ARB, etc) for the pipeline wallet.
    async fn native_balance(&self, chain: &str) -> Result<Nat, String>;

    // ERC20 transfer from the pipeline wallet.
    async fn erc20_transfer(
        &self,
        chain: &str,
        token_address: &str,
        to: &str,
        amount_wei: Nat,
    ) -> Result<String, String>; // tx hash

    // Native transfer from the pipeline wallet.
    async fn native_transfer(&self, chain: &str, to: &str, amount_wei: Nat) -> Result<String, String>;

    // Decimals
    async fn erc20_decimals(&self, chain: &str, token_address: &str) -> Result<u8, String>;
}

fn nat_to_u256(n: &Nat) -> Result<U256, String> {
    let bytes: Vec<u8> = n.0.to_bytes_be();
    if bytes.len() > 32 {
        return Err("Nat too large for U256".into());
    }
    Ok(U256::from_be_slice(&bytes))
}

fn u256_to_nat(v: U256) -> Nat {
    Nat::from(v.to::<u128>())
}

fn is_nonce_too_low_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("nonce too low")
        || normalized.contains("nonce has already been used")
        || normalized.contains("invalid transaction nonce")
}

pub struct EvmBackendImpl<P> {
    pub provider: P,
}

impl<P: Provider<AnyNetwork>> EvmBackendImpl<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }
}

impl<P> EvmBackendImpl<P>
where
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    pub(crate) async fn erc20_balance_of_raw(&self, token: Address, owner: Address) -> Result<U256, String> {
        let contract = IERC20::new(token, self.provider.clone());
        contract
            .balanceOf(owner)
            .call()
            .await
            .map_err(|e| format!("ERC20 balanceOf failed for token {token}: {e}"))
    }

    pub(crate) async fn erc20_allowance_raw(
        &self,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> Result<U256, String> {
        let contract = IERC20::new(token, self.provider.clone());
        contract
            .allowance(owner, spender)
            .call()
            .await
            .map_err(|e| format!("ERC20 allowance(owner={owner}, spender={spender}) failed for token {token}: {e}"))
    }

    pub(crate) async fn erc20_decimals_raw(&self, token: Address) -> Result<u8, String> {
        let contract = IERC20::new(token, self.provider.clone());
        contract
            .decimals()
            .call()
            .await
            .map_err(|e| format!("ERC20 decimals() failed for token {token}: {e}"))
    }

    pub(crate) async fn erc20_approve_and_wait_raw(
        &self,
        token: Address,
        spender: Address,
        amount: U256,
    ) -> Result<TxHash, String> {
        let contract = IERC20::new(token, self.provider.clone());
        let estimated_gas = contract.approve(spender, amount).estimate_gas().await.ok();
        let gas_limit = estimated_gas
            .map(|estimate| {
                let extra = estimate.saturating_mul(ERC20_APPROVE_GAS_BUFFER_BPS) / 10_000;
                estimate.saturating_add(extra).max(ERC20_APPROVE_GAS_LIMIT)
            })
            .unwrap_or(ERC20_APPROVE_GAS_LIMIT);

        let mut attempt: u8 = 0;
        let pending = loop {
            attempt = attempt.saturating_add(1);
            let send_result = contract
                .approve(spender, amount)
                // Some RPC/provider stacks under-estimate ERC20 approve gas, causing
                // intermittent out-of-gas failures. Use estimated gas with headroom.
                .gas(gas_limit)
                .send()
                .await;

            match send_result {
                Ok(pending) => break pending,
                Err(err) => {
                    let err_message = err.to_string();
                    if attempt < NONCE_RETRY_MAX_ATTEMPTS && is_nonce_too_low_error(&err_message) {
                        let backoff_ms = NONCE_RETRY_BASE_DELAY_MS.saturating_mul(attempt as u64);
                        sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(format!("ERC20 approve(spender={spender}) failed for token {token}: {err}"));
                }
            }
        };
        let tx_hash = *pending.tx_hash();

        pending
            .watch()
            .await
            .map_err(|e| format!("ERC20 approve confirmation failed for token {token}: {e}"))?;

        Ok(tx_hash)
    }
}

#[async_trait]
impl<P> EvmBackend for EvmBackendImpl<P>
where
    P: Provider<AnyNetwork> + WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    async fn erc20_balance(&self, _chain: &str, token_address: &str) -> Result<Nat, String> {
        let owner = self.provider.wallet().default_signer_address();

        let token_addr: Address = token_address
            .parse()
            .map_err(|e| format!("invalid token address: {e}"))?;

        let res = self.erc20_balance_of_raw(token_addr, owner).await?;

        Ok(u256_to_nat(res))
    }

    async fn native_balance(&self, _chain: &str) -> Result<Nat, String> {
        let bal: U256 = self
            .provider
            .get_balance(self.wallet_address())
            .await
            .map_err(|e| format!("native get_balance failed: {e}"))?;

        Ok(u256_to_nat(bal))
    }

    async fn erc20_transfer(
        &self,
        _chain: &str,
        token_address: &str,
        to: &str,
        amount_wei: Nat,
    ) -> Result<String, String> {
        let token_addr: Address = token_address
            .parse()
            .map_err(|e| format!("invalid token address: {e}"))?;
        let to_addr: Address = to.parse().map_err(|e| format!("invalid recipient address: {e}"))?;

        let amount = nat_to_u256(&amount_wei)?;

        let contract = IERC20::new(token_addr, self.provider.clone());

        let tx = contract
            .transfer(to_addr, amount)
            .send()
            .await
            .map_err(|e| format!("erc20 transfer send failed: {e}"))?;

        let tx_hash = format!("{:#x}", tx.tx_hash());
        Ok(tx_hash)
    }

    async fn native_transfer(&self, _chain: &str, to: &str, amount_wei: Nat) -> Result<String, String> {
        let to_addr: Address = to.parse().map_err(|e| format!("invalid recipient address: {e}"))?;
        let amount = nat_to_u256(&amount_wei)?;

        let tx = TransactionRequest::default().with_to(to_addr).with_value(amount);

        let pending = self
            .provider
            .send_transaction(tx.into())
            .await
            .map_err(|e| format!("native transfer send failed: {e}"))?;

        let tx_hash = pending
            .watch()
            .await
            .map_err(|e| format!("native transfer watch failed: {e}"))?;

        Ok(format!("{:#x}", tx_hash))
    }

    async fn erc20_decimals(&self, _chain: &str, token_address: &str) -> Result<u8, String> {
        let token_addr: Address = token_address
            .parse()
            .map_err(|e| format!("invalid token address: {e}"))?;

        self.erc20_decimals_raw(token_addr)
            .await
            .map_err(|e| format!("erc20 decimals() failed: {e}"))
    }
}

impl<P> EvmBackendImpl<P>
where
    P: WalletProvider<AnyNetwork> + Send + Sync + Clone + 'static,
{
    fn wallet_address(&self) -> Address {
        self.provider.default_signer_address()
    }
}

#[cfg(test)]
mod tests {
    use super::is_nonce_too_low_error;

    #[test]
    fn nonce_too_low_detector_matches_common_error_shapes() {
        assert!(is_nonce_too_low_error("error code -32000: Nonce too low"));
        assert!(is_nonce_too_low_error(
            "transaction rejected: nonce has already been used"
        ));
        assert!(is_nonce_too_low_error("invalid transaction nonce"));
        assert!(!is_nonce_too_low_error("insufficient funds for gas * price + value"));
    }
}
