use std::sync::Arc;

use async_trait::async_trait;
use candid::Principal;
use liquidium_pipeline_connectors::pipeline_agent::PipelineAgent;
use liquidium_pipeline_core::tokens::chain_token::ChainToken;
use log::{info, warn};

use crate::error::AppResult;
use crate::swappers::kong::kong_swapper::KongSwapSwapper;
use crate::swappers::kong::kong_types::{
    SwapAmountsReply as KongSwapAmountsReply, SwapArgs as KongSwapArgs, SwapReply as KongSwapReply,
};
use crate::swappers::model::{SwapExecution, SwapQuote, SwapRequest};
use crate::swappers::router::SwapVenue;

/// KongVenue is a generic venue wrapper over the KongSwapSwapper.
/// It takes a generic SwapRequest / SwapQuote / SwapExecution and
/// bridges them to the Kong-specific types and canister calls.
pub struct KongVenue<A: PipelineAgent> {
    pub swapper: Arc<KongSwapSwapper<A>>,
    pub tokens: Vec<ChainToken>, // registry of known ICRC tokens (by symbol)
}

impl<A: PipelineAgent> KongVenue<A> {
    pub fn new(swapper: Arc<KongSwapSwapper<A>>, tokens: Vec<ChainToken>) -> Self {
        Self { swapper, tokens }
    }

    fn find_token(&self, symbol: &str) -> AppResult<ChainToken> {
        self.tokens
            .iter()
            .find(|t| t.symbol() == symbol)
            .cloned()
            .ok_or_else(|| format!("Unknown ICRC token symbol in KongVenue: {}", symbol).into())
    }
}

#[async_trait]
impl<A> SwapVenue for KongVenue<A>
where
    A: PipelineAgent + Send + Sync + 'static,
{
    fn venue_name(&self) -> &'static str {
        "kong"
    }

    async fn init(&self) -> AppResult<()> {
        let kong_tokens = self
            .tokens
            .iter()
            .map(|item| {
                Principal::from_text(item.asset_id().address)
                    .map_err(|e| format!("invalid token principal {}: {}", item.asset_id().address, e))
            })
            .collect::<Result<Vec<Principal>, String>>()?;

        self.swapper
            .init(&kong_tokens)
            .await
            .map_err(|e| format!("could not init kong venue: {}", e))?;

        Ok(())
    }

    async fn quote(&self, req: &SwapRequest) -> AppResult<SwapQuote> {
        let token_out = self.find_token(&req.receive_asset.symbol)?;
        let token_in = self.find_token(&req.pay_asset.symbol)?;

        info!(
            "KongVenue quote {} {} -> {} | {}",
            req.pay_amount.formatted(),
            req.pay_amount.token.symbol(),
            token_out.symbol(),
            req.receive_asset.symbol
        );

        // Use the existing KongSwapSwapper IcrcSwapInterface implementation
        let kong_reply: KongSwapAmountsReply = self
            .swapper
            .get_swap_info(&token_in, &token_out, &req.pay_amount)
            .await?;

        // Convert KongSwapAmountsReply -> generic SwapQuote via adapter
        Ok(SwapQuote::from(kong_reply))
    }

    async fn execute(&self, req: &SwapRequest) -> AppResult<SwapExecution> {
        let token_in = self.find_token(&req.pay_asset.symbol)?;

        let mut kong_req: KongSwapArgs = KongSwapArgs::from(req.clone());
        kong_req.pay_amount = req.pay_amount.value.clone();

        let approved = self.swapper.ensure_allowance(&token_in).await?;
        if approved {
            let fee = token_in.fee();
            if fee > 0u8 {
                if kong_req.pay_amount <= fee {
                    return Err(format!(
                        "swap amount {} too small to cover approval fee {} for {}",
                        req.pay_amount.formatted(),
                        fee,
                        token_in.symbol()
                    )
                    .into());
                }
                kong_req.pay_amount -= fee.clone();
                info!(
                    "KongVenue approval fee applied: asset={} fee={} adjusted_pay={}",
                    token_in.symbol(),
                    fee,
                    kong_req.pay_amount
                );
            }
        }

        let balance = self.swapper.balance_of(&token_in).await?;
        let fee = token_in.fee();
        if balance <= fee {
            return Err(format!("insufficient balance for fee: balance={} fee={}", balance, fee).into());
        }
        let max_pay = balance - fee.clone();
        if kong_req.pay_amount > max_pay {
            info!(
                "[kong] adjust pay_amount for balance: requested={} max_pay={} fee={}",
                kong_req.pay_amount, max_pay, fee
            );
            kong_req.pay_amount = max_pay;
        }

        info!(
            "[kong] execute pay={} {} -> {} recv_addr={:?} max_slip={:?}",
            req.pay_amount.value,
            req.pay_asset.symbol,
            req.receive_asset.symbol,
            req.receive_address,
            req.max_slippage_bps
        );

        let max_slip_bps = req.max_slippage_bps;
        let max_slip_pct = max_slip_bps.map(|bps| (bps as f64) / 100.0);
        let kong_max_slip = kong_req.max_slippage;
        let pay_amount_final = kong_req.pay_amount.clone();

        // Execute swap on Kong
        let reply: KongSwapReply = self.swapper.swap(kong_req).await.map_err(|e| {
            warn!(
                "[kong] execute failed pay={} {} -> {} err={} max_slip_bps={:?} max_slip_pct={:?} kong_max_slip={:?} pay_amount_final={}",
                req.pay_amount.value,
                req.pay_asset.symbol,
                req.receive_asset.symbol,
                e,
                max_slip_bps,
                max_slip_pct,
                kong_max_slip,
                pay_amount_final
            );
            e
        })?;

        // KongSwapReply -> generic SwapExecution via adapter
        Ok(SwapExecution::from(reply))
    }
}

#[cfg(test)]
mod tests {
    use crate::approval_state::ApprovalState;
    use crate::swappers::router::SwapVenue;
    use candid::{Decode, Nat, Principal};
    use icrc_ledger_types::icrc1::account::Account;
    use icrc_ledger_types::icrc2::allowance::Allowance;
    use icrc_ledger_types::icrc2::approve::ApproveError;
    use liquidium_pipeline_connectors::pipeline_agent::MockPipelineAgent;
    use liquidium_pipeline_core::tokens::{chain_token::ChainToken, chain_token_amount::ChainTokenAmount};
    use std::sync::{Arc, Mutex};

    use crate::swappers::kong::kong_swapper::KongSwapSwapper;
    use crate::swappers::kong::kong_types::{SwapArgs, SwapReply, SwapResult};
    use crate::swappers::model::SwapRequest;

    use super::KongVenue;

    fn dummy_swap_reply(pay_amount: Nat) -> SwapReply {
        SwapReply {
            tx_id: 1,
            request_id: 2,
            status: "ok".to_string(),
            pay_chain: "ICP".to_string(),
            pay_symbol: "ICP".to_string(),
            pay_amount,
            receive_chain: "ICP".to_string(),
            receive_symbol: "ckBTC".to_string(),
            receive_amount: Nat::from(1u8),
            mid_price: 1.0,
            price: 1.0,
            slippage: 0.0,
            txs: Vec::new(),
            transfer_ids: Vec::new(),
            claim_ids: Vec::new(),
            ts: 0,
        }
    }

    #[tokio::test]
    async fn kong_venue_execute_applies_fee_after_approve() {
        let ledger = Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap();
        let token_in = ChainToken::Icp {
            ledger,
            symbol: "ICP".to_string(),
            decimals: 6,
            fee: Nat::from(10u8),
        };
        let token_out = ChainToken::Icp {
            ledger: Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(10u8),
        };

        let pay_amount = Nat::from(100u8);
        let req = SwapRequest {
            pay_asset: token_in.asset_id(),
            pay_amount: ChainTokenAmount::from_raw(token_in.clone(), pay_amount.clone()),
            receive_asset: token_out.asset_id(),
            receive_address: None,
            max_slippage_bps: None,
            venue_hint: None,
        };

        let mut mock_agent = MockPipelineAgent::new();
        mock_agent
            .expect_agent()
            .returning(|| panic!("agent() should not be called in kong_venue_execute_applies_fee_after_approve"));

        let ledger_allowance = ledger;
        let ledger_balance = ledger_allowance;

        mock_agent
            .expect_call_query::<Allowance>()
            .withf(move |canister, method, _| *canister == ledger_allowance && method == "icrc2_allowance")
            .returning(|_, _, _| {
                Ok(Allowance {
                    allowance: Nat::from(0u8),
                    expires_at: None,
                })
            });

        mock_agent
            .expect_call_query::<Nat>()
            .withf(move |canister, method, _| *canister == ledger_balance && method == "icrc1_balance_of")
            .returning(|_, _, _| Ok(Nat::from(1_000u32)));

        let counts = Arc::new(Mutex::new((0usize, 0usize)));
        let counts_clone = counts.clone();
        let counts_clone_approve = counts_clone.clone();
        mock_agent
            .expect_call_update::<Result<Nat, ApproveError>>()
            .withf(|_, method, _| method == "icrc2_approve")
            .times(1)
            .returning(move |_, _, _| {
                let mut guard = counts_clone_approve.lock().expect("counts mutex poisoned");
                guard.0 += 1;
                Ok(Ok(Nat::from(500u32)))
            });

        let counts_clone_swap = counts_clone.clone();
        mock_agent
            .expect_call_update::<SwapResult>()
            .withf(|_, method, arg| {
                if method != "swap" {
                    return false;
                }
                let decoded: SwapArgs = Decode!(&arg, SwapArgs).expect("decode swap args");
                decoded.pay_amount == 90u8
            })
            .times(1)
            .returning(move |_, _, _| {
                let mut guard = counts_clone_swap.lock().expect("counts mutex poisoned");
                guard.1 += 1;
                Ok(SwapResult::Ok(dummy_swap_reply(Nat::from(90u8))))
            });

        let swapper = Arc::new(KongSwapSwapper::new(
            Arc::new(mock_agent),
            Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
            Arc::new(ApprovalState::new()),
        ));
        let venue = KongVenue::new(swapper, vec![token_in, token_out]);

        venue.execute(&req).await.expect("execute should succeed");

        let counts = counts.lock().expect("counts mutex poisoned");
        assert_eq!(*counts, (1, 1));
    }

    #[tokio::test]
    async fn kong_venue_execute_skips_fee_when_allowance_ok() {
        let ledger = Principal::from_text("ryjl3-tyaaa-aaaaa-aaaba-cai").unwrap();
        let token_in = ChainToken::Icp {
            ledger,
            symbol: "ICP".to_string(),
            decimals: 6,
            fee: Nat::from(10u8),
        };
        let token_out = ChainToken::Icp {
            ledger: Principal::from_text("mxzaz-hqaaa-aaaar-qaada-cai").unwrap(),
            symbol: "ckBTC".to_string(),
            decimals: 8,
            fee: Nat::from(10u8),
        };

        let pay_amount = Nat::from(100u8);
        let req = SwapRequest {
            pay_asset: token_in.asset_id(),
            pay_amount: ChainTokenAmount::from_raw(token_in.clone(), pay_amount.clone()),
            receive_asset: token_out.asset_id(),
            receive_address: None,
            max_slippage_bps: None,
            venue_hint: None,
        };

        let mut mock_agent = MockPipelineAgent::new();
        mock_agent
            .expect_agent()
            .returning(|| panic!("agent() should not be called in kong_venue_execute_skips_fee_when_allowance_ok"));

        let ledger_allowance = ledger;
        let ledger_balance = ledger_allowance;

        mock_agent
            .expect_call_query::<Allowance>()
            .withf(move |canister, method, _| *canister == ledger_allowance && method == "icrc2_allowance")
            .returning(|_, _, _| {
                Ok(Allowance {
                    allowance: Nat::from(u64::MAX),
                    expires_at: None,
                })
            });

        mock_agent
            .expect_call_query::<Nat>()
            .withf(move |canister, method, _| *canister == ledger_balance && method == "icrc1_balance_of")
            .returning(|_, _, _| Ok(Nat::from(1_000u32)));

        mock_agent
            .expect_call_update::<SwapResult>()
            .withf(|_, method, arg| {
                if method != "swap" {
                    return false;
                }
                let decoded: SwapArgs = Decode!(&arg, SwapArgs).expect("decode swap args");
                decoded.pay_amount == 100u8
            })
            .times(1)
            .returning(move |_, _, _| Ok(SwapResult::Ok(dummy_swap_reply(Nat::from(100u8)))));

        let swapper = Arc::new(KongSwapSwapper::new(
            Arc::new(mock_agent),
            Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
            Arc::new(ApprovalState::new()),
        ));
        let venue = KongVenue::new(swapper, vec![token_in, token_out]);

        venue.execute(&req).await.expect("execute should succeed");
    }
}
