//! Drives one Rift leg from the trader account without a liquidation: the
//! preview, then one step per cycle until the leg completes or parks. The leg
//! is saved to a file after every cycle, so a stopped run resumes from it.
use std::{path::PathBuf, sync::Arc, time::Duration};

use liquidium_pipeline_core::tokens::{
    chain_token::ChainToken, chain_token_amount::ChainTokenAmount, token_registry::TokenRegistryTrait,
};
use tokio::sync::Mutex;

use crate::{
    bridge::{claims::WalBridgeClaims, runtime::BridgeRuntime},
    context::init_context,
    finalizers::{
        multi_venue::{MultiVenueAdapter, RIFT_VENUE_ID, VenueLegCheckpoint, VenueLegProgress, VenuePlanningContext},
        rift::runtime::build_rift_finalizer,
    },
    outbound::driver::OperatorDecision,
    persistance::{VenueLegQuote, VenueLegState, VenueLegStatus, sqlite::SqliteWalStore},
    swappers::model::SwapRequest,
    utils::now_ts,
};

/// Bridges take minutes and Rift serves one request a second without a key.
const CYCLE_INTERVAL: Duration = Duration::from_secs(15);

/// The leg on disk. Every cycle and every saved send rewrites it.
struct LegFile {
    path: PathBuf,
    leg: Mutex<VenueLegState>,
}

impl LegFile {
    fn save(path: &PathBuf, leg: &VenueLegState) -> Result<(), String> {
        let json = serde_json::to_string_pretty(leg).map_err(|error| error.to_string())?;
        std::fs::write(path, json).map_err(|error| format!("could not write {}: {error}", path.display()))
    }

    fn load(path: &PathBuf) -> Result<VenueLegState, String> {
        let json =
            std::fs::read_to_string(path).map_err(|error| format!("could not read {}: {error}", path.display()))?;
        serde_json::from_str(&json).map_err(|error| format!("{} is not a saved leg: {error}", path.display()))
    }

    async fn apply(&self, progress: &VenueLegProgress) -> Result<(), String> {
        let mut leg = self.leg.lock().await;
        leg.execution = progress.execution.clone();
        leg.status = progress.status;
        leg.result = progress.result.clone();
        leg.last_error = progress.last_error.clone();
        leg.failure = progress.failure;
        Self::save(&self.path, &leg)
    }
}

#[async_trait::async_trait]
impl VenueLegCheckpoint for LegFile {
    async fn checkpoint(&self, progress: VenueLegProgress) -> Result<(), String> {
        self.apply(&progress).await
    }
}

/// The ICP token of a symbol, from the registry the daemon uses.
fn icp_token(registry: &dyn TokenRegistryTrait, symbol: &str) -> Result<ChainToken, String> {
    let mut tokens = registry
        .by_symbol(symbol)
        .into_iter()
        .map(|(_, token)| token)
        .filter(|token| token.chain().eq_ignore_ascii_case("icp"));
    let token = tokens
        .next()
        .ok_or_else(|| format!("{symbol} is not an ICP token in the registry"))?;
    if tokens.next().is_some() {
        return Err(format!("{symbol} names more than one ICP token in the registry"));
    }
    Ok(token)
}

/// The operator answer from `--answer`: `absent` or `landed:<receipt>`.
fn parse_answer(answer: &str) -> Result<OperatorDecision, String> {
    match answer.trim() {
        "absent" => Ok(OperatorDecision::Absent),
        other => other
            .strip_prefix("landed:")
            .map(|receipt| OperatorDecision::Landed {
                receipt: receipt.trim().to_string(),
            })
            .filter(|decision| !matches!(decision, OperatorDecision::Landed { receipt } if receipt.is_empty()))
            .ok_or_else(|| "--answer takes `absent` or `landed:<receipt>`".to_string()),
    }
}

/// The leg's step, from its saved state.
fn step_of(leg: &VenueLegState) -> String {
    leg.execution
        .state
        .pointer("/rift/step")
        .and_then(|value| value.as_str())
        .unwrap_or("-")
        .to_string()
}

/// How long each step took, from the moments the step changed.
fn step_durations(timeline: &[(String, i64)], end_ts: i64) -> String {
    timeline
        .iter()
        .enumerate()
        .map(|(index, (step, started))| {
            let ended = timeline.get(index + 1).map_or(end_ts, |(_, next)| *next);
            format!("{step} {}s", ended - started)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// One line of where the leg stands, from its saved state.
fn describe(leg: &VenueLegState) -> String {
    let rift = leg.execution.state.get("rift").unwrap_or(&leg.execution.state);
    let text = |pointer: &str| {
        rift.pointer(pointer)
            .and_then(|value| value.as_str())
            .unwrap_or("-")
            .to_string()
    };
    format!(
        "status={:?} step={} order={} ({}) funding_txid={} deposit_bridge={} return_bridge={}{}",
        leg.status,
        text("/step"),
        text("/order/id"),
        text("/order/status"),
        text("/funding_txid"),
        text("/bridge_out/deposit_bridge_id"),
        text("/bridge_in/withdraw_bridge_id"),
        leg.last_error
            .as_deref()
            .map(|error| format!(" last_error={error}"))
            .unwrap_or_default()
    )
}

pub async fn check(
    pay: Option<String>,
    receive: Option<String>,
    amount: f64,
    confirm: bool,
    resume: Option<PathBuf>,
    answer: Option<String>,
) -> Result<(), String> {
    let ctx = init_context().await?;
    let config = ctx.config.clone();
    let db =
        Arc::new(SqliteWalStore::new(&config.db_path).map_err(|error| format!("could not open the db: {error:#}"))?);
    let bridge = BridgeRuntime::build(&config, Arc::new(WalBridgeClaims::new(db))).await?;
    let finalizer = build_rift_finalizer(&ctx, bridge)?;
    let answer = answer.as_deref().map(parse_answer).transpose()?;

    let (path, leg) = match resume {
        Some(path) => {
            let mut leg = LegFile::load(&path)?;
            println!("resumed {}: {}", path.display(), describe(&leg));
            if leg.status == VenueLegStatus::OperatorRequired {
                for question in finalizer.operator_questions(&leg)? {
                    println!("question: {}", question.detail);
                }
                // A parked leg moves only on an operator's word. The answer is
                // written into the leg; the next step reads it.
                leg.execution = finalizer.operator_release(&leg, answer.as_ref())?;
                leg.status = VenueLegStatus::Running;
                LegFile::save(&path, &leg)?;
                println!("released");
            }
            (path, leg)
        }
        None => {
            let pay = pay.ok_or("--pay is required to start a leg")?;
            let receive = receive.ok_or("--receive is required to start a leg")?;
            if !(amount.is_finite() && amount > 0.0) {
                return Err("--amount must be a positive number to start a leg".to_string());
            }
            let pay_token = icp_token(ctx.registry.as_ref(), &pay)?;
            let receive_token = icp_token(ctx.registry.as_ref(), &receive)?;
            let pay_amount = ChainTokenAmount::from_formatted(pay_token.clone(), amount);
            let request = SwapRequest {
                pay_asset: pay_token.asset_id(),
                pay_amount: pay_amount.clone(),
                receive_asset: receive_token.asset_id(),
                receive_address: None,
                max_slippage_bps: None,
                venue_hint: Some(RIFT_VENUE_ID.to_string()),
            };
            let id = format!("rift-check-{}", now_ts());
            finalizer
                .check_amount(&request)
                .await
                .map_err(|error| format!("Rift cannot take this request: {error}"))?;
            let preview = finalizer
                .preview(
                    &VenuePlanningContext {
                        liquidation_id: id.clone(),
                    },
                    &request,
                )
                .await?;
            let estimated = ChainTokenAmount::from_raw(receive_token.clone(), preview.quote.receive_amount.clone());
            println!(
                "preview: pay {} -> estimated {} (floor {}), route {}, price {}",
                pay_amount.formatted(),
                estimated.formatted(),
                preview.conservative_receive.formatted(),
                preview.quote.legs[0].route_id,
                preview.quote.exec_price
            );
            for cost in &preview.side_costs {
                println!("side cost: {} {} ({})", cost.amount, cost.symbol, cost.label);
            }
            println!(
                "the trader account pays the amount plus one ledger fee; the proceeds mint to the liquidator account"
            );
            if !confirm {
                println!("dry run: nothing sent. Add --confirm to run the leg.");
                return Ok(());
            }
            let leg = VenueLegState {
                leg_id: id.clone(),
                venue_id: RIFT_VENUE_ID.to_string(),
                request: preview.request,
                quote: VenueLegQuote {
                    pay_amount,
                    estimated_receive: estimated,
                    conservative_receive: preview.conservative_receive,
                    estimated_price_impact_bps: preview.quote.estimated_price_impact_bps,
                    route_id: preview.quote.legs[0].route_id.clone(),
                    // The planner prices side costs for the edge check; this
                    // check has no edge check, and the leg state keeps the raw costs.
                    side_costs: Vec::new(),
                },
                execution: preview.initial_execution_state,
                status: VenueLegStatus::Planned,
                result: None,
                last_error: None,
                failure: None,
            };
            let path = PathBuf::from(format!("{id}.json"));
            LegFile::save(&path, &leg)?;
            println!(
                "leg saved to {}; resume with --resume {}",
                path.display(),
                path.display()
            );
            (path, leg)
        }
    };

    let file = LegFile {
        path,
        leg: Mutex::new(leg),
    };
    let started = now_ts();
    // When each step began, for the summary. A resumed run times from here.
    let mut timeline: Vec<(String, i64)> = vec![(step_of(&*file.leg.lock().await), started)];
    loop {
        let leg = file.leg.lock().await.clone();
        let progress = finalizer.advance(&leg, &file).await?;
        file.apply(&progress).await?;
        let leg = file.leg.lock().await.clone();
        let now = now_ts();
        let step = step_of(&leg);
        if timeline.last().is_some_and(|(last, _)| *last != step) {
            timeline.push((step, now));
        }
        println!("[{:>5}s] {}", now - started, describe(&leg));
        match leg.status {
            VenueLegStatus::Completed | VenueLegStatus::Recovered => {
                let result = leg.result.ok_or("completed without a result")?;
                println!(
                    "RESULT: ok. paid {} {} and received {} {} at {} after {}s",
                    result.pay_amount,
                    result.pay_asset.symbol,
                    result.receive_amount,
                    result.receive_asset.symbol,
                    result.exec_price,
                    now - started
                );
                println!("steps: {}", step_durations(&timeline, now));
                return Ok(());
            }
            VenueLegStatus::FailedPermanent => {
                return Err(format!(
                    "the leg failed: {}",
                    leg.last_error.unwrap_or_else(|| "no error recorded".to_string())
                ));
            }
            VenueLegStatus::OperatorRequired => {
                println!("steps so far: {}", step_durations(&timeline, now));
                for question in finalizer.operator_questions(&leg)? {
                    println!("question: {}", question.detail);
                }
                return Err(format!(
                    "the leg parked. Check the ids above, then rerun with --resume {} and, for an open question, --answer absent or --answer landed:<receipt>",
                    file.path.display()
                ));
            }
            _ => tokio::time::sleep(CYCLE_INTERVAL).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_answer;
    use crate::outbound::driver::OperatorDecision;

    #[test]
    fn the_summary_times_each_step_until_the_next_one() {
        let timeline = vec![
            ("Order".to_string(), 100),
            ("BridgeOut".to_string(), 110),
            ("Wait".to_string(), 1_310),
        ];
        assert_eq!(
            super::step_durations(&timeline, 2_000),
            "Order 10s, BridgeOut 1200s, Wait 690s"
        );
    }

    #[test]
    fn an_answer_is_absent_or_a_landed_receipt() {
        assert_eq!(parse_answer("absent").unwrap(), OperatorDecision::Absent);
        assert_eq!(
            parse_answer(" landed:0xabc ").unwrap(),
            OperatorDecision::Landed {
                receipt: "0xabc".to_string()
            }
        );
        for bad in ["landed:", "landed", "yes", ""] {
            assert!(parse_answer(bad).is_err(), "{bad}");
        }
    }
}
