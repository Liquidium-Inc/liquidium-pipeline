use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use candid::{Nat, Principal};
use ic_agent::{Agent, Identity};
use icrc_ledger_types::icrc1::account::Account;
use liquidium_pipeline_commons::env::config_dir;
use liquidium_pipeline_connectors::{
    account::icp_account::derive_icp_identity,
    backend::icp_backend::{IcpBackend, IcpBackendImpl},
};
use liquidium_pipeline_core::tokens::{
    chain_token::ChainToken,
    chain_token_amount::ChainTokenAmount,
    exact_amount::{format_units, parse_decimal_units},
};
use serde::{Deserialize, Serialize};

use crate::{
    config::{parse_icpswap_factory_from_env, parse_icpswap_fee_tiers_from_env},
    swappers::{
        icpswap::{
            client::IcpswapClient,
            execution::IcpswapExecutionStateStore,
            state::{initial_slippage_bps, validate_execution_state},
            types::{IcpswapExecutionPlan, IcpswapExecutionState, IcpswapStep, IcpswapTokenMetadata},
            venue::{IcpswapFinalizerLogic, IcpswapVenue},
        },
        model::SwapRequest,
    },
    utils::{CKUSDC_LEDGER_PRINCIPAL, ICP_LEDGER_PRINCIPAL, now_nanos},
};

const DEFAULT_SLIPPAGE_BPS: u32 = 125;
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(600);
const RUN_FILE_VERSION: u32 = 4;

#[derive(Debug)]
pub struct IcpswapCommandOptions {
    pub amount: Option<String>,
    pub slippage_bps: Option<u32>,
    pub execute: bool,
    pub resume: Option<String>,
}

struct Runtime {
    endpoint: String,
    owner: Account,
    backend: Arc<IcpBackendImpl<Agent>>,
    workflow: Arc<dyn IcpswapFinalizerLogic>,
    icp: ChainToken,
    ckusdc: ChainToken,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunFile {
    version: u32,
    endpoint: String,
    state: IcpswapExecutionState,
}

struct FileStateStore {
    path: PathBuf,
    run_id: String,
    endpoint: String,
    owner: Account,
}

struct CliOwnerLock {
    path: PathBuf,
    run_id: String,
}

impl CliOwnerLock {
    fn acquire(owner: Account, run_id: &str) -> Result<Self, String> {
        let directory = PathBuf::from(config_dir()).join("icpswap-runs");
        fs::create_dir_all(&directory)
            .map_err(|error| format!("failed creating ICPSwap run directory {}: {error}", directory.display()))?;
        let path = directory.join(format!("owner-{}.lock", owner.owner.to_text()));
        let lock = Self {
            path,
            run_id: run_id.to_string(),
        };
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&lock.path) {
            Ok(mut file) => {
                file.write_all(run_id.as_bytes())
                    .and_then(|_| file.sync_all())
                    .map_err(|error| format!("failed writing owner lock {}: {error}", lock.path.display()))?;
                Ok(lock)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let active = fs::read_to_string(&lock.path)
                    .map_err(|read| format!("failed reading owner lock {}: {read}", lock.path.display()))?;
                if active.trim() == run_id {
                    Ok(lock)
                } else {
                    Err(format!(
                        "manual ICPSwap run {} already owns liquidator {}; resume it before starting {}",
                        active.trim(),
                        owner.owner,
                        run_id
                    ))
                }
            }
            Err(error) => Err(format!("failed creating owner lock {}: {error}", lock.path.display())),
        }
    }

    fn release(&self) -> Result<(), String> {
        let active = fs::read_to_string(&self.path)
            .map_err(|error| format!("failed reading owner lock {}: {error}", self.path.display()))?;
        if active.trim() != self.run_id {
            return Err(format!(
                "refusing to release owner lock held by {}, expected {}",
                active.trim(),
                self.run_id
            ));
        }
        fs::remove_file(&self.path)
            .map_err(|error| format!("failed releasing owner lock {}: {error}", self.path.display()))
    }
}

impl FileStateStore {
    fn new(path: PathBuf, run_id: String, endpoint: String, owner: Account) -> Self {
        Self {
            path,
            run_id,
            endpoint,
            owner,
        }
    }

    fn read_run(&self) -> Result<Option<RunFile>, String> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.path)
            .map_err(|error| format!("failed reading run file {}: {error}", self.path.display()))?;
        let run: RunFile = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid run file {}: {error}", self.path.display()))?;
        self.validate(&run)?;
        Ok(Some(run))
    }

    fn validate(&self, run: &RunFile) -> Result<(), String> {
        if run.version != RUN_FILE_VERSION {
            return Err(format!(
                "unsupported ICPSwap run-file version {}; expected {}",
                run.version, RUN_FILE_VERSION
            ));
        }
        if run.endpoint != self.endpoint {
            return Err(format!(
                "run endpoint {} differs from configured IC_URL {}",
                run.endpoint, self.endpoint
            ));
        }
        validate_execution_state(&run.state, &self.run_id, self.owner)?;
        validate_fixed_pair(&run.state.plan)
    }

    fn write_run(&self, state: &IcpswapExecutionState) -> Result<(), String> {
        validate_execution_state(state, &self.run_id, self.owner)?;
        if let Some(existing) = self.read_run()?
            && existing.state.plan != state.plan
        {
            return Err(format!("refusing to replace persisted plan for run {}", self.run_id));
        }

        let parent = self
            .path
            .parent()
            .ok_or_else(|| format!("run file {} has no parent directory", self.path.display()))?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed creating run directory {}: {error}", parent.display()))?;
        let run = RunFile {
            version: RUN_FILE_VERSION,
            endpoint: self.endpoint.clone(),
            state: state.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&run).map_err(|error| format!("failed encoding run state: {error}"))?;
        let temporary = self
            .path
            .with_extension(format!("tmp-{}-{}", std::process::id(), now_nanos()));

        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .map_err(|error| format!("failed opening checkpoint {}: {error}", temporary.display()))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| format!("failed writing checkpoint {}: {error}", temporary.display()))?;
        fs::rename(&temporary, &self.path).map_err(|error| {
            format!(
                "failed committing checkpoint {} -> {}: {error}",
                temporary.display(),
                self.path.display()
            )
        })?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("failed syncing checkpoint directory {}: {error}", parent.display()))?;
        Ok(())
    }
}

#[async_trait]
impl IcpswapExecutionStateStore for FileStateStore {
    async fn load(&self, liquidation_id: &str) -> Result<Option<IcpswapExecutionState>, String> {
        if liquidation_id != self.run_id {
            return Err(format!(
                "state-store key {liquidation_id} differs from run {}",
                self.run_id
            ));
        }
        Ok(self.read_run()?.map(|run| run.state))
    }

    async fn persist(&self, liquidation_id: &str, state: &IcpswapExecutionState) -> Result<(), String> {
        if liquidation_id != self.run_id {
            return Err(format!(
                "state-store key {liquidation_id} differs from run {}",
                self.run_id
            ));
        }
        self.write_run(state)
    }
}

pub async fn run(options: IcpswapCommandOptions) -> Result<(), String> {
    let runtime = build_runtime().await?;
    match options.resume {
        Some(run_id) => resume(runtime, &run_id).await,
        None => quote_or_execute(runtime, options).await,
    }
}

async fn quote_or_execute(runtime: Runtime, options: IcpswapCommandOptions) -> Result<(), String> {
    let amount_text = options
        .amount
        .as_deref()
        .ok_or_else(|| "--amount is required".to_string())?;
    let budget = parse_decimal_units(amount_text, runtime.icp.decimals())?;
    if budget == 0u8 {
        return Err("--amount must be greater than zero".to_string());
    }
    let balance = runtime
        .backend
        .icrc1_balance(
            runtime
                .icp
                .asset_id()
                .address
                .parse()
                .map_err(|e| format!("invalid ICP ledger: {e}"))?,
            &runtime.owner,
        )
        .await
        .map_err(|error| format!("failed reading liquidator ICP balance: {error}"))?;
    if balance < budget {
        return Err(format!(
            "insufficient liquidator ICP balance: requested {}, available {}",
            format_units(&budget, runtime.icp.decimals()),
            format_units(&balance, runtime.icp.decimals())
        ));
    }

    let request = SwapRequest {
        pay_asset: runtime.icp.asset_id(),
        pay_amount: ChainTokenAmount::from_raw(runtime.icp.clone(), budget.clone()),
        receive_asset: runtime.ckusdc.asset_id(),
        receive_address: None,
        max_slippage_bps: options.slippage_bps,
        venue_hint: Some("icpswap".to_string()),
    };
    let quoted = runtime
        .workflow
        .preview_route(&request)
        .await
        .map_err(|error| format!("ICPSwap quote failed: {error}"))?;
    print_quote(&runtime, &budget, &quoted.route);

    if !options.execute {
        println!("\nQuote only; no tokens were moved. Add --execute to submit this swap.");
        return Ok(());
    }
    if !confirm_execution(&runtime.owner, &budget, runtime.icp.decimals())? {
        println!("Swap cancelled; no tokens were moved.");
        return Ok(());
    }

    let run_id = new_run_id();
    let store = store_for(&runtime, &run_id)?;
    let owner_lock = CliOwnerLock::acquire(runtime.owner, &run_id)?;
    println!("Run ID: {run_id}");
    println!("Checkpoint: {}", store.path.display());
    drive_to_terminal(
        runtime.workflow.as_ref(),
        &store,
        &run_id,
        runtime.owner,
        Some(quoted.route),
        &owner_lock,
    )
    .await
}

async fn resume(runtime: Runtime, run_id: &str) -> Result<(), String> {
    validate_run_id(run_id)?;
    let store = store_for(&runtime, run_id)?;
    let state = store
        .load(run_id)
        .await?
        .ok_or_else(|| format!("no ICPSwap run found for {run_id}"))?;
    let owner_lock = CliOwnerLock::acquire(runtime.owner, run_id)?;
    validate_fixed_pair(&state.plan)?;
    println!("Resuming ICPSwap run {run_id} from step {:?}", state.step);
    drive_to_terminal(
        runtime.workflow.as_ref(),
        &store,
        run_id,
        runtime.owner,
        None,
        &owner_lock,
    )
    .await
}

async fn drive_to_terminal(
    workflow: &dyn IcpswapFinalizerLogic,
    store: &FileStateStore,
    run_id: &str,
    owner: Account,
    initial_plan: Option<IcpswapExecutionPlan>,
    owner_lock: &CliOwnerLock,
) -> Result<(), String> {
    let deadline = Instant::now() + COMMAND_TIMEOUT;
    let mut plan = initial_plan;
    loop {
        if Instant::now() >= deadline {
            print_resume(run_id, "Timed out while waiting for a terminal result.");
            return Err(format!("run {run_id} remains resumable"));
        }

        let state = store.load(run_id).await?;
        if let Some(state) = state.as_ref() {
            match state.step {
                IcpswapStep::Completed => {
                    let credited = state
                        .withdraw
                        .wallet_credited_amount
                        .as_ref()
                        .ok_or_else(|| "completed swap has no confirmed wallet credit".to_string())?;
                    println!(
                        "ICPSwap completed: received {} ckUSDC in the liquidator account.",
                        format_units(credited, state.plan.net_expected_output().token.decimals())
                    );
                    owner_lock.release()?;
                    return Ok(());
                }
                IcpswapStep::Refunded => {
                    let credited = state
                        .recovery
                        .wallet_credited_amount
                        .as_ref()
                        .map(|amount| format_units(amount, state.plan.amount_in.token.decimals()))
                        .unwrap_or_else(|| "unknown".to_string());
                    println!("ICPSwap failed, but {credited} ICP was confirmed returned to the liquidator account.");
                    owner_lock.release()?;
                    return Ok(());
                }
                IcpswapStep::OperatorRequired => {
                    // Run one read-only reconciliation pass below. If it
                    // remains ambiguous, the result handler stops without
                    // replaying the pending update.
                }
                IcpswapStep::Failed => {
                    return Err(format!(
                        "run {run_id} reached a terminal failure: {}",
                        state.last_error.as_deref().unwrap_or("unknown error")
                    ));
                }
                _ => {}
            }
        }

        let now = now_nanos();
        let advance = async {
            let state = match state {
                Some(state) => state,
                None => {
                    let fresh_plan = plan.take().ok_or_else(|| "missing initial ICPSwap plan".to_string())?;
                    let state = workflow.prepare(run_id, fresh_plan, owner);
                    store.persist(run_id, &state).await?;
                    state
                }
            };
            workflow.advance_loaded(store, run_id, owner, now, state).await
        };

        tokio::select! {
            result = advance => {
                match result {
                    Ok(state) => {
                        println!("ICPSwap run {run_id}: step {:?}", state.step);
                        if state.step == IcpswapStep::OperatorRequired {
                            print_resume(
                                run_id,
                                "The last update still has an ambiguous outcome; automatic replay is disabled.",
                            );
                            return Err(state
                                .last_error
                                .clone()
                                .unwrap_or_else(|| "operator reconciliation required".to_string()));
                        }
                    }
                    Err(error) => {
                        eprintln!("ICPSwap run {run_id}: retryable error: {error}");
                        let mut persisted = store.load(run_id).await?;
                        if persisted.is_none() {
                            return Err(error);
                        }
                        if let Some(state) = persisted.as_mut() {
                            state.last_error = Some(error.clone());
                            store.persist(run_id, state).await?;
                        }
                        if persisted
                            .as_ref()
                            .is_some_and(|state| state.step == IcpswapStep::OperatorRequired)
                        {
                            print_resume(
                                run_id,
                                "The last update has an ambiguous outcome; automatic replay is disabled.",
                            );
                            return Err(error);
                        }
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                print_resume(run_id, "Interrupted.");
                return Ok(());
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
            _ = tokio::signal::ctrl_c() => {
                print_resume(run_id, "Interrupted.");
                return Ok(());
            }
        }
    }
}

async fn build_runtime() -> Result<Runtime, String> {
    let endpoint = env::var("IC_URL").map_err(|_| "IC_URL not configured".to_string())?;
    let mnemonic_path = env::var("MNEMONIC_FILE").map_err(|_| "MNEMONIC_FILE not configured".to_string())?;
    let mnemonic_path = expand_tilde(&mnemonic_path);
    let mnemonic = fs::read_to_string(&mnemonic_path)
        .map_err(|error| format!("failed reading mnemonic file {}: {error}", mnemonic_path.display()))?;
    let identity = derive_icp_identity(mnemonic.trim(), 0, 0)
        .map_err(|error| format!("could not create liquidator identity: {error}"))?;
    let principal = identity
        .sender()
        .map_err(|error| format!("could not derive liquidator principal: {error}"))?;
    let agent = Arc::new(
        Agent::builder()
            .with_url(endpoint.clone())
            .with_identity(identity)
            .with_max_tcp_error_retries(3)
            .build()
            .map_err(|error| format!("failed building liquidator IC agent: {error}"))?,
    );
    let backend = Arc::new(IcpBackendImpl::new(agent.clone()));
    let icp_ledger = Principal::from_text(ICP_LEDGER_PRINCIPAL).map_err(|error| error.to_string())?;
    let ckusdc_ledger = Principal::from_text(CKUSDC_LEDGER_PRINCIPAL).map_err(|error| error.to_string())?;
    let (icp_decimals, icp_fee, ckusdc_decimals, ckusdc_fee) = tokio::try_join!(
        backend.icrc1_decimals(icp_ledger),
        backend.icrc1_fee(icp_ledger),
        backend.icrc1_decimals(ckusdc_ledger),
        backend.icrc1_fee(ckusdc_ledger),
    )
    .map_err(|error| format!("failed loading ledger metadata: {error}"))?;
    let icp = ChainToken::Icp {
        ledger: icp_ledger,
        symbol: "ICP".to_string(),
        decimals: icp_decimals,
        fee: icp_fee,
    };
    let ckusdc = ChainToken::Icp {
        ledger: ckusdc_ledger,
        symbol: "ckUSDC".to_string(),
        decimals: ckusdc_decimals,
        fee: ckusdc_fee,
    };
    let factory = parse_icpswap_factory_from_env()?;
    let fee_tiers = parse_icpswap_fee_tiers_from_env()?;
    let default_slippage = env::var("MAX_ALLOWED_DEX_SLIPPAGE")
        .or_else(|_| env::var("MAX_ALLOWED_SLIPPAGE_BPS"))
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(DEFAULT_SLIPPAGE_BPS);
    let client = Arc::new(IcpswapClient::new(agent, backend.clone(), factory));
    let workflow: Arc<dyn IcpswapFinalizerLogic> = Arc::new(
        IcpswapVenue::new(
            client,
            vec![
                IcpswapTokenMetadata {
                    token: icp.clone(),
                    standard: "ICRC2".to_string(),
                },
                IcpswapTokenMetadata {
                    token: ckusdc.clone(),
                    standard: "ICRC2".to_string(),
                },
            ],
            fee_tiers,
            default_slippage,
        )
        .map_err(|error| format!("invalid ICPSwap configuration: {error}"))?,
    );
    Ok(Runtime {
        endpoint,
        owner: Account {
            owner: principal,
            subaccount: None,
        },
        backend,
        workflow,
        icp,
        ckusdc,
    })
}

fn print_quote(runtime: &Runtime, budget: &Nat, plan: &IcpswapExecutionPlan) {
    let expected_output = plan.net_expected_output();
    println!("\n=== ICPSwap ICP -> ckUSDC ===\n");
    println!("Endpoint:             {}", runtime.endpoint);
    println!("Liquidator:           {}", runtime.owner);
    println!("Pool:                 {}", plan.pool);
    println!("Funding flow:         ICRC-1 transfer -> pool subaccount -> deposit");
    println!("Fee tier:             {}", plan.fee_tier);
    println!(
        "Maximum ICP debit:    {} ICP",
        format_units(budget, runtime.icp.decimals())
    );
    println!(
        "Pool input:           {} ICP",
        format_units(&plan.amount_in.value, runtime.icp.decimals())
    );
    println!(
        "ICP ledger fees:      up to {} ICP",
        format_units(
            &(plan.input_ledger_fee.value.clone() * Nat::from(2u8)),
            runtime.icp.decimals()
        )
    );
    println!("Output overview:      net = gross - ckUSDC ledger fee");
    println!(
        "Gross quoted output:  {} ckUSDC",
        format_units(&plan.gross_quoted_out.value, runtime.ckusdc.decimals())
    );
    println!(
        "Expected net output:  {} ckUSDC (gross quoted output - ledger fee)",
        format_units(&expected_output.value, runtime.ckusdc.decimals())
    );
    println!(
        "ckUSDC ledger fee:    {} ckUSDC",
        format_units(&plan.output_ledger_fee.value, runtime.ckusdc.decimals())
    );
    let initial_minimum = crate::swappers::icpswap::plan::amount_out_minimum(
        &plan.gross_quoted_out.value,
        initial_slippage_bps(plan.max_slippage_bps),
    )
    .unwrap_or_else(|_| plan.amount_out_minimum.value.clone());
    println!(
        "Initial gross minimum: {} ckUSDC",
        format_units(&initial_minimum, runtime.ckusdc.decimals())
    );
    println!(
        "Hard gross minimum:    {} ckUSDC",
        format_units(&plan.amount_out_minimum.value, runtime.ckusdc.decimals())
    );
    println!(
        "Minimum net credit:   {} ckUSDC (minimum gross output - ledger fee)",
        format_units(
            &subtract_or_zero(&plan.amount_out_minimum.value, &plan.output_ledger_fee.value),
            runtime.ckusdc.decimals()
        )
    );
    println!(
        "Pool slippage cap:    {} bps (up to 3 balance-reconciled retries)",
        plan.max_slippage_bps
    );
}

fn confirm_execution(owner: &Account, budget: &Nat, decimals: u8) -> Result<bool, String> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut stdout = io::stdout();
    confirm_execution_with(
        &mut reader,
        &mut stdout,
        &format!(
            "Submit a live swap spending up to {} ICP from {}? [y/N] ",
            format_units(budget, decimals),
            owner
        ),
    )
}

fn confirm_execution_with(reader: &mut dyn BufRead, writer: &mut dyn Write, prompt: &str) -> Result<bool, String> {
    writer
        .write_all(prompt.as_bytes())
        .and_then(|_| writer.flush())
        .map_err(|error| format!("failed writing confirmation prompt: {error}"))?;
    let mut answer = String::new();
    reader
        .read_line(&mut answer)
        .map_err(|error| format!("failed reading confirmation: {error}"))?;
    Ok(matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes"))
}

fn store_for(runtime: &Runtime, run_id: &str) -> Result<FileStateStore, String> {
    validate_run_id(run_id)?;
    let path = PathBuf::from(config_dir())
        .join("icpswap-runs")
        .join(format!("{run_id}.json"));
    Ok(FileStateStore::new(
        path,
        run_id.to_string(),
        runtime.endpoint.clone(),
        runtime.owner,
    ))
}

fn validate_fixed_pair(plan: &IcpswapExecutionPlan) -> Result<(), String> {
    let icp = Principal::from_text(ICP_LEDGER_PRINCIPAL).map_err(|error| error.to_string())?;
    let ckusdc = Principal::from_text(CKUSDC_LEDGER_PRINCIPAL).map_err(|error| error.to_string())?;
    if plan.token_in != icp || plan.token_out != ckusdc {
        return Err(format!(
            "persisted plan is not canonical ICP -> ckUSDC: {} -> {}",
            plan.token_in, plan.token_out
        ));
    }
    Ok(())
}

fn subtract_or_zero(value: &Nat, fee: &Nat) -> Nat {
    if value > fee {
        value.clone() - fee.clone()
    } else {
        Nat::from(0u8)
    }
}

fn validate_run_id(run_id: &str) -> Result<(), String> {
    if run_id.is_empty()
        || run_id.len() > 128
        || !run_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err("invalid run ID; use only letters, digits, '-' and '_'".to_string());
    }
    Ok(())
}

fn new_run_id() -> String {
    format!("icpswap-{}-{}", now_nanos(), std::process::id())
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(relative) = path.strip_prefix("~/")
        && let Ok(home) = env::var("HOME")
    {
        return Path::new(&home).join(relative);
    }
    PathBuf::from(path)
}

fn print_resume(run_id: &str, prefix: &str) {
    println!("{prefix} Resume safely with: liquidator icpswap --resume {run_id}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_plan() -> IcpswapExecutionPlan {
        let icp_ledger = Principal::from_text(ICP_LEDGER_PRINCIPAL).unwrap();
        let ckusdc_ledger = Principal::from_text(CKUSDC_LEDGER_PRINCIPAL).unwrap();
        let icp = ChainToken::Icp {
            ledger: icp_ledger,
            symbol: "ICP".to_string(),
            decimals: 8,
            fee: Nat::from(10_000u64),
        };
        let ckusdc = ChainToken::Icp {
            ledger: ckusdc_ledger,
            symbol: "ckUSDC".to_string(),
            decimals: 6,
            fee: Nat::from(10_000u64),
        };
        IcpswapExecutionPlan::new(
            Principal::management_canister(),
            icp_ledger,
            ckusdc_ledger,
            Nat::from(3_000u64),
            ChainTokenAmount::from_raw(icp.clone(), Nat::from(9_998_000u64)),
            ChainTokenAmount::from_raw(icp, Nat::from(10_000u64)),
            ChainTokenAmount::from_raw(ckusdc.clone(), Nat::from(5_000_000u64)),
            ChainTokenAmount::from_raw(ckusdc, Nat::from(10_000u64)),
            125,
        )
        .unwrap()
    }

    #[test]
    fn parses_decimal_amounts_exactly() {
        assert_eq!(parse_decimal_units("0.1", 8).unwrap(), Nat::from(10_000_000u64));
        assert_eq!(parse_decimal_units("1.00000001", 8).unwrap(), Nat::from(100_000_001u64));
        assert_eq!(parse_decimal_units(".5", 8).unwrap(), Nat::from(50_000_000u64));
        assert_eq!(parse_decimal_units("1", 8).unwrap(), Nat::from(100_000_000u64));
    }

    #[test]
    fn rejects_invalid_decimal_amounts() {
        for invalid in ["", "-1", "+1", "1e2", "1.2.3", "abc", "0.000000001"] {
            assert!(parse_decimal_units(invalid, 8).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn formats_native_units_without_float_rounding() {
        assert_eq!(format_units(&Nat::from(10_000_000u64), 8), "0.1");
        assert_eq!(format_units(&Nat::from(100_000_001u64), 8), "1.00000001");
        assert_eq!(format_units(&Nat::from(0u8), 6), "0");
    }

    #[test]
    fn minimum_net_credit_subtracts_output_ledger_fee() {
        let plan = test_plan();
        assert_eq!(plan.amount_out_minimum.value, Nat::from(4_937_500u64));
        assert_eq!(
            subtract_or_zero(&plan.amount_out_minimum.value, &plan.output_ledger_fee.value),
            Nat::from(4_927_500u64)
        );
        assert_eq!(subtract_or_zero(&Nat::from(10u8), &Nat::from(10u8)), Nat::from(0u8));
    }

    #[test]
    fn confirmation_is_fail_closed() {
        let mut yes = io::Cursor::new(b"yes\n".to_vec());
        let mut no = io::Cursor::new(b"anything else\n".to_vec());
        let mut output = Vec::new();
        assert!(confirm_execution_with(&mut yes, &mut output, "confirm").unwrap());
        assert!(!confirm_execution_with(&mut no, &mut output, "confirm").unwrap());
    }

    #[test]
    fn run_ids_cannot_escape_checkpoint_directory() {
        assert!(validate_run_id("icpswap-123_456").is_ok());
        assert!(validate_run_id("../wal.db").is_err());
        assert!(validate_run_id("a/b").is_err());
    }

    #[tokio::test]
    async fn checkpoint_round_trip_preserves_execution_state() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run.json");
        let owner = Account {
            owner: Principal::anonymous(),
            subaccount: None,
        };
        let store = FileStateStore::new(path, "run-1".to_string(), "https://icp-api.io".to_string(), owner);
        let state = IcpswapExecutionState::prepare("run-1", test_plan(), owner);

        store.persist("run-1", &state).await.unwrap();

        assert_eq!(store.load("run-1").await.unwrap(), Some(state));
    }

    #[tokio::test]
    async fn checkpoint_round_trip_preserves_manual_workflow_state() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run.json");
        let owner = Account {
            owner: Principal::anonymous(),
            subaccount: None,
        };
        let store = FileStateStore::new(path, "run-1".to_string(), "https://icp-api.io".to_string(), owner);
        let mut state = IcpswapExecutionState::prepare("run-1", test_plan(), owner);
        state.deposit.input_pool_balance_before = Some(Nat::from(17u8));

        store.persist("run-1", &state).await.unwrap();

        assert_eq!(store.load("run-1").await.unwrap(), Some(state));
    }

    #[tokio::test]
    async fn checkpoint_rejects_endpoint_and_plan_replacement() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run.json");
        let owner = Account {
            owner: Principal::anonymous(),
            subaccount: None,
        };
        let original = FileStateStore::new(
            path.clone(),
            "run-1".to_string(),
            "https://icp-api.io".to_string(),
            owner,
        );
        let state = IcpswapExecutionState::prepare("run-1", test_plan(), owner);
        original.persist("run-1", &state).await.unwrap();

        let wrong_endpoint = FileStateStore::new(
            path.clone(),
            "run-1".to_string(),
            "https://example.invalid".to_string(),
            owner,
        );
        assert!(wrong_endpoint.load("run-1").await.is_err());

        let wrong_owner = FileStateStore::new(
            path,
            "run-1".to_string(),
            "https://icp-api.io".to_string(),
            Account {
                owner: Principal::management_canister(),
                subaccount: None,
            },
        );
        assert!(wrong_owner.load("run-1").await.is_err());

        let mut changed = state;
        changed.plan.fee_tier += Nat::from(1u8);
        assert!(original.persist("run-1", &changed).await.is_err());
    }

    #[tokio::test]
    async fn checkpoint_rejects_execution_id_mismatch() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run.json");
        let owner = Account {
            owner: Principal::anonymous(),
            subaccount: None,
        };
        let store = FileStateStore::new(path, "run-1".to_string(), "https://icp-api.io".to_string(), owner);
        let state = IcpswapExecutionState::prepare("different-run", test_plan(), owner);

        assert!(store.persist("run-1", &state).await.is_err());
        assert!(!store.path.exists());
    }

    #[tokio::test]
    async fn corrupt_checkpoint_is_rejected() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("run.json");
        fs::write(&path, b"not json").unwrap();
        let store = FileStateStore::new(
            path,
            "run-1".to_string(),
            "https://icp-api.io".to_string(),
            Account {
                owner: Principal::anonymous(),
                subaccount: None,
            },
        );

        assert!(store.load("run-1").await.is_err());
    }

    #[test]
    fn cli_owner_lock_is_exclusive_and_execution_scoped() {
        let directory = TempDir::new().unwrap();
        let lock = CliOwnerLock {
            path: directory.path().join("owner.lock"),
            run_id: "run-1".to_string(),
        };
        fs::write(&lock.path, "run-1").unwrap();
        assert!(lock.release().is_ok());
        assert!(!lock.path.exists());

        fs::write(&lock.path, "run-2").unwrap();
        assert!(lock.release().is_err());
        assert!(lock.path.exists());
    }
}
