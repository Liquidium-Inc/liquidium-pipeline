
use crate::swappers::kong::kong_types::{
    ICTransferReply, SwapAmountsReply as KongSwapAmountsReply, SwapArgs as KongSwapArgs, SwapReply as KongSwapReply,
    SwapTxReply, TransferReply, TxId as KongTxId,
};
use crate::swappers::model::{AssetId, SwapExecution, SwapQuote, SwapQuoteLeg, SwapRequest, TransferRecord, TxRef};

fn default_ic_chain() -> String {
    "IC".to_string()
}

fn asset_to_chain_and_symbol(asset: &AssetId) -> (String, String) {
    (asset.chain.clone(), asset.symbol.clone())
}

fn chain_symbol_to_asset(chain: &str, symbol: &str) -> AssetId {
    AssetId {
        chain: chain.to_string(),
        symbol: symbol.to_string(),
    }
}

fn txref_from_ic_transfer(ic: &ICTransferReply) -> TxRef {
    TxRef::IcBlockIndex {
        ledger: ic.canister_id.clone(),
        block_index: ic.block_index.clone(),
    }
}

fn txref_to_kong(tx: &TxRef) -> Option<KongTxId> {
    match tx {
        TxRef::IcBlockIndex { block_index, .. } => Some(KongTxId::BlockIndex(block_index.clone())),
        TxRef::TxHash { hash, .. } => Some(KongTxId::TransactionHash(hash.clone())),
    }
}

// SwapRequest -> KongSwapArgs (for requests going to Kong)
impl From<SwapRequest> for KongSwapArgs {
    fn from(req: SwapRequest) -> Self {
        let (_, pay_symbol) = asset_to_chain_and_symbol(&req.pay_asset);
        let (_, recv_symbol) = asset_to_chain_and_symbol(&req.receive_asset);

        let pay_tx_id = req.pay_tx_ref.as_ref().and_then(|tx| txref_to_kong(tx));

        KongSwapArgs {
            pay_token: pay_symbol,
            pay_amount: req.pay_amount,
            pay_tx_id,
            receive_token: recv_symbol,
            receive_amount: None,
            receive_address: req.receive_address,
            max_slippage: req.max_slippage_bps.map(|bps| (bps as f64) / 10_000.0),
            referred_by: req.referred_by,
        }
    }
}

// Kong SwapAmountsReply (quote) -> generic SwapQuote
impl From<KongSwapAmountsReply> for SwapQuote {
    fn from(k: KongSwapAmountsReply) -> Self {
        let pay_asset = chain_symbol_to_asset(&k.pay_chain, &k.pay_symbol);
        let receive_asset = chain_symbol_to_asset(&k.receive_chain, &k.receive_symbol);

        let legs: Vec<SwapQuoteLeg> = k
            .txs
            .into_iter()
            .map(|tx| SwapQuoteLeg {
                venue: "kong".to_string(),
                route_id: tx.pool_symbol.clone(),

                pay_chain: tx.pay_chain.clone(),
                pay_symbol: tx.pay_symbol.clone(),
                pay_amount: tx.pay_amount.clone(),

                receive_chain: tx.receive_chain.clone(),
                receive_symbol: tx.receive_symbol.clone(),
                receive_amount: tx.receive_amount.clone(),

                price: tx.price,
                lp_fee: tx.lp_fee.clone(),
                gas_fee: tx.gas_fee.clone(),
            })
            .collect();

        SwapQuote {
            pay_asset,
            pay_amount: k.pay_amount,
            receive_asset,
            receive_amount: k.receive_amount,
            mid_price: k.mid_price,
            exec_price: k.price,
            slippage: k.slippage,
            legs,
        }
    }
}

// Kong SwapReply (execution) -> generic SwapExecution
impl From<KongSwapReply> for SwapExecution {
    fn from(r: KongSwapReply) -> Self {
        let pay_asset = chain_symbol_to_asset(&r.pay_chain, &r.pay_symbol);
        let receive_asset = chain_symbol_to_asset(&r.receive_chain, &r.receive_symbol);

        let legs: Vec<SwapQuoteLeg> = r
            .txs
            .iter()
            .map(|tx: &SwapTxReply| SwapQuoteLeg {
                venue: "kong".to_string(),
                route_id: tx.pool_symbol.clone(),

                pay_chain: tx.pay_chain.clone(),
                pay_symbol: tx.pay_symbol.clone(),
                pay_amount: tx.pay_amount.clone(),

                receive_chain: tx.receive_chain.clone(),
                receive_symbol: tx.receive_symbol.clone(),
                receive_amount: tx.receive_amount.clone(),

                price: tx.price,
                lp_fee: tx.lp_fee.clone(),
                gas_fee: tx.gas_fee.clone(),
            })
            .collect();

        let transfers: Vec<TransferRecord> = r
            .transfer_ids
            .iter()
            .flat_map(|t| match &t.transfer {
                TransferReply::IC(ic) => Some(TransferRecord {
                    asset: chain_symbol_to_asset(&ic.chain, &ic.symbol),
                    is_send: ic.is_send,
                    amount: ic.amount.clone(),
                    tx_ref: txref_from_ic_transfer(ic),
                }),
            })
            .collect();

        SwapExecution {
            swap_id: r.tx_id,
            request_id: r.request_id,
            status: r.status.clone(),
            pay_asset,
            pay_amount: r.pay_amount.clone(),
            receive_asset,
            receive_amount: r.receive_amount.clone(),
            mid_price: r.mid_price,
            exec_price: r.price,
            slippage: r.slippage,
            legs,
            transfers,
            claim_ids: r.claim_ids.clone(),
            ts: r.ts,
        }
    }
}
