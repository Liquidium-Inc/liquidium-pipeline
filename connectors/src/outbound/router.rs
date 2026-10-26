//! Picks the sender for a send: by token when preparing, by the sender name
//! saved in the record afterwards.
use std::sync::Arc;

use async_trait::async_trait;
use liquidium_pipeline_core::{
    outbound::{OutboundIntent, OutboundSender, PreparedSend, SendOutcome},
    tokens::chain_token::ChainToken,
};

pub struct OutboundRouter {
    pub icp: Arc<dyn OutboundSender>,
    pub solana: Arc<dyn OutboundSender>,
    /// Chains without their own sender yet.
    pub fallback: Arc<dyn OutboundSender>,
}

impl OutboundRouter {
    fn for_token(&self, token: &ChainToken) -> &dyn OutboundSender {
        match token {
            ChainToken::Icp { .. } => self.icp.as_ref(),
            ChainToken::SolanaNative { .. } | ChainToken::SolanaSpl { .. } => self.solana.as_ref(),
            _ => self.fallback.as_ref(),
        }
    }

    fn for_record(&self, prepared: &PreparedSend) -> &dyn OutboundSender {
        match prepared.sender.as_str() {
            super::icp::NAME => self.icp.as_ref(),
            super::solana::NAME => self.solana.as_ref(),
            _ => self.fallback.as_ref(),
        }
    }
}

#[async_trait]
impl OutboundSender for OutboundRouter {
    async fn prepare(&self, intent: &OutboundIntent) -> Result<PreparedSend, String> {
        self.for_token(&intent.token).prepare(intent).await
    }

    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome {
        self.for_record(prepared).submit(prepared).await
    }

    async fn settle(&self, prepared: &PreparedSend) -> SendOutcome {
        self.for_record(prepared).settle(prepared).await
    }

    fn open_window_secs(&self, prepared: &PreparedSend) -> i64 {
        self.for_record(prepared).open_window_secs(prepared)
    }
}
