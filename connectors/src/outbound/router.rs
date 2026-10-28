//! Picks the sender for a send: by token when preparing, by the sender name
//! saved in the record afterwards.
use std::sync::Arc;

use async_trait::async_trait;
use liquidium_pipeline_core::{
    outbound::{OutboundIntent, OutboundSender, PreparedSend, SendOutcome},
    tokens::chain_token::ChainToken,
};

/// Sends that their caller signed, picked by the sender name saved with each.
pub struct SignedSendRouter {
    pub solana: Arc<dyn OutboundSender>,
    pub evm: Arc<dyn OutboundSender>,
}

impl SignedSendRouter {
    fn for_record(&self, prepared: &PreparedSend) -> Option<&dyn OutboundSender> {
        match prepared.sender.as_str() {
            super::solana::NAME => Some(self.solana.as_ref()),
            super::evm::NAME => Some(self.evm.as_ref()),
            _ => None,
        }
    }

    fn unknown(prepared: &PreparedSend) -> SendOutcome {
        SendOutcome::Open {
            reason: format!("no sender named `{}` for send {}", prepared.sender, prepared.identity),
        }
    }
}

#[async_trait]
impl OutboundSender for SignedSendRouter {
    async fn prepare(&self, _intent: &OutboundIntent) -> Result<PreparedSend, String> {
        Err("a signed send is prepared by its caller".to_string())
    }

    async fn submit(&self, prepared: &PreparedSend) -> SendOutcome {
        match self.for_record(prepared) {
            Some(sender) => sender.submit(prepared).await,
            None => Self::unknown(prepared),
        }
    }

    async fn settle(&self, prepared: &PreparedSend) -> SendOutcome {
        match self.for_record(prepared) {
            Some(sender) => sender.settle(prepared).await,
            None => Self::unknown(prepared),
        }
    }

    fn open_window_secs(&self, prepared: &PreparedSend) -> i64 {
        // An unknown sender parks at once: nothing here can decide it.
        self.for_record(prepared)
            .map_or(0, |sender| sender.open_window_secs(prepared))
    }
}

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
