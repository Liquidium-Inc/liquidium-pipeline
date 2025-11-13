use std::sync::Arc;

use async_trait::async_trait;

use crate::swappers::model::{SwapExecution, SwapQuote, SwapRequest};

#[async_trait]
pub trait SwapVenue: Send + Sync {
    fn venue_name(&self) -> &'static str;
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String>;
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String>;
}

pub struct SwapRouter {
    pub kong: Arc<dyn SwapVenue>,
    pub mexc: Arc<dyn SwapVenue>,
    // later: raydium, hyperliquid, etc
}

impl SwapRouter {
    pub fn new(kong: Arc<dyn SwapVenue>, mexc: Arc<dyn SwapVenue>) -> Self {
        Self { kong, mexc }
    }

    fn pick_venue<'a>(&'a self, req: &SwapRequest) -> &'a dyn SwapVenue {
        match req.venue_hint.as_deref() {
            Some("mexc") => &*self.mexc,
            Some("kong") | None => &*self.kong,
            Some(_) => &*self.kong, // fallback for unknown hints
        }
    }

    pub async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        self.pick_venue(req).quote(req).await
    }

    pub async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        self.pick_venue(req).execute(req).await
    }
}
