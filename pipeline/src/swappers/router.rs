use std::sync::Arc;

use async_trait::async_trait;

use crate::swappers::{
    model::{SwapExecution, SwapQuote, SwapRequest},
    swap_interface::SwapInterface,
};

#[async_trait]
pub trait SwapVenue: Send + Sync {
    fn venue_name(&self) -> &'static str;
    async fn init(&self) -> Result<(), String>;
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String>;
    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String>;
}

#[async_trait]
impl SwapInterface for SwapRouter {
    async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        // delegate to inherent method to avoid recursion
        SwapRouter::quote(self, req).await
    }

    async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        SwapRouter::execute(self, req).await
    }
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

    pub async fn init(&self) -> Result<(), String> {
        let _ = self.kong.init().await;
        let _ = self.mexc.init().await;

        Ok(())
    }

    pub async fn quote(&self, req: &SwapRequest) -> Result<SwapQuote, String> {
        self.pick_venue(req).quote(req).await
    }

    pub async fn execute(&self, req: &SwapRequest) -> Result<SwapExecution, String> {
        self.pick_venue(req).execute(req).await
    }
}
