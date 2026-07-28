use std::{collections::HashSet, sync::Arc};

use futures::future::join_all;

use super::{MultiVenueAdapter, VenueRoutePreview};
use crate::swappers::model::SwapRequest;

#[derive(Clone)]
pub(super) struct VenueRegistry {
    adapters: Vec<Arc<dyn MultiVenueAdapter>>,
}

impl VenueRegistry {
    // Registration order is retained because it gives quote collection and
    // later execution planning a deterministic venue order.
    pub(super) fn new(adapters: Vec<Arc<dyn MultiVenueAdapter>>) -> Result<Self, String> {
        if adapters.is_empty() {
            return Err("venue registry must contain at least one adapter".to_string());
        }

        let mut venue_ids = HashSet::new();
        for adapter in &adapters {
            let venue_id = adapter.venue_id();
            if venue_id.is_empty() {
                return Err("venue adapter has an empty venue ID".to_string());
            }
            if !venue_ids.insert(venue_id) {
                return Err(format!("duplicate venue adapter `{venue_id}`"));
            }
            adapter
                .validate_configuration()
                .map_err(|error| format!("venue adapter `{venue_id}` is not configured: {error}"))?;
        }

        Ok(Self { adapters })
    }

    pub(super) fn contains(&self, venue_id: &str) -> bool {
        self.adapters.iter().any(|adapter| adapter.venue_id() == venue_id)
    }

    pub(super) fn adapter(&self, venue_id: &str) -> Option<&dyn MultiVenueAdapter> {
        self.adapters
            .iter()
            .find(|adapter| adapter.venue_id() == venue_id)
            .map(AsRef::as_ref)
    }

    pub(super) fn venue_ids(&self) -> Vec<String> {
        self.adapters
            .iter()
            .map(|adapter| adapter.venue_id().to_string())
            .collect()
    }

    // Starts every registered preview before awaiting the combined result.
    // `join_all` preserves input order even when venues finish out of order.
    #[cfg(test)]
    pub(super) async fn preview_all<F>(&self, request_for: F) -> VenueQuoteBook
    where
        F: Fn(&str) -> SwapRequest,
    {
        let previews = self.adapters.iter().map(|adapter| {
            let venue_id = adapter.venue_id().to_string();
            let request = request_for(&venue_id);
            async move {
                let outcome = match adapter.preview(&request).await {
                    Ok(preview) => VenuePreviewOutcome::Quoted(preview),
                    Err(error) => VenuePreviewOutcome::Unavailable(error),
                };
                VenuePreview {
                    venue_id,
                    request,
                    outcome,
                }
            }
        });

        VenueQuoteBook {
            previews: join_all(previews).await,
        }
    }

    // Quotes a strategy-selected subset concurrently while preserving the
    // order supplied by strategy configuration.
    pub(super) async fn preview_venues<F>(&self, venue_ids: &[String], request_for: F) -> Result<VenueQuoteBook, String>
    where
        F: Fn(&str) -> SwapRequest,
    {
        let mut adapters = Vec::with_capacity(venue_ids.len());
        for venue_id in venue_ids {
            let adapter = self
                .adapters
                .iter()
                .find(|adapter| adapter.venue_id() == venue_id)
                .ok_or_else(|| format!("venue adapter `{venue_id}` is not registered"))?;
            adapters.push(adapter);
        }

        let previews = adapters.into_iter().map(|adapter| {
            let venue_id = adapter.venue_id().to_string();
            let request = request_for(&venue_id);
            async move {
                let outcome = match adapter.preview(&request).await {
                    Ok(preview) => VenuePreviewOutcome::Quoted(preview),
                    Err(error) => VenuePreviewOutcome::Unavailable(error),
                };
                VenuePreview {
                    venue_id,
                    request,
                    outcome,
                }
            }
        });

        Ok(VenueQuoteBook {
            previews: join_all(previews).await,
        })
    }
}

pub(super) struct VenueQuoteBook {
    previews: Vec<VenuePreview>,
}

impl VenueQuoteBook {
    // Iteration follows deterministic registry/configuration order.
    pub(super) fn iter(&self) -> impl Iterator<Item = &VenuePreview> {
        self.previews.iter()
    }

    pub(super) fn get(&self, venue_id: &str) -> Option<&VenuePreview> {
        self.previews.iter().find(|preview| preview.venue_id == venue_id)
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = &mut VenuePreview> {
        self.previews.iter_mut()
    }
}

pub(super) struct VenuePreview {
    pub(super) venue_id: String,
    pub(super) request: SwapRequest,
    pub(super) outcome: VenuePreviewOutcome,
}

pub(super) enum VenuePreviewOutcome {
    // The adapter returned a response; strategy validation happens afterward.
    Quoted(VenueRoutePreview),
    // The adapter could not produce a response, for example during an outage.
    Unavailable(String),
    // A response was returned but failed validation and cannot be allocated.
    Invalid(String),
}
