//! Rift as a venue backend: the REST client and its wire format. The venue
//! adapter that drives a leg through Rift lives under `finalizers`.
pub mod client;
mod protocol;

pub use client::RiftClient;

#[cfg(test)]
mod tests;
