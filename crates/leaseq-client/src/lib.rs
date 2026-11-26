//! Talk to a leaseq server, and run workers against it.
//!
//! ```no_run
//! use leaseq_client::{Client, Delivery, Worker, WorkerConfig};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::new("http://127.0.0.1:7878")?;
//! let config = WorkerConfig::new("mail".parse()?);
//! let shutdown = tokio_util::sync::CancellationToken::new();
//!
//! Worker::new(client, config, |job: Delivery| async move {
//!     println!("{}", job.payload.get());
//!     Ok(())
//! })
//! .run(shutdown)
//! .await;
//! # Ok(()) }
//! ```

mod client;
mod worker;

pub use client::{Client, ClientError};
pub use leaseq_proto::*;
pub use worker::{Handler, Worker, WorkerConfig};
