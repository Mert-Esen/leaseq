//! The `leaseq` command.

mod bench;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use leaseq_client::{
    Client, DEFAULT_MAX_ATTEMPTS, Delivery, EnqueueRequest, JobId, JobsQuery, Priority, QueueName,
    RawValue, Status, Worker, WorkerConfig,
};
use leaseq_engine::{Config, Engine, SyncPolicy};
use leaseq_server::Options;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(version, about = "A durable, lease-based job queue")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Args)]
struct Remote {
    /// Server to talk to.
    #[arg(long, default_value = "http://127.0.0.1:7878", global = true)]
    url: String,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server and dashboard.
    Serve {
        /// Data directory.
        #[arg(long, default_value = "data")]
        data: PathBuf,
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:7878")]
        listen: SocketAddr,
        /// Allow non-loopback access. There is no authentication.
        #[arg(long)]
        allow_remote: bool,
        /// Skip fsync: much faster, but recent jobs can be lost on power failure.
        #[arg(long)]
        no_fsync: bool,
    },
    /// Add a job.
    Enqueue {
        #[command(flatten)]
        remote: Remote,
        queue: QueueName,
        /// JSON payload.
        payload: String,
        /// 0 (lowest) to 9 (highest).
        #[arg(long, default_value_t = 4)]
        priority: u8,
        /// Wait this long before the job is ready.
        #[arg(long, default_value_t = 0)]
        delay_ms: u64,
        /// Attempts before dead-lettering.
        #[arg(long, default_value_t = DEFAULT_MAX_ATTEMPTS)]
        max_attempts: u32,
    },
    /// Run a demonstration worker. Jobs whose payload has `"fail": true` fail.
    Work {
        #[command(flatten)]
        remote: Remote,
        queue: QueueName,
        /// Jobs processed at once.
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        /// Simulated processing time.
        #[arg(long, default_value_t = 200)]
        work_ms: u64,
    },
    /// Show per-queue counters.
    Stats {
        #[command(flatten)]
        remote: Remote,
    },
    /// List live jobs.
    Jobs {
        #[command(flatten)]
        remote: Remote,
        #[arg(long)]
        queue: Option<QueueName>,
        #[arg(long)]
        state: Option<StateArg>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Return a dead job to its queue.
    Retry {
        #[command(flatten)]
        remote: Remote,
        id: u64,
    },
    /// Measure in-process engine throughput in a temporary directory.
    Bench(bench::BenchArgs),
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StateArg {
    Ready,
    Delayed,
    Leased,
    Dead,
}

impl From<StateArg> for Status {
    fn from(state: StateArg) -> Self {
        match state {
            StateArg::Ready => Self::Ready,
            StateArg::Delayed => Self::Delayed,
            StateArg::Leased => Self::Leased,
            StateArg::Dead => Self::Dead,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().command {
        Command::Serve {
            data,
            listen,
            allow_remote,
            no_fsync,
        } => serve(data, listen, Options { allow_remote }, no_fsync).await,
        Command::Enqueue {
            remote,
            queue,
            payload,
            priority,
            delay_ms,
            max_attempts,
        } => {
            let payload = RawValue::from_string(payload).context("payload is not valid JSON")?;
            let request = EnqueueRequest {
                priority: Priority::try_from(priority)?,
                delay_ms,
                max_attempts,
                ..EnqueueRequest::new(payload)
            };
            let id = Client::new(&remote.url)?.enqueue(&queue, &request).await?;
            println!("{id}");
            Ok(())
        }
        Command::Work {
            remote,
            queue,
            concurrency,
            work_ms,
        } => {
            work(
                &remote.url,
                queue,
                concurrency,
                Duration::from_millis(work_ms),
            )
            .await
        }
        Command::Stats { remote } => stats(&remote.url).await,
        Command::Jobs {
            remote,
            queue,
            state,
            limit,
        } => {
            let query = JobsQuery {
                queue,
                state: state.map(Status::from),
                limit: Some(limit),
            };
            for job in Client::new(&remote.url)?.jobs(&query).await? {
                println!(
                    "{:>8}  {:<20} {:<8} {}/{}  {}",
                    job.id,
                    job.queue,
                    format!("{:?}", job.state).to_lowercase(),
                    job.attempts,
                    job.max_attempts,
                    job.last_error.as_deref().unwrap_or("-"),
                );
            }
            Ok(())
        }
        Command::Retry { remote, id } => {
            Client::new(&remote.url)?.retry(JobId(id)).await?;
            Ok(())
        }
        Command::Bench(args) => bench::run(args).await,
    }
}

async fn serve(
    data: PathBuf,
    listen: SocketAddr,
    options: Options,
    no_fsync: bool,
) -> anyhow::Result<()> {
    // Refuse before touching the data directory or the network.
    options.check_address(listen)?;
    let mut config = Config::new(data);
    if no_fsync {
        config.sync = SyncPolicy::Never;
    }
    let engine = tokio::task::spawn_blocking(move || Engine::open(config)).await??;
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot listen on {listen}"))?;
    tracing::info!("dashboard: http://{}", listener.local_addr()?);
    if options.allow_remote {
        tracing::warn!("remote access is enabled and there is no authentication");
    }
    leaseq_server::serve(listener, engine.clone(), options, interrupted()).await?;
    engine.shutdown().await;
    tracing::info!("stopped");
    Ok(())
}

async fn work(
    url: &str,
    queue: QueueName,
    concurrency: usize,
    time: Duration,
) -> anyhow::Result<()> {
    let mut config = WorkerConfig::new(queue);
    config.concurrency = concurrency;
    let handler = move |job: Delivery| async move {
        tokio::time::sleep(time).await;
        let payload: serde_json::Value =
            serde_json::from_str(job.payload.get()).map_err(|e| e.to_string())?;
        if payload.get("fail") == Some(&serde_json::Value::Bool(true)) {
            return Err("the payload asked to fail".to_owned());
        }
        tracing::info!(id = %job.id, attempt = job.attempt, "processed");
        Ok(())
    };
    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        interrupted().await;
        tracing::info!("finishing jobs in progress");
        signal.cancel();
    });
    Worker::new(Client::new(url)?, config, handler)
        .run(shutdown)
        .await;
    Ok(())
}

async fn stats(url: &str) -> anyhow::Result<()> {
    println!(
        "{:<24} {:>8} {:>8} {:>8} {:>8} {:>10}",
        "queue", "ready", "delayed", "leased", "dead", "completed"
    );
    for q in Client::new(url)?.stats().await? {
        println!(
            "{:<24} {:>8} {:>8} {:>8} {:>8} {:>10}",
            q.name.as_str(),
            q.ready,
            q.delayed,
            q.leased,
            q.dead,
            q.completed
        );
    }
    Ok(())
}

/// Resolves on Ctrl-C, or SIGTERM on Unix.
async fn interrupted() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "cannot listen for ctrl-c");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => tokio::select! {
                () = ctrl_c => {}
                _ = terminate.recv() => {}
            },
            Err(error) => {
                tracing::error!(%error, "cannot listen for SIGTERM");
                ctrl_c.await;
            }
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    #[test]
    fn command_definition_is_consistent() {
        super::Cli::command().debug_assert();
    }
}
