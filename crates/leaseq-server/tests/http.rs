//! The real server, driven over TCP by the real client and by raw requests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use leaseq_client::{
    Client, ClientError, Delivery, EnqueueRequest, JobsQuery, LeaseRequest, QueueName, RawValue,
    Status, Worker, WorkerConfig,
};
use leaseq_engine::core::RetryPolicy;
use leaseq_engine::{Config, Engine, SyncPolicy};
use leaseq_server::{Options, serve};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct TestServer {
    client: Client,
    address: std::net::SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
    engine: Engine,
    _dir: tempfile::TempDir,
}

async fn start() -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path());
    config.sync = SyncPolicy::Never;
    config.retry = RetryPolicy {
        base: Duration::from_millis(10),
        max: Duration::from_millis(10),
    };
    let engine = Engine::open(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(serve(
        listener,
        engine.clone(),
        Options::default(),
        stop.clone().cancelled_owned(),
    ));
    TestServer {
        client: Client::new(&format!("http://{address}")).unwrap(),
        address,
        stop,
        task,
        engine,
        _dir: dir,
    }
}

fn queue() -> QueueName {
    "jobs".parse().unwrap()
}

fn request(json: &str) -> EnqueueRequest {
    EnqueueRequest::new(RawValue::from_string(json.to_owned()).unwrap())
}

/// Sends hand-written HTTP and returns the whole reply.
async fn raw(server: &TestServer, head: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    let message = format!(
        "{head}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(message.as_bytes()).await.unwrap();
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await.unwrap();
    String::from_utf8_lossy(&reply).into_owned()
}

#[tokio::test]
async fn a_job_travels_through_the_api_unchanged() {
    let server = start().await;
    let payload = r#"{"big":12345678901234567890,"spaced" : [1,2]}"#;
    let id = server
        .client
        .enqueue(&queue(), &request(payload))
        .await
        .unwrap();

    let jobs = server
        .client
        .lease(&queue(), &LeaseRequest::default())
        .await
        .unwrap();
    assert_eq!((jobs[0].id, jobs[0].payload.get()), (id, payload));
    server.client.ack(id, jobs[0].token).await.unwrap();

    let again = server.client.ack(id, jobs[0].token).await.unwrap_err();
    assert!(again.is_lease_lost(), "{again}");
    assert_eq!(server.client.stats().await.unwrap()[0].completed, 1);
}

#[tokio::test]
async fn workers_retry_failures_and_dead_letter_poison_jobs() {
    let server = start().await;
    for n in 0..20 {
        let body = format!("{{\"n\":{n}}}");
        server
            .client
            .enqueue(&queue(), &request(&body))
            .await
            .unwrap();
    }
    let mut poison = request("\"poison\"");
    poison.max_attempts = 2;
    server.client.enqueue(&queue(), &poison).await.unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let handler = move |job: Delivery| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            if job.payload.get() == "\"poison\"" {
                Err("cannot digest".to_owned())
            } else {
                Ok(())
            }
        }
    };
    let mut config = WorkerConfig::new(queue());
    config.poll_wait = Duration::from_millis(100);
    let stop = CancellationToken::new();
    let worker =
        tokio::spawn(Worker::new(server.client.clone(), config, handler).run(stop.clone()));

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let stats = server.client.stats().await.unwrap();
            if stats[0].completed == 20 && stats[0].dead == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("queue did not drain");
    stop.cancel();
    worker.await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 22);

    let query = JobsQuery {
        state: Some(Status::Dead),
        ..JobsQuery::default()
    };
    let dead = server.client.jobs(&query).await.unwrap();
    assert_eq!(dead[0].last_error.as_deref(), Some("cannot digest"));
    server.client.retry(dead[0].id).await.unwrap();
}

#[tokio::test]
async fn heartbeats_keep_a_slow_job_leased() {
    let server = start().await;
    server
        .client
        .enqueue(&queue(), &request("1"))
        .await
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let handler = move |_| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            Ok(())
        }
    };
    let mut config = WorkerConfig::new(queue());
    config.lease = Duration::from_millis(600);
    config.poll_wait = Duration::from_millis(100);
    let stop = CancellationToken::new();
    let worker =
        tokio::spawn(Worker::new(server.client.clone(), config, handler).run(stop.clone()));

    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.cancel(); // shutdown must wait for the job in progress
    worker.await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(server.client.stats().await.unwrap()[0].completed, 1);
}

#[tokio::test]
async fn browser_based_attacks_are_blocked() {
    let server = start().await;
    let host = server.address.to_string();
    let post = |host: &str, extra: &str| {
        format!("POST /api/queues/jobs/jobs HTTP/1.1\r\nHost: {host}{extra}")
    };
    let body = r#"{"payload":1}"#;

    let rebinding = raw(
        &server,
        &post("attacker.example", "\r\nContent-Type: application/json"),
        body,
    )
    .await;
    assert!(rebinding.starts_with("HTTP/1.1 403"), "{rebinding}");

    let cross_site = raw(
        &server,
        &post(
            &host,
            "\r\nContent-Type: application/json\r\nOrigin: http://attacker.example",
        ),
        body,
    )
    .await;
    assert!(cross_site.starts_with("HTTP/1.1 403"), "{cross_site}");

    // A form post needs no preflight, so it must be refused for its content type.
    let form = raw(&server, &post(&host, "\r\nContent-Type: text/plain"), body).await;
    assert!(form.starts_with("HTTP/1.1 415"), "{form}");
    assert!(server.client.stats().await.unwrap().is_empty());

    let page = raw(&server, &format!("GET / HTTP/1.1\r\nHost: {host}"), "").await;
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    assert!(
        page.to_ascii_lowercase()
            .contains("content-security-policy: default-src 'self'")
    );
}

#[tokio::test]
async fn invalid_input_gets_a_structured_error() {
    let server = start().await;
    let mut huge = request(&format!("\"{}\"", "x".repeat(300 * 1024)));
    let error = server.client.enqueue(&queue(), &huge).await.unwrap_err();
    assert!(
        matches!(&error, ClientError::Api { code, .. } if code == "invalid_request"),
        "{error}"
    );

    huge = request("1");
    huge.max_attempts = 0;
    assert!(server.client.enqueue(&queue(), &huge).await.is_err());

    let host = server.address.to_string();
    let head = format!(
        "POST /api/queues/bad%20name/jobs HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json"
    );
    let reply = raw(&server, &head, r#"{"payload":1}"#).await;
    assert!(
        reply.starts_with("HTTP/1.1 400") && reply.contains("invalid_request"),
        "{reply}"
    );
}

#[tokio::test]
async fn shutdown_is_not_held_open_by_streams_or_long_polls() {
    let server = start().await;
    let mut events = TcpStream::connect(server.address).await.unwrap();
    let head = format!(
        "GET /api/events HTTP/1.1\r\nHost: {}\r\n\r\n",
        server.address
    );
    events.write_all(head.as_bytes()).await.unwrap();
    let mut first = [0; 1024];
    let read = events.read(&mut first).await.unwrap();
    assert!(String::from_utf8_lossy(&first[..read]).starts_with("HTTP/1.1 200"));

    let client = server.client.clone();
    let poll = tokio::spawn(async move {
        let request = LeaseRequest {
            wait_ms: 30_000,
            ..LeaseRequest::default()
        };
        client.lease(&queue(), &request).await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    server.stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), server.task)
        .await
        .expect("shutdown hung")
        .unwrap()
        .unwrap();
    assert!(poll.await.unwrap().unwrap().is_empty());
    server.engine.shutdown().await;
}

#[tokio::test]
async fn remote_listeners_must_be_opted_into() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(Config::new(dir.path())).unwrap();
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let error = serve(listener, engine, Options::default(), async {})
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
}

#[test]
fn listen_addresses_can_be_checked_before_anything_is_opened() {
    let allowed = |options: Options, address: &str| {
        options
            .check_address(address.parse().unwrap())
            .map_err(|error| error.kind())
    };
    let denied = Err(std::io::ErrorKind::PermissionDenied);
    assert_eq!(allowed(Options::default(), "127.0.0.1:1"), Ok(()));
    assert_eq!(allowed(Options::default(), "[::1]:1"), Ok(()));
    assert_eq!(allowed(Options::default(), "0.0.0.0:1"), denied);
    assert_eq!(allowed(Options { allow_remote: true }, "0.0.0.0:1"), Ok(()));
}
