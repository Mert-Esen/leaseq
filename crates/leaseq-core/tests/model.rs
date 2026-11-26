//! Behavioural tests and randomised checks of the state machine's invariants.

use std::time::Duration;

use leaseq_core::{
    Event, JobId, JobSpec, LeaseToken, Priority, QueueError, QueueName, RetryPolicy, State, Status,
    Timestamp,
};
use proptest::prelude::*;

const LEASE: Duration = Duration::from_millis(100);

fn policy() -> RetryPolicy {
    RetryPolicy {
        base: Duration::from_millis(10),
        max: Duration::from_millis(80),
    }
}

fn queue(name: &str) -> QueueName {
    name.parse().unwrap()
}

fn spec(priority: u8, max_attempts: u32) -> JobSpec {
    JobSpec {
        queue: queue("q"),
        payload: "{}".into(),
        priority: Priority::try_from(priority).unwrap(),
        max_attempts,
    }
}

fn at(millis: u64) -> Timestamp {
    Timestamp::from_millis(millis)
}

fn add(state: &mut State, spec: JobSpec, run_at: Timestamp) -> JobId {
    let event = state.decide_enqueue(spec, run_at);
    state.apply(&event).unwrap();
    let Event::Enqueued { id, .. } = event else {
        unreachable!()
    };
    id
}

fn lease(state: &mut State, now: u64, max: usize) -> Vec<(JobId, LeaseToken)> {
    state.promote(at(now));
    let events = state.decide_lease(&queue("q"), max, at(now).saturating_add(LEASE));
    events
        .iter()
        .map(|event| {
            state.apply(event).unwrap();
            let Event::Leased { id, token, .. } = event else {
                unreachable!()
            };
            (*id, *token)
        })
        .collect()
}

#[test]
fn leases_by_priority_then_age() {
    let mut state = State::default();
    let low = add(&mut state, spec(1, 1), at(0));
    let high_a = add(&mut state, spec(9, 1), at(0));
    let high_b = add(&mut state, spec(9, 1), at(0));
    let order: Vec<_> = lease(&mut state, 0, 10)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(order, [high_a, high_b, low]);
}

#[test]
fn delayed_jobs_wait_until_due() {
    let mut state = State::default();
    add(&mut state, spec(4, 1), at(50));
    assert!(lease(&mut state, 49, 1).is_empty());
    assert_eq!(state.next_deadline(), Some(at(50)));
    assert_eq!(lease(&mut state, 50, 1).len(), 1);
}

#[test]
fn a_stale_worker_cannot_ack_after_its_lease_moved_on() {
    let mut state = State::default();
    let id = add(&mut state, spec(4, 5), at(0));
    let (_, first) = lease(&mut state, 0, 1)[0];

    // The lease is already invalid at its expiry instant, before anyone sweeps it.
    assert_eq!(
        state.decide_ack(id, first, at(100)),
        Err(QueueError::LeaseLost(id))
    );

    for event in state.decide_expired(at(100), 10, &policy()) {
        state.apply(&event).unwrap();
    }
    let (_, second) = lease(&mut state, 500, 1)[0];
    assert!(second > first);
    assert_eq!(
        state.decide_ack(id, first, at(501)),
        Err(QueueError::LeaseLost(id))
    );
    assert!(state.decide_ack(id, second, at(501)).is_ok());
}

#[test]
fn exhausted_jobs_are_dead_lettered_and_can_be_revived() {
    let mut state = State::default();
    let id = add(&mut state, spec(4, 2), at(0));
    for round in 0..2 {
        let now = round * 1_000;
        let (_, token) = lease(&mut state, now, 1)[0];
        let event = state
            .decide_nack(id, token, "boom".into(), at(now), &policy())
            .unwrap();
        state.apply(&event).unwrap();
    }
    assert_eq!(state.job(id).unwrap().status, Status::Dead);
    assert_eq!(
        state.decide_revive(JobId(99), at(0)),
        Err(QueueError::NotFound(JobId(99)))
    );

    let event = state.decide_revive(id, at(5_000)).unwrap();
    state.apply(&event).unwrap();
    assert_eq!(state.job(id).unwrap().job.attempts, 0);
    assert_eq!(lease(&mut state, 5_000, 1).len(), 1);
}

#[test]
fn contradictory_events_are_rejected_without_changing_state() {
    let mut state = State::default();
    let id = add(&mut state, spec(4, 1), at(0));
    let before = state.clone();
    assert!(state.apply(&Event::Completed { id }).is_err());
    assert!(state.apply(&Event::Completed { id: JobId(42) }).is_err());
    assert_eq!(state, before);
}

#[derive(Debug, Clone)]
enum Op {
    Enqueue {
        priority: u8,
        attempts: u32,
        delay: u64,
    },
    Lease {
        max: usize,
    },
    Ack {
        pick: prop::sample::Index,
    },
    Nack {
        pick: prop::sample::Index,
    },
    Extend {
        pick: prop::sample::Index,
    },
    Revive {
        pick: prop::sample::Index,
    },
    Advance {
        millis: u64,
    },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u8..10, 1u32..4, 0u64..50).prop_map(|(priority, attempts, delay)| Op::Enqueue { priority, attempts, delay }),
        3 => (1usize..5).prop_map(|max| Op::Lease { max }),
        2 => any::<prop::sample::Index>().prop_map(|pick| Op::Ack { pick }),
        2 => any::<prop::sample::Index>().prop_map(|pick| Op::Nack { pick }),
        1 => any::<prop::sample::Index>().prop_map(|pick| Op::Extend { pick }),
        1 => any::<prop::sample::Index>().prop_map(|pick| Op::Revive { pick }),
        2 => (1u64..150).prop_map(|millis| Op::Advance { millis }),
    ]
}

proptest! {
    /// For any sequence of operations: replaying the recorded events or restoring
    /// a snapshot gives the live state, and no job is lost or duplicated.
    #[test]
    fn replay_and_snapshot_match_live_state(ops in prop::collection::vec(op(), 0..120)) {
        let mut live = State::default();
        let mut log: Vec<Event> = Vec::new();
        let mut held: Vec<(JobId, LeaseToken)> = Vec::new();
        let (mut now, mut enqueued, mut completed) = (0u64, 0u64, 0u64);

        for op in ops {
            live.promote(at(now));
            let expired = live.decide_expired(at(now), usize::MAX, &policy());
            for event in expired {
                live.apply(&event).unwrap();
                log.push(event);
            }
            live.promote(at(now));

            let events: Vec<Event> = match op {
                Op::Enqueue { priority, attempts, delay } => {
                    enqueued += 1;
                    vec![live.decide_enqueue(spec(priority, attempts), at(now + delay))]
                }
                Op::Lease { max } => live.decide_lease(&queue("q"), max, at(now).saturating_add(LEASE)),
                Op::Ack { pick } | Op::Nack { pick } | Op::Extend { pick } if held.is_empty() => {
                    let _ = pick;
                    vec![]
                }
                Op::Ack { pick } => {
                    let (id, token) = held[pick.index(held.len())];
                    live.decide_ack(id, token, at(now)).into_iter().collect()
                }
                Op::Nack { pick } => {
                    let (id, token) = held[pick.index(held.len())];
                    live.decide_nack(id, token, "no".into(), at(now), &policy()).into_iter().collect()
                }
                Op::Extend { pick } => {
                    let (id, token) = held[pick.index(held.len())];
                    live.decide_extend(id, token, at(now), at(now).saturating_add(LEASE)).into_iter().collect()
                }
                Op::Revive { pick } => {
                    let dead: Vec<_> = live.jobs().filter(|v| v.status == Status::Dead).map(|v| v.job.id).collect();
                    if dead.is_empty() {
                        vec![]
                    } else {
                        live.decide_revive(dead[pick.index(dead.len())], at(now)).into_iter().collect()
                    }
                }
                Op::Advance { millis } => {
                    now += millis;
                    vec![]
                }
            };

            for event in events {
                live.apply(&event).unwrap();
                match &event {
                    Event::Leased { id, token, .. } => held.push((*id, *token)),
                    Event::Completed { .. } => completed += 1,
                    _ => {}
                }
                log.push(event);
            }

            let stats = live.stats();
            let live_jobs: u64 = stats.iter().map(|s| s.ready + s.delayed + s.leased + s.dead).sum();
            prop_assert_eq!(live_jobs, live.jobs().count() as u64);
            prop_assert_eq!(live_jobs + completed, enqueued);
        }

        live.promote(at(now));
        let mut replayed = State::default();
        for event in &log {
            replayed.apply(event).unwrap();
        }
        replayed.promote(at(now));
        prop_assert_eq!(&replayed, &live);

        let json = serde_json::to_vec(&live.snapshot()).unwrap();
        let mut restored = State::restore(serde_json::from_slice(&json).unwrap()).unwrap();
        restored.promote(at(now));
        prop_assert_eq!(&restored, &live);
    }
}
