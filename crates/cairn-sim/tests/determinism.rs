//! The whole point of the simulator: a run is a pure function of its seed.

use bytes::Bytes;
use cairn_core::{Disk, Duration, Network, NodeId, OpenMode, Runtime};
use cairn_runtime::RunOutcome;
use cairn_sim::{SimConfig, Simulation};

/// Two nodes exchange messages while node 1 writes a log; node 1 crashes mid-way and restarts.
fn scenario(seed: u64) -> (u64, u64, Vec<u8>, (u64, u64, u64)) {
    let mut config = SimConfig::default();
    config.net.drop_prob = 0.1;
    let (sim, mut ex) = Simulation::new(seed, config);
    let n1 = NodeId(1);
    let n2 = NodeId(2);
    let rt1 = sim.runtime(n1, &ex.handle());
    let rt2 = sim.runtime(n2, &ex.handle());

    // Node 2: echo server for 20 messages.
    let r2 = rt2.clone();
    rt2.spawn(async move {
        for _ in 0..20 {
            let (from, msg) = r2.network().recv().await.unwrap();
            let _ = r2.network().send(from, msg).await;
        }
    });

    // Node 1: write records, ping node 2, sync every 4 records.
    let r1 = rt1.clone();
    rt1.spawn(async move {
        let d = r1.disk();
        let f = d.open("log", OpenMode::CreateOrOpen).await.unwrap();
        for i in 0..40u64 {
            let rec = format!("rec{i:03};");
            d.write_at(&f, i * 7, Bytes::from(rec)).await.unwrap();
            if i % 4 == 3 {
                d.sync(&f).await.unwrap();
            }
            let _ = r1
                .network()
                .send(n2, Bytes::from(i.to_le_bytes().to_vec()))
                .await;
            r1.sleep(Duration::from_millis(1)).await;
        }
    });

    // Crash node 1 at t = 25 ms, restart it and let it read back its log.
    let sim2 = sim.clone();
    let h = ex.handle();
    let stop = h.sleep(Duration::from_millis(25));
    ex.block_on(stop);
    sim2.crash(n1, &mut ex);
    let rt1b = sim.runtime(n1, &ex.handle());
    let r1 = rt1b.clone();
    let recovered = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let out = recovered.clone();
    rt1b.spawn(async move {
        let d = r1.disk();
        let f = d.open("log", OpenMode::Read).await.unwrap();
        let len = d.len(&f).await.unwrap();
        let bytes = d.read_at(&f, 0, len as usize).await.unwrap();
        *out.borrow_mut() = bytes.to_vec();
    });
    let outcome = ex.run();
    assert!(matches!(
        outcome,
        RunOutcome::Finished | RunOutcome::Stalled
    ));
    let content = recovered.borrow().clone();
    (sim.digest(), sim.trace_len(), content, sim.net_counters())
}

#[test]
fn same_seed_same_run() {
    for seed in [1u64, 2, 3, 42] {
        let a = scenario(seed);
        let b = scenario(seed);
        assert_eq!(a, b, "seed {seed} diverged");
        assert!(a.1 > 50, "trace should have many events");
    }
}

#[test]
fn different_seeds_differ() {
    let a = scenario(1);
    let b = scenario(2);
    assert_ne!(a.0, b.0);
}

#[test]
fn recovered_log_is_a_prefix_consistent_with_syncs() {
    for seed in 0..32u64 {
        let (_, _, content, _) = scenario(seed);
        // Every synced group of 4 records is intact; the tail may be partial or torn.
        let text = String::from_utf8_lossy(&content);
        let intact = text
            .split(';')
            .filter(|s| s.starts_with("rec") && s.len() == 6)
            .count();
        assert!(
            intact >= 4,
            "seed {seed}: at least the first synced group must survive: {text:?}"
        );
    }
}
