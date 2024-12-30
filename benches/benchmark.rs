use criterion::{Criterion, black_box, criterion_group, criterion_main};
use pilgrimage::broker::{Broker, Node};
use std::sync::{Arc, Mutex};

fn broker_benchmark(c: &mut Criterion) {
    // Brokerの初期化
    let broker = Broker::new("broker1", 3, 2, "storage_path");

    // ノードの追加
    let node1 = Node {
        data: Arc::new(Mutex::new(Vec::new())),
    };
    let node2 = Node {
        data: Arc::new(Mutex::new(Vec::new())),
    };
    broker.add_node("node1".to_string(), node1);
    broker.add_node("node2".to_string(), node2);

    let mut group = c.benchmark_group("broker_benchmarks");

    group.bench_function("start_election", |b| {
        b.iter(|| {
            black_box(broker.start_election());
        })
    });

    group.bench_function("send_message", |b| {
        b.iter(|| {
            black_box(broker.send_message("test message".to_string()));
        })
    });

    group.bench_function("receive_message", |b| {
        b.iter(|| {
            // 受信前にメッセージを送信
            broker.send_message("benchmark message".to_string());
            black_box(broker.receive_message());
        })
    });

    group.finish();
}

criterion_group!(benches, broker_benchmark);
criterion_main!(benches);
