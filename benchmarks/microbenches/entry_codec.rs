use criterion::{black_box, criterion_group, criterion_main, Criterion};
use helixdb::Entry;
use rand::{thread_rng, Rng};

fn do_benchmark(key_size: usize, value_size: usize, c: &mut Criterion) {
    let mut rng = thread_rng();
    let key = (0..key_size).map(|_| rng.gen()).collect();
    let value = (0..value_size).map(|_| rng.gen()).collect();
    let entry = Entry {
        timestamp: 1234423,
        key,
        value,
    };

    c.bench_function(
        &format!("encode {}B / {}B entry", key_size, value_size),
        |b| b.iter(|| entry.encode()),
    );
    let bytes = entry.encode();
    c.bench_function(
        &format!("decode {}B / {}B entry", key_size, value_size),
        |b| b.iter(|| Entry::decode(&bytes)),
    );
}

fn entry_codec_benchmark(c: &mut Criterion) {
    do_benchmark(64, 8, c);
    do_benchmark(64, 32, c);
    do_benchmark(64, 1024, c);
    do_benchmark(64, 4096, c);
}

fn fibonacci(n: u64) -> u64 {
    let mut a = 0;
    let mut b = 1;

    match n {
        0 => b,
        _ => {
            for _ in 0..n {
                let c = a + b;
                a = b;
                b = c;
            }
            b
        }
    }
}

fn some_bench(c: &mut Criterion) {
    c.bench_function("fib 20", |b| b.iter(|| fibonacci(black_box(20))));
}

criterion_group!(benches, entry_codec_benchmark, some_bench);
criterion_main!(benches);
