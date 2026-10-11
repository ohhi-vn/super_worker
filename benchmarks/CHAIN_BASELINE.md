# Chain Baseline (pre-change)

Recorded before `improve-chain-perf-and-errors` changes. Machine: darwin, Erlang/OTP 27, Elixir 1.20.4.

## `mix run benchmarks/bench_main.exs`

| Row | Result |
|---|---|
| Chain worker message passing | 501,681 µs per iteration (±0.09%) |

This row includes a fixed `Process.sleep(500)` in the scenario, so it measures the sleep, not chain throughput. It is kept for comparison only.

## Focused measurements (`/tmp/chain_perf/measure.exs`, not in repo)

### Add-worker scaling (single-replica nodes, `Chain.add_worker/2`)

| N nodes | Total ms | Per add µs |
|---|---|---|
| 250 | 10.08 | 40.3 |
| 500 | 41.35 | 82.7 |
| 1000 | 180.65 | 180.7 |

Per-add cost grows linearly with N, so total add work is quadratic.

### Message throughput (3-node single-replica chain, queue_length 100)

| Messages | Total ms | Msgs/s |
|---|---|---|
| 20,000 | 980.1 | 20,407 |

## Test baseline

`test/standart_test/chain_test.exs`, `chain_messaging_test.exs`, `chain_config_test.exs`, `test/workload_test/chain_workers_test.exs`, `test/standart_test/function_chain_test.exs`: 112 tests, 112 passed in one run; 111 passed and 1 failed in an earlier run (flaky, not yet identified).

---

# Chain After (improve-chain-perf-and-errors)

## `mix run benchmarks/bench_main.exs`

| Row | Before | After |
|---|---|---|
| Chain worker message passing | 501,681 µs | 502,077 µs (sleep-dominated, unchanged) |

## Focused measurements (`/tmp/chain_perf/measure.exs`)

### Add-worker scaling

| N nodes | Before total ms | After total ms | After per add µs |
|---|---|---|---|
| 250 | 10.08 | 0.59 | 2.4 |
| 500 | 41.35 | 1.47 | 3.0 |
| 1000 | 180.65 | 3.55 | 3.6 |

Total add work is now close to linear in N. The stale-row scan in `Db.put_worker/5` was removed from the add path (`Db.put_new_worker/5`); restarts still use it.

### Message throughput (3-node chain, queue_length 100)

Five runs after the change: 18,627 / 24,098 / 20,131 / 22,421 / 22,098 msgs/s (median ~22,100). The single baseline run was 20,407 msgs/s. The spread between runs (~±13%) is larger than the difference from baseline, so no throughput regression is visible.
