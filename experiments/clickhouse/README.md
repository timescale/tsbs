Vanilla docker installation of clickhouse, no replication engine or anything like that added.

### Ingestion benchmarks

┌─table──────────────────────────┬─size───────┬─────rows─┬─latest_modification─┬─bytes_size─┬─engine────┬─primary_keys_size─┐
│ benchmark.cpu                  │ 1.35 GiB   │ 50000000 │ 2021-05-31 19:18:45 │ 1446079239 │ MergeTree │ 48.18 KiB         │
│ system.query_log               │ 23.18 MiB  │   277364 │ 2021-05-31 19:22:51 │   24305940 │ MergeTree │ 312.00 B          │
│ system.query_thread_log        │ 22.08 MiB  │   154522 │ 2021-05-31 19:22:51 │   23157671 │ MergeTree │ 210.00 B          │
│ system.metric_log              │ 2.59 MiB   │    19788 │ 2021-05-31 19:22:47 │    2720185 │ MergeTree │ 48.00 B           │
│ system.trace_log               │ 1.86 MiB   │    40470 │ 2021-05-31 19:18:18 │    1952100 │ MergeTree │ 60.00 B           │
│ benchmark.tags                 │ 567.87 KiB │    25000 │ 2021-05-31 19:15:40 │     581499 │ MergeTree │ 16.00 B           │
│ system.asynchronous_metric_log │ 50.22 KiB  │    18260 │ 2021-05-31 19:22:24 │      51422 │ MergeTree │ 72.00 B           │
└────────────────────────────────┴────────────┴──────────┴─────────────────────┴────────────┴───────────┴───────────────────┘
