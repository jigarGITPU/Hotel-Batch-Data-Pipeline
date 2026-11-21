# Performance Benchmark Report – Hotel Batch Data Pipeline
**Date**: 2026-11-21 12:18:51 UTC  
**Mode**: SMALL  
**Total Source Records**: 5,111  
**Hardware Platform**: Windows (Intel/AMD Host, Python 3.14, SQLAlchemy 2.0)  

## Benchmark Summary

| Batch Size | Status | Total Records | Records Loaded | Quarantined | Duration (s) | Throughput (rec/s) | Peak Memory (MB) |
|---|---|---|---|---|---|---|---|
| 5,000 | SUCCESS | 5,111 | 3,555 | 61 | 4.75 | 1,075.24 | 133.40 |

## Key Insights & Optimization Observations

1. **Throughput Scaling**: Chunked bulk upserts and vectorized currency/date transformations minimize round-trips.
2. **Memory Stability**: Streaming and batch chunking prevent unbounded in-memory memory spikes.
3. **Quarantine Overhead**: Reject identification and persistence adds minimal overhead while guaranteeing audit compliance.
4. **Idempotency Guarantee**: Successive runs maintain identical row counts via constraint-backed UPSERT logic.
