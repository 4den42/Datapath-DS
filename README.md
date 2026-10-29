# datapath-ds

A lock-free ring buffer and a 5-tuple flow table in C11, with a driver that
tests them, runs them together as a mini packet pipeline, and benchmarks them.

## Files

| File | What it is |
|---|---|
| `ring.h` | Header-only single-producer/single-consumer lock-free ring (C11 atomics) |
| `flowtable.h`, `flowtable.c` | Open-addressing hash table keyed on (src IP, dst IP, src port, dst port, proto) |
| `driver.c` | Unit tests, two-thread stress test, packet pipeline demo, benchmarks |
| `Makefile` | Normal, AddressSanitizer, and ThreadSanitizer builds |

## Build and run

Needs a C11 compiler and pthreads (Linux, macOS, or WSL).

```sh
make run          # build and run everything
make check        # also run under ASan/UBSan and TSan
./driver test     # tests only
./driver pipeline # packet pipeline only
./driver bench    # benchmarks only
```

## Ring buffer design

- **Power-of-two size**, so the slot is `index & mask`, not `index % size`.
- **Free-running 32-bit head/tail counters.** `head - tail` is the fill level
  even after the counters overflow, and every slot is usable (no "waste one
  slot to tell full from empty" trick). The test forces the overflow.
- **Acquire/release ordering.** The producer writes slots, then publishes
  `head` with a release store. The consumer reads `head` with an acquire load,
  so it is guaranteed to see those slot writes. Same in reverse for `tail`.
- **Cached opposite index.** Each side keeps a private copy of the other side's
  index and only re-reads the shared atomic when the copy says full/empty. On
  the fast path the producer never touches the consumer's cache line.
- **Separate cache lines** for producer and consumer fields, avoiding false
  sharing.
- **Burst enqueue/dequeue**, one index publish per burst, like
  `rte_ring_enqueue_burst`.

## Flow table design

- **Open addressing with linear probing** in one flat array: probes walk
  adjacent memory instead of chasing linked-list pointers.
- **Stored hash per entry** checked before the full key compare (DPDK's
  `rte_hash` calls these signatures).
- **Backward-shift deletion** instead of tombstones, so heavy insert/delete
  churn (connections opening and closing) never degrades lookups.
- **75% load-factor cap.** Insert returns NULL when full so the caller decides
  what to do (drop, evict), the way a real connection table must.
- **Not thread-safe on purpose.** In run-to-completion data paths each core
  owns its own table shard, so the hot path needs no locks.

## The pipeline demo

```
RX thread                          worker thread
take empty pkt from free_ring      dequeue burst from rx_ring
fill 5-tuple + length      ---->   upsert flow, add packets/bytes
enqueue burst to rx_ring           on FIN: record totals, delete flow
                           <----   return pkt to free_ring
```

Packets are preallocated and recycled through a second ring, the way NIC
drivers recycle mbufs through a mempool. Traffic is skewed (half of packets go
to the hottest 1% of flows) and ~0.5% are FINs that delete their flow. At the
end every flow's counters are checked against what RX sent, and every packet
must be back in the pool.

## Sample results (2-core cloud VM)

```
pipeline:  5,000,000 packets, 29.7 Mpps, per-flow counters match: yes

burst    lock-free (M/s)      mutex (M/s)   speedup
1                   91.4              6.4     14.3x
8                  225.5             18.9     12.0x
32                 282.5             18.1     15.6x

flow table: 90,000 flows at 69% load, avg probe 1.09, ~99 ns/lookup
```

The lookup time is dominated by cache misses: the table is ~5 MB and lookups
are random, so most of them go to main memory. Shrinking `struct flow_entry`,
or prefetching the next packet's bucket while processing the current one
(as DPDK's bulk lookup does), are the next optimizations to try.

## Ideas for extending it

- Multi-producer/multi-consumer ring using compare-and-swap on head/tail
  (DPDK's MP/MC mode).
- Bulk flow lookup with `__builtin_prefetch` across a burst.
- Flow aging: timestamp each entry and expire idle flows.
- Bidirectional flows: normalize the 5-tuple so both directions hit one entry.
- Robin Hood probing to cap worst-case probe length.
- Pin threads to cores with `pthread_setaffinity_np` and compare numbers.
