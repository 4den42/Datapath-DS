/*
 * driver.c - tests, a mini packet pipeline, and benchmarks for ring.h and
 * flowtable.c.
 *
 *   ./driver            run everything
 *   ./driver test       unit + concurrency tests only
 *   ./driver pipeline   RX thread -> ring -> worker thread -> flow table
 *   ./driver bench      lock-free ring vs mutex ring throughput
 *   add --quick         smaller iteration counts (used for sanitizer builds)
 */
#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "flowtable.h"
#include "ring.h"

/* ------------------------------------------------------------------ */
/* Tiny test harness                                                   */
/* ------------------------------------------------------------------ */

static int g_failures;
static int g_checks;

#define CHECK(cond)                                                       \
    do {                                                                  \
        g_checks++;                                                       \
        if (!(cond)) {                                                    \
            g_failures++;                                                 \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                 \
    } while (0)

static void section(const char *name) { printf("\n== %s ==\n", name); }

static int g_quick;
static uint64_t scale(uint64_t n) { return g_quick ? n / 20 : n; }

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* xorshift64*: fast deterministic PRNG for test data. */
static inline uint64_t rng_next(uint64_t *s)
{
    uint64_t x = *s;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *s = x;
    return x * 0x2545F4914F6CDD1DULL;
}

#define U2P(x) ((void *)(uintptr_t)(x))
#define P2U(p) ((uintptr_t)(p))

/* ------------------------------------------------------------------ */
/* Ring: single-threaded tests                                         */
/* ------------------------------------------------------------------ */

static void test_ring_basic(void)
{
    section("ring: basic");

    CHECK(ring_create(0) == NULL);
    CHECK(ring_create(3) == NULL);
    CHECK(ring_create(100) == NULL);

    struct ring *r = ring_create(8);
    CHECK(r != NULL);
    CHECK(ring_empty(r));

    void *out;
    CHECK(!ring_dequeue(r, &out));            /* empty */

    for (uintptr_t i = 1; i <= 8; i++)
        CHECK(ring_enqueue(r, U2P(i)));
    CHECK(ring_full(r));
    CHECK(!ring_enqueue(r, U2P(99)));         /* full: uses every slot */

    for (uintptr_t i = 1; i <= 8; i++) {
        CHECK(ring_dequeue(r, &out));
        CHECK(P2U(out) == i);                 /* FIFO order */
    }
    CHECK(ring_empty(r));
    ring_free(r);
    printf("  create/full/empty/FIFO ok\n");
}

static void test_ring_wraparound(void)
{
    section("ring: wraparound");

    /* Tiny ring, many laps: exercises index masking and counter wrap. */
    struct ring *r = ring_create(4);
    uint64_t seed = 42, next_in = 0, next_out = 0;
    uint64_t ops = scale(2000000);
    int order_ok = 1;

    for (uint64_t i = 0; i < ops; i++) {
        if (rng_next(&seed) & 1) {
            if (ring_enqueue(r, U2P(next_in)))
                next_in++;
        } else {
            void *o;
            if (ring_dequeue(r, &o)) {
                if (P2U(o) != (uintptr_t)next_out)
                    order_ok = 0;
                next_out++;
            }
        }
        if (ring_count(r) != next_in - next_out)
            order_ok = 0;
    }
    CHECK(order_ok);
    printf("  %llu random ops on size-4 ring, %llu items passed through, order ok\n",
           (unsigned long long)ops, (unsigned long long)next_out);

    /* Force the 32-bit counters to wrap by starting them near UINT32_MAX. */
    atomic_store(&r->head, UINT32_MAX - 2);
    atomic_store(&r->tail, UINT32_MAX - 2);
    r->cached_head = r->cached_tail = UINT32_MAX - 2;
    for (uintptr_t i = 0; i < 4; i++)
        CHECK(ring_enqueue(r, U2P(i)));
    CHECK(ring_full(r));
    for (uintptr_t i = 0; i < 4; i++) {
        void *o;
        CHECK(ring_dequeue(r, &o) && P2U(o) == i);
    }
    CHECK(ring_empty(r));
    printf("  32-bit index overflow ok\n");
    ring_free(r);
}

static void test_ring_burst(void)
{
    section("ring: burst");

    struct ring *r = ring_create(16);
    void *in[32], *out[32];
    for (uintptr_t i = 0; i < 32; i++)
        in[i] = U2P(i + 100);

    CHECK(ring_enqueue_burst(r, in, 10) == 10);
    CHECK(ring_enqueue_burst(r, in + 10, 10) == 6);   /* partial: only 6 fit */
    CHECK(ring_enqueue_burst(r, in, 1) == 0);
    CHECK(ring_dequeue_burst(r, out, 32) == 16);      /* partial: only 16 there */
    int ok = 1;
    for (int i = 0; i < 16; i++)
        ok &= P2U(out[i]) == (uintptr_t)(i + 100);
    CHECK(ok);
    CHECK(ring_dequeue_burst(r, out, 4) == 0);
    ring_free(r);
    printf("  partial enqueue/dequeue bursts ok\n");
}

/* ------------------------------------------------------------------ */
/* Ring: two-thread stress test                                        */
/* ------------------------------------------------------------------ */

struct spsc_arg {
    struct ring *r;
    uint64_t n;
    int ok;
};

static void *spsc_producer(void *p)
{
    struct spsc_arg *a = p;
    uint64_t seed = 7, sent = 0;
    void *buf[64];
    while (sent < a->n) {
        uint32_t want = 1 + (uint32_t)(rng_next(&seed) % 64);
        if (want > a->n - sent)
            want = (uint32_t)(a->n - sent);
        for (uint32_t i = 0; i < want; i++)
            buf[i] = U2P(sent + i + 1);       /* never NULL */
        uint32_t got = ring_enqueue_burst(a->r, buf, want);
        sent += got;
        if (got == 0)
            sched_yield();
    }
    return NULL;
}

static void *spsc_consumer(void *p)
{
    struct spsc_arg *a = p;
    uint64_t seed = 11, expect = 1;
    void *buf[64];
    a->ok = 1;
    while (expect <= a->n) {
        uint32_t want = 1 + (uint32_t)(rng_next(&seed) % 64);
        uint32_t got = ring_dequeue_burst(a->r, buf, want);
        for (uint32_t i = 0; i < got; i++)
            if (P2U(buf[i]) != (uintptr_t)expect++)
                a->ok = 0;
        if (got == 0)
            sched_yield();
    }
    return NULL;
}

static void test_ring_spsc_threads(void)
{
    section("ring: producer/consumer threads");

    struct ring *r = ring_create(256);
    struct spsc_arg a = { .r = r, .n = scale(10000000) };
    pthread_t pt, ct;
    double t0 = now_sec();
    pthread_create(&ct, NULL, spsc_consumer, &a);
    pthread_create(&pt, NULL, spsc_producer, &a);
    pthread_join(pt, NULL);
    pthread_join(ct, NULL);
    double dt = now_sec() - t0;

    CHECK(a.ok);
    CHECK(ring_empty(r));
    printf("  %llu items, random burst sizes, strict order %s (%.2f s)\n",
           (unsigned long long)a.n, a.ok ? "verified" : "BROKEN", dt);
    ring_free(r);
}

/* ------------------------------------------------------------------ */
/* Flow table tests                                                    */
/* ------------------------------------------------------------------ */

static struct flow_key key_from_index(uint32_t i)
{
    /* 10.x.x.x -> 192.168.x.x, varied ports, TCP or UDP. */
    return flow_key_make(0x0a000000u | (i & 0xffffff),
                         0xc0a80000u | ((i * 7) & 0xffff),
                         (uint16_t)(1024 + (i % 60000)),
                         (uint16_t)((i & 1) ? 443 : 53),
                         (uint8_t)((i & 1) ? 6 : 17));
}

static void test_flowtable_basic(void)
{
    section("flowtable: basic");

    struct flowtable t;
    CHECK(!flowtable_init(&t, 100));          /* not a power of two */
    CHECK(flowtable_init(&t, 64));

    struct flow_key k = flow_key_make(0x0a000001, 0x0a000002, 1234, 80, 6);
    CHECK(flowtable_lookup(&t, &k) == NULL);

    bool created;
    struct flow_stats *s = flowtable_upsert(&t, &k, &created);
    CHECK(s && created);
    s->packets = 1;
    s->bytes = 1500;

    s = flowtable_upsert(&t, &k, &created);
    CHECK(s && !created && s->packets == 1);

    /* Same tuple, different protocol = different flow. */
    struct flow_key k_udp = flow_key_make(0x0a000001, 0x0a000002, 1234, 80, 17);
    CHECK(flowtable_lookup(&t, &k_udp) == NULL);

    /* Reverse direction = different flow (no direction normalization here). */
    struct flow_key k_rev = flow_key_make(0x0a000002, 0x0a000001, 80, 1234, 6);
    CHECK(flowtable_lookup(&t, &k_rev) == NULL);

    CHECK(flowtable_delete(&t, &k));
    CHECK(!flowtable_delete(&t, &k));
    CHECK(flowtable_lookup(&t, &k) == NULL);
    CHECK(t.count == 0);

    /* Load-factor cap: 64 * 3/4 = 48 entries max. */
    for (uint32_t i = 0; i < 48; i++) {
        struct flow_key ki = key_from_index(i);
        CHECK(flowtable_upsert(&t, &ki, NULL) != NULL);
    }
    struct flow_key extra = key_from_index(1000);
    CHECK(flowtable_upsert(&t, &extra, NULL) == NULL);   /* full: rejected */
    struct flow_key existing = key_from_index(5);
    CHECK(flowtable_upsert(&t, &existing, NULL) != NULL); /* existing still found */
    printf("  insert/lookup/delete, key distinctness, load cap ok "
           "(avg probe at 75%% load: %.2f)\n", flowtable_avg_probe(&t));
    flowtable_destroy(&t);
}

/*
 * Randomized differential test: run the same random insert/delete/lookup
 * sequence against the flow table and a trivially correct reference array,
 * then compare. A small table forces long probe runs and lots of
 * backward-shift deletes, which is where open-addressing bugs hide.
 */
static void test_flowtable_random(void)
{
    section("flowtable: randomized vs reference");

    enum { UNIVERSE = 3000 };
    struct flowtable t;
    flowtable_init(&t, 4096);                 /* max 3072 entries */

    static uint8_t present[UNIVERSE];
    static uint64_t ref_pkts[UNIVERSE];
    memset(present, 0, sizeof(present));
    memset(ref_pkts, 0, sizeof(ref_pkts));
    uint32_t ref_count = 0;

    uint64_t seed = 12345, ops = scale(3000000);
    int ok = 1;
    for (uint64_t op = 0; op < ops; op++) {
        uint32_t i = (uint32_t)(rng_next(&seed) % UNIVERSE);
        struct flow_key k = key_from_index(i);
        switch (rng_next(&seed) % 3) {
        case 0: { /* upsert + count a packet */
            bool created;
            struct flow_stats *s = flowtable_upsert(&t, &k, &created);
            if (!s || created != !present[i]) { ok = 0; break; }
            if (created) { present[i] = 1; ref_count++; }
            s->packets++;
            ref_pkts[i]++;
            break;
        }
        case 1: { /* delete */
            bool removed = flowtable_delete(&t, &k);
            if (removed != present[i]) ok = 0;
            if (removed) { present[i] = 0; ref_pkts[i] = 0; ref_count--; }
            break;
        }
        default: { /* lookup */
            struct flow_stats *s = flowtable_lookup(&t, &k);
            if ((s != NULL) != present[i]) ok = 0;
            if (s && s->packets != ref_pkts[i]) ok = 0;
            break;
        }
        }
        if (t.count != ref_count) ok = 0;
    }

    /* Final full sweep: every key agrees with the reference. */
    for (uint32_t i = 0; i < UNIVERSE; i++) {
        struct flow_key k = key_from_index(i);
        struct flow_stats *s = flowtable_lookup(&t, &k);
        if ((s != NULL) != present[i] || (s && s->packets != ref_pkts[i]))
            ok = 0;
    }
    CHECK(ok);
    printf("  %llu random ops, %u live flows, table matches reference: %s\n",
           (unsigned long long)ops, ref_count, ok ? "yes" : "NO");
    flowtable_destroy(&t);
}

/* Every key in the same home bucket: worst case for linear probing. */
static void test_flowtable_collisions(void)
{
    section("flowtable: forced collisions");

    struct flowtable t;
    flowtable_init(&t, 64);
    uint32_t mask = t.mask, want_bucket = 0, found = 0;
    struct flow_key keys[40];

    for (uint32_t i = 0; found < 40 && i < 1000000; i++) {
        struct flow_key k = key_from_index(i);
        if ((flow_hash(&k) & mask) == want_bucket)
            keys[found++] = k;
    }
    CHECK(found == 40);

    for (uint32_t i = 0; i < found; i++)
        CHECK(flowtable_upsert(&t, &keys[i], NULL) != NULL);

    /* Delete every other one from the middle of the run, then check the rest. */
    for (uint32_t i = 0; i < found; i += 2)
        CHECK(flowtable_delete(&t, &keys[i]));
    int ok = 1;
    for (uint32_t i = 0; i < found; i++)
        ok &= (flowtable_lookup(&t, &keys[i]) != NULL) == (i % 2 == 1);
    CHECK(ok);
    CHECK(t.count == found / 2);
    printf("  40 keys in one bucket, 20 deleted mid-run, survivors all reachable: %s\n",
           ok ? "yes" : "NO");
    flowtable_destroy(&t);
}

/* ------------------------------------------------------------------ */
/* Mini packet pipeline                                                */
/* ------------------------------------------------------------------ */
/*
 *   RX thread                         worker thread
 *   ---------                         -------------
 *   take empty pkt from free_ring     dequeue burst from rx_ring
 *   fill in 5-tuple + length          flow = upsert(5-tuple)
 *   enqueue burst to rx_ring   --->   flow.packets++, flow.bytes += len
 *                                     on FIN: record totals, delete flow
 *                              <---   return pkt to free_ring
 *
 * Packets are preallocated and recycled through free_ring, the way NIC
 * drivers recycle mbufs through a mempool. Each ring has exactly one
 * producer and one consumer, so the SPSC ring is safe for both.
 */

#define BURST      32
#define POOL_SIZE  4096
#define NUM_FLOWS  10000
#define FLAG_FIN   0x01

struct pkt {
    struct flow_key key;
    uint32_t len;
    uint32_t flow_idx;   /* test metadata only, for verification */
    uint8_t  flags;
};

struct pipeline {
    struct ring *rx_ring;
    struct ring *free_ring;
    uint64_t total_pkts;
    _Atomic int rx_done;

    /* Written only by RX thread. */
    struct flow_stats expected[NUM_FLOWS];
    /* Written only by worker thread. */
    struct flowtable table;
    struct flow_stats closed[NUM_FLOWS];
    uint64_t fins, table_full_drops;
};

static void *rx_thread(void *p)
{
    struct pipeline *pl = p;
    uint64_t seed = 99, sent = 0;
    void *burst[BURST];

    while (sent < pl->total_pkts) {
        uint32_t want = BURST;
        if (want > pl->total_pkts - sent)
            want = (uint32_t)(pl->total_pkts - sent);
        uint32_t n = ring_dequeue_burst(pl->free_ring, burst, want);
        if (n == 0) { sched_yield(); continue; }

        for (uint32_t i = 0; i < n; i++) {
            struct pkt *pk = burst[i];
            uint64_t r = rng_next(&seed);
            /* Skewed traffic: half the packets hit the hottest 1% of flows. */
            uint32_t idx = (r & 1) ? (uint32_t)((r >> 8) % (NUM_FLOWS / 100))
                                   : (uint32_t)((r >> 8) % NUM_FLOWS);
            pk->key = key_from_index(idx);
            pk->len = 64 + (uint32_t)((r >> 40) % 1437);   /* 64..1500 bytes */
            pk->flow_idx = idx;
            pk->flags = ((r >> 56) % 200 == 0) ? FLAG_FIN : 0;  /* ~0.5% FINs */
            pl->expected[idx].packets++;
            pl->expected[idx].bytes += pk->len;
        }

        uint32_t off = 0;
        while (off < n) {
            uint32_t q = ring_enqueue_burst(pl->rx_ring, burst + off, n - off);
            off += q;
            if (q == 0)
                sched_yield();
        }
        sent += n;
    }
    atomic_store_explicit(&pl->rx_done, 1, memory_order_release);
    return NULL;
}

static void *worker_thread(void *p)
{
    struct pipeline *pl = p;
    void *burst[BURST];

    for (;;) {
        uint32_t n = ring_dequeue_burst(pl->rx_ring, burst, BURST);
        if (n == 0) {
            /* Check done flag, then re-check the ring to avoid a lost burst. */
            if (atomic_load_explicit(&pl->rx_done, memory_order_acquire) &&
                ring_dequeue_burst(pl->rx_ring, burst, BURST) == 0)
                break;
            sched_yield();
            continue;
        }
        for (uint32_t i = 0; i < n; i++) {
            struct pkt *pk = burst[i];
            struct flow_stats *s = flowtable_upsert(&pl->table, &pk->key, NULL);
            if (!s) { pl->table_full_drops++; continue; }
            s->packets++;
            s->bytes += pk->len;
            if (pk->flags & FLAG_FIN) {
                pl->closed[pk->flow_idx].packets += s->packets;
                pl->closed[pk->flow_idx].bytes += s->bytes;
                flowtable_delete(&pl->table, &pk->key);
                pl->fins++;
            }
        }
        uint32_t off = 0;
        while (off < n) {
            uint32_t q = ring_enqueue_burst(pl->free_ring, burst + off, n - off);
            off += q;
            if (q == 0)
                sched_yield();
        }
    }
    return NULL;
}

static void run_pipeline(void)
{
    section("pipeline: RX -> ring -> worker -> flow table");

    struct pipeline *pl = calloc(1, sizeof(*pl));
    struct pkt *pool = calloc(POOL_SIZE, sizeof(*pool));
    pl->rx_ring = ring_create(1024);
    pl->free_ring = ring_create(POOL_SIZE);
    pl->total_pkts = scale(5000000);
    flowtable_init(&pl->table, 16384);

    /* Fill the free ring before threads start (thread creation orders this). */
    for (uint32_t i = 0; i < POOL_SIZE; i++)
        ring_enqueue(pl->free_ring, &pool[i]);

    pthread_t rx, wk;
    double t0 = now_sec();
    pthread_create(&wk, NULL, worker_thread, pl);
    pthread_create(&rx, NULL, rx_thread, pl);
    pthread_join(rx, NULL);
    pthread_join(wk, NULL);
    double dt = now_sec() - t0;

    /* Verify: for every flow, closed totals + live entry == what RX sent. */
    uint64_t sum_pkts = 0, sum_bytes = 0, live = 0;
    int ok = 1;
    for (uint32_t i = 0; i < NUM_FLOWS; i++) {
        struct flow_key k = key_from_index(i);
        struct flow_stats *s = flowtable_lookup(&pl->table, &k);
        uint64_t p = pl->closed[i].packets + (s ? s->packets : 0);
        uint64_t b = pl->closed[i].bytes + (s ? s->bytes : 0);
        if (s) live++;
        if (p != pl->expected[i].packets || b != pl->expected[i].bytes)
            ok = 0;
        sum_pkts += p;
        sum_bytes += b;
    }
    CHECK(pl->table_full_drops == 0);
    CHECK(ok);
    CHECK(sum_pkts == pl->total_pkts);
    CHECK(live == pl->table.count);
    CHECK(ring_count(pl->free_ring) == POOL_SIZE);   /* every packet came home */

    printf("  packets:        %llu (%.2f Mpps, %.2f s)\n",
           (unsigned long long)sum_pkts, sum_pkts / dt / 1e6, dt);
    printf("  bytes:          %llu\n", (unsigned long long)sum_bytes);
    printf("  FINs (deletes): %llu\n", (unsigned long long)pl->fins);
    printf("  live flows:     %llu, avg probe distance %.2f\n",
           (unsigned long long)live, flowtable_avg_probe(&pl->table));
    printf("  per-flow counters match RX: %s, pool fully recycled: %s\n",
           ok ? "yes" : "NO",
           ring_count(pl->free_ring) == POOL_SIZE ? "yes" : "NO");

    flowtable_destroy(&pl->table);
    ring_free(pl->rx_ring);
    ring_free(pl->free_ring);
    free(pool);
    free(pl);
}

/* ------------------------------------------------------------------ */
/* Benchmark: lock-free ring vs mutex-protected ring                   */
/* ------------------------------------------------------------------ */

struct mutex_ring {
    pthread_mutex_t lock;
    uint32_t head, tail, mask, size;
    void **slots;
};

static struct mutex_ring *mring_create(uint32_t size)
{
    struct mutex_ring *m = calloc(1, sizeof(*m));
    pthread_mutex_init(&m->lock, NULL);
    m->size = size;
    m->mask = size - 1;
    m->slots = calloc(size, sizeof(void *));
    return m;
}

static void mring_free(struct mutex_ring *m)
{
    pthread_mutex_destroy(&m->lock);
    free(m->slots);
    free(m);
}

static uint32_t mring_enqueue_burst(struct mutex_ring *m, void *const *o, uint32_t n)
{
    pthread_mutex_lock(&m->lock);
    uint32_t free_slots = m->size - (m->head - m->tail);
    if (n > free_slots) n = free_slots;
    for (uint32_t i = 0; i < n; i++)
        m->slots[(m->head + i) & m->mask] = o[i];
    m->head += n;
    pthread_mutex_unlock(&m->lock);
    return n;
}

static uint32_t mring_dequeue_burst(struct mutex_ring *m, void **o, uint32_t n)
{
    pthread_mutex_lock(&m->lock);
    uint32_t avail = m->head - m->tail;
    if (n > avail) n = avail;
    for (uint32_t i = 0; i < n; i++)
        o[i] = m->slots[(m->tail + i) & m->mask];
    m->tail += n;
    pthread_mutex_unlock(&m->lock);
    return n;
}

struct bench_arg {
    void *ring;
    int is_mutex;
    uint32_t burst;
    uint64_t n;
};

static void *bench_producer(void *p)
{
    struct bench_arg *a = p;
    void *buf[64];
    for (uint32_t i = 0; i < 64; i++) buf[i] = U2P(i + 1);
    uint64_t sent = 0;
    while (sent < a->n) {
        uint32_t want = a->burst;
        if (want > a->n - sent) want = (uint32_t)(a->n - sent);
        uint32_t got = a->is_mutex ? mring_enqueue_burst(a->ring, buf, want)
                                   : ring_enqueue_burst(a->ring, buf, want);
        sent += got;
        if (!got) sched_yield();
    }
    return NULL;
}

static void *bench_consumer(void *p)
{
    struct bench_arg *a = p;
    void *buf[64];
    uint64_t got_total = 0;
    while (got_total < a->n) {
        uint32_t got = a->is_mutex ? mring_dequeue_burst(a->ring, buf, a->burst)
                                   : ring_dequeue_burst(a->ring, buf, a->burst);
        got_total += got;
        if (!got) sched_yield();
    }
    return NULL;
}

static double bench_one(int is_mutex, uint32_t burst, uint64_t n)
{
    void *r = is_mutex ? (void *)mring_create(1024) : (void *)ring_create(1024);
    struct bench_arg a = { r, is_mutex, burst, n };
    pthread_t pt, ct;
    double t0 = now_sec();
    pthread_create(&ct, NULL, bench_consumer, &a);
    pthread_create(&pt, NULL, bench_producer, &a);
    pthread_join(pt, NULL);
    pthread_join(ct, NULL);
    double dt = now_sec() - t0;
    if (is_mutex) mring_free(r); else ring_free(r);
    return n / dt / 1e6;
}

static void run_bench(void)
{
    section("bench: items/sec through a 1024-slot ring, 2 threads");

    uint64_t n = scale(20000000);
    uint32_t bursts[] = { 1, 8, 32 };
    printf("  %-7s %16s %16s %9s\n", "burst", "lock-free (M/s)", "mutex (M/s)", "speedup");
    for (size_t i = 0; i < sizeof(bursts) / sizeof(bursts[0]); i++) {
        double lf = bench_one(0, bursts[i], n);
        double mx = bench_one(1, bursts[i], n);
        printf("  %-7u %16.1f %16.1f %8.1fx\n", bursts[i], lf, mx, lf / mx);
    }

    section("bench: flow table lookups");
    struct flowtable t;
    flowtable_init(&t, 1 << 17);                 /* 131072 slots */
    uint32_t nflows = 90000;                     /* ~69% load */
    for (uint32_t i = 0; i < nflows; i++) {
        struct flow_key k = key_from_index(i);
        flowtable_upsert(&t, &k, NULL);
    }
    uint64_t seed = 5, hits = 0, lookups = scale(20000000);
    double t0 = now_sec();
    for (uint64_t i = 0; i < lookups; i++) {
        /* ~90% hits, ~10% misses. */
        struct flow_key k = key_from_index((uint32_t)(rng_next(&seed) % 100000));
        hits += flowtable_lookup(&t, &k) != NULL;
    }
    double dt = now_sec() - t0;
    printf("  %u flows at %.0f%% load, avg probe %.2f\n",
           nflows, 100.0 * nflows / t.capacity, flowtable_avg_probe(&t));
    printf("  %.1f M lookups/s (%.1f ns each, %.1f%% hits)\n",
           lookups / dt / 1e6, dt / lookups * 1e9, 100.0 * hits / lookups);
    flowtable_destroy(&t);
}

/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    const char *mode = "all";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--quick") == 0) g_quick = 1;
        else mode = argv[i];
    }
    int all = strcmp(mode, "all") == 0;

    if (!all && strcmp(mode, "test") && strcmp(mode, "pipeline") && strcmp(mode, "bench")) {
        fprintf(stderr, "usage: %s [all|test|pipeline|bench] [--quick]\n", argv[0]);
        return 2;
    }

    if (all || !strcmp(mode, "test")) {
        test_ring_basic();
        test_ring_wraparound();
        test_ring_burst();
        test_ring_spsc_threads();
        test_flowtable_basic();
        test_flowtable_random();
        test_flowtable_collisions();
    }
    if (all || !strcmp(mode, "pipeline"))
        run_pipeline();
    if (all || !strcmp(mode, "bench"))
        run_bench();

    printf("\n%d checks, %d failures\n", g_checks, g_failures);
    return g_failures ? 1 : 0;
}
