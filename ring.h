/*
 * ring.h - lock-free single-producer / single-consumer ring buffer.
 *
 * Design notes (modeled on DPDK rte_ring and Linux kfifo):
 *   - Capacity is a power of two, so slot = index & mask (no modulo).
 *   - head and tail are free-running 32-bit counters; unsigned wraparound
 *     makes (head - tail) the fill level even after they overflow.
 *   - Producer owns head, consumer owns tail. Each side publishes its index
 *     with a release store and reads the other side's with an acquire load,
 *     so slot contents written before the publish are visible after it.
 *   - Each side keeps a cached copy of the other side's index and only
 *     re-reads the shared atomic when the cache says "full"/"empty". That
 *     cuts cross-core cache-line traffic on the hot path.
 *   - Producer and consumer fields live on separate cache lines to avoid
 *     false sharing.
 *   - Bulk enqueue/dequeue: packet paths move bursts (e.g. 32 mbufs), and
 *     one index publish per burst is much cheaper than one per element.
 *
 * Elements are void* (like rte_ring); store pointers to packets/objects.
 */
#ifndef RING_H
#define RING_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define CACHE_LINE 64

/*
 * Portable cache-aligned allocation. C11 aligned_alloc isn't available in
 * the Windows C runtime (MSVC or MinGW), which has _aligned_malloc instead.
 * Memory from _aligned_malloc must be released with _aligned_free.
 */
#if defined(_WIN32)
#include <malloc.h>
#define ring_aligned_alloc(align, size) _aligned_malloc((size), (align))
#define ring_aligned_free(p)            _aligned_free(p)
#else
#define ring_aligned_alloc(align, size) aligned_alloc((align), (size))
#define ring_aligned_free(p)            free(p)
#endif

struct ring {
    /* Producer side: written by producer, read by consumer. */
    _Alignas(CACHE_LINE) _Atomic uint32_t head;
    uint32_t cached_tail;          /* producer's view of tail */

    /* Consumer side: written by consumer, read by producer. */
    _Alignas(CACHE_LINE) _Atomic uint32_t tail;
    uint32_t cached_head;          /* consumer's view of head */

    /* Read-only after creation. */
    _Alignas(CACHE_LINE) uint32_t size;
    uint32_t mask;
    void *slots[];                 /* flexible array, size entries */
};

static inline bool ring_is_pow2(uint32_t n) { return n && !(n & (n - 1)); }

/* Returns NULL if size is not a power of two or allocation fails. */
static inline struct ring *ring_create(uint32_t size)
{
    if (!ring_is_pow2(size) || size > (1u << 30))
        return NULL;
    size_t bytes = sizeof(struct ring) + (size_t)size * sizeof(void *);
    bytes = (bytes + CACHE_LINE - 1) & ~(size_t)(CACHE_LINE - 1);
    struct ring *r = ring_aligned_alloc(CACHE_LINE, bytes);
    if (!r)
        return NULL;
    atomic_init(&r->head, 0);
    atomic_init(&r->tail, 0);
    r->cached_tail = 0;
    r->cached_head = 0;
    r->size = size;
    r->mask = size - 1;
    return r;
}

static inline void ring_free(struct ring *r) { ring_aligned_free(r); }

/*
 * Producer: enqueue up to n objects. Returns the number enqueued (0..n).
 * Burst semantics: enqueues as many as fit, like rte_ring_enqueue_burst.
 */
static inline uint32_t ring_enqueue_burst(struct ring *r, void *const *objs, uint32_t n)
{
    uint32_t head = atomic_load_explicit(&r->head, memory_order_relaxed);
    uint32_t free_slots = r->size - (head - r->cached_tail);

    if (free_slots < n) {
        /* Cache says not enough room: refresh from the real tail. */
        r->cached_tail = atomic_load_explicit(&r->tail, memory_order_acquire);
        free_slots = r->size - (head - r->cached_tail);
        if (n > free_slots)
            n = free_slots;
        if (n == 0)
            return 0;
    }

    for (uint32_t i = 0; i < n; i++)
        r->slots[(head + i) & r->mask] = objs[i];

    /* Release: slot writes above become visible before the new head. */
    atomic_store_explicit(&r->head, head + n, memory_order_release);
    return n;
}

/* Consumer: dequeue up to n objects. Returns the number dequeued (0..n). */
static inline uint32_t ring_dequeue_burst(struct ring *r, void **objs, uint32_t n)
{
    uint32_t tail = atomic_load_explicit(&r->tail, memory_order_relaxed);
    uint32_t avail = r->cached_head - tail;

    if (avail < n) {
        /* Acquire pairs with the producer's release on head. */
        r->cached_head = atomic_load_explicit(&r->head, memory_order_acquire);
        avail = r->cached_head - tail;
        if (n > avail)
            n = avail;
        if (n == 0)
            return 0;
    }

    for (uint32_t i = 0; i < n; i++)
        objs[i] = r->slots[(tail + i) & r->mask];

    /* Release: our slot reads finish before the producer may reuse slots. */
    atomic_store_explicit(&r->tail, tail + n, memory_order_release);
    return n;
}

static inline bool ring_enqueue(struct ring *r, void *obj)
{
    return ring_enqueue_burst(r, &obj, 1) == 1;
}

static inline bool ring_dequeue(struct ring *r, void **obj)
{
    return ring_dequeue_burst(r, obj, 1) == 1;
}

/* Approximate when called concurrently; exact when the ring is quiescent. */
static inline uint32_t ring_count(const struct ring *r)
{
    uint32_t h = atomic_load_explicit(&((struct ring *)r)->head, memory_order_acquire);
    uint32_t t = atomic_load_explicit(&((struct ring *)r)->tail, memory_order_acquire);
    return h - t;
}

static inline bool ring_empty(const struct ring *r) { return ring_count(r) == 0; }
static inline bool ring_full(const struct ring *r) { return ring_count(r) == r->size; }

#endif /* RING_H */
