#include "flowtable.h"

#include <stdlib.h>
#include <string.h>

#define LOAD_FACTOR_NUM 3
#define LOAD_FACTOR_DEN 4

struct flow_key flow_key_make(uint32_t src_ip, uint32_t dst_ip,
                              uint16_t src_port, uint16_t dst_port, uint8_t proto)
{
    struct flow_key k;
    memset(&k, 0, sizeof(k));
    k.src_ip = src_ip;
    k.dst_ip = dst_ip;
    k.src_port = src_port;
    k.dst_port = dst_port;
    k.proto = proto;
    return k;
}

/* Murmur3 finalizer-style mixing: cheap and avalanches well. */
static inline uint64_t mix64(uint64_t x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

uint32_t flow_hash(const struct flow_key *k)
{
    uint64_t a = ((uint64_t)k->src_ip << 32) | k->dst_ip;
    uint64_t b = ((uint64_t)k->src_port << 32) | ((uint64_t)k->dst_port << 16) | k->proto;
    uint64_t h = mix64(a ^ mix64(b + 0x9e3779b97f4a7c15ULL));
    return (uint32_t)(h ^ (h >> 32));
}

static inline bool key_eq(const struct flow_key *a, const struct flow_key *b)
{
    return memcmp(a, b, sizeof(*a)) == 0;
}

bool flowtable_init(struct flowtable *t, uint32_t capacity)
{
    if (capacity == 0 || (capacity & (capacity - 1)))
        return false;
    t->entries = calloc(capacity, sizeof(*t->entries));
    if (!t->entries)
        return false;
    t->capacity = capacity;
    t->mask = capacity - 1;
    t->count = 0;
    t->max_count = (uint32_t)((uint64_t)capacity * LOAD_FACTOR_NUM / LOAD_FACTOR_DEN);
    return true;
}

void flowtable_destroy(struct flowtable *t)
{
    free(t->entries);
    t->entries = NULL;
    t->capacity = t->mask = t->count = t->max_count = 0;
}

/*
 * Find the slot holding key, or the first empty slot in its probe run.
 * The load-factor cap guarantees an empty slot exists, so this terminates.
 */
static uint32_t find_slot(const struct flowtable *t, const struct flow_key *k, uint32_t h)
{
    uint32_t i = h & t->mask;
    for (;;) {
        const struct flow_entry *e = &t->entries[i];
        if (!e->used)
            return i;
        if (e->hash == h && key_eq(&e->key, k))
            return i;
        i = (i + 1) & t->mask;
    }
}

struct flow_stats *flowtable_lookup(struct flowtable *t, const struct flow_key *k)
{
    uint32_t h = flow_hash(k);
    struct flow_entry *e = &t->entries[find_slot(t, k, h)];
    return e->used ? &e->stats : NULL;
}

struct flow_stats *flowtable_upsert(struct flowtable *t, const struct flow_key *k, bool *created)
{
    uint32_t h = flow_hash(k);
    struct flow_entry *e = &t->entries[find_slot(t, k, h)];

    if (e->used) {
        if (created)
            *created = false;
        return &e->stats;
    }
    if (t->count >= t->max_count)
        return NULL;   /* full: caller decides (drop, evict, etc.) */

    e->key = *k;
    e->hash = h;
    e->used = 1;
    e->stats.packets = 0;
    e->stats.bytes = 0;
    t->count++;
    if (created)
        *created = true;
    return &e->stats;
}

/* Distance from an entry's home bucket to where it actually sits. */
static inline uint32_t probe_dist(const struct flowtable *t, uint32_t slot, uint32_t hash)
{
    return (slot - (hash & t->mask)) & t->mask;
}

bool flowtable_delete(struct flowtable *t, const struct flow_key *k)
{
    uint32_t h = flow_hash(k);
    uint32_t hole = find_slot(t, k, h);
    if (!t->entries[hole].used)
        return false;

    /*
     * Backward-shift: walk forward from the hole. Any entry whose home
     * bucket is at or before the hole (cyclically) can move back into it,
     * which keeps every remaining key reachable from its home bucket.
     * Stop at an empty slot.
     */
    uint32_t j = hole;
    for (;;) {
        j = (j + 1) & t->mask;
        struct flow_entry *e = &t->entries[j];
        if (!e->used)
            break;
        if (probe_dist(t, j, e->hash) >= ((j - hole) & t->mask)) {
            t->entries[hole] = *e;
            hole = j;
        }
    }
    memset(&t->entries[hole], 0, sizeof(t->entries[hole]));
    t->count--;
    return true;
}

double flowtable_avg_probe(const struct flowtable *t)
{
    if (t->count == 0)
        return 0.0;
    uint64_t total = 0;
    for (uint32_t i = 0; i < t->capacity; i++)
        if (t->entries[i].used)
            total += probe_dist(t, i, t->entries[i].hash);
    return (double)total / t->count;
}
