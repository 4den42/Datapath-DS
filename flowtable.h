/*
 * flowtable.h - open-addressing hash table keyed on a 5-tuple.
 *
 * This is the shape of a connection/flow table in a packet path:
 * look up a flow by (src ip, dst ip, src port, dst port, protocol),
 * create it on first packet, update counters, delete on close/timeout.
 *
 * Design notes:
 *   - Open addressing with linear probing: entries live in one flat array,
 *     so a probe sequence walks adjacent memory (cache friendly), unlike
 *     chained buckets that chase pointers.
 *   - Capacity is a power of two; bucket = hash & mask.
 *   - The full hash is stored per entry. Probes compare the 32-bit hash
 *     first and only compare the full key on a hash match (DPDK rte_hash
 *     does the same with "signatures").
 *   - Deletion uses backward-shift instead of tombstones, so lookups never
 *     slow down from accumulated deleted markers.
 *   - Load factor is capped (default 75%) to keep probe lengths short.
 *
 * Not thread-safe: in a real data path each core usually owns its own
 * flow table shard (run-to-completion), so no locking is needed.
 */
#ifndef FLOWTABLE_H
#define FLOWTABLE_H

#include <stdbool.h>
#include <stdint.h>

struct flow_key {
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t src_port;
    uint16_t dst_port;
    uint8_t  proto;
    uint8_t  pad[3];   /* always zero, so the key is safe to hash/compare as bytes */
};

struct flow_stats {
    uint64_t packets;
    uint64_t bytes;
};

struct flow_entry {
    struct flow_key   key;
    struct flow_stats stats;
    uint32_t hash;
    uint8_t  used;
};

struct flowtable {
    struct flow_entry *entries;
    uint32_t capacity;     /* power of two */
    uint32_t mask;
    uint32_t count;
    uint32_t max_count;    /* capacity * load factor */
};

/* Build a zero-padded key. Use this rather than filling the struct by hand. */
struct flow_key flow_key_make(uint32_t src_ip, uint32_t dst_ip,
                              uint16_t src_port, uint16_t dst_port, uint8_t proto);

uint32_t flow_hash(const struct flow_key *k);

/* capacity must be a power of two. Returns false on bad args / OOM. */
bool flowtable_init(struct flowtable *t, uint32_t capacity);
void flowtable_destroy(struct flowtable *t);

/* Returns the stats for key, or NULL if absent. */
struct flow_stats *flowtable_lookup(struct flowtable *t, const struct flow_key *k);

/*
 * Returns the stats for key, inserting a zeroed entry if absent.
 * *created is set to true if a new entry was made. Returns NULL if the
 * table is at its load-factor limit and the key is not already present.
 */
struct flow_stats *flowtable_upsert(struct flowtable *t, const struct flow_key *k, bool *created);

/* Returns true if the key was present and removed. */
bool flowtable_delete(struct flowtable *t, const struct flow_key *k);

/* Average probe distance of stored entries (diagnostic). */
double flowtable_avg_probe(const struct flowtable *t);

#endif /* FLOWTABLE_H */
