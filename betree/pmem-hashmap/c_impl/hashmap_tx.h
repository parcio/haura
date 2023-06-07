/* SPDX-License-Identifier: BSD-3-Clause */
/* Copyright 2015-2020, Intel Corporation */
#ifndef HASHMAP_TX_H
#define HASHMAP_TX_H

#include "./hashmap.h"
#include <libpmemobj.h>
#include <libpmemobj/types.h>
#include <stddef.h>
#include <stdint.h>

#ifndef HASHMAP_TX_TYPE_OFFSET
#define HASHMAP_TX_TYPE_OFFSET 1004
#endif

struct hashmap_tx;
TOID_DECLARE(struct hashmap_tx, HASHMAP_TX_TYPE_OFFSET + 0);

struct root {
  TOID(struct hashmap_tx) map;
};

struct map_value {
  uint64_t len;
  uint8_t buf[];
};

POBJ_LAYOUT_BEGIN(hashmap_tx);
POBJ_LAYOUT_ROOT(hashmap_tx, struct root);
POBJ_LAYOUT_TOID(hashmap_tx, struct map_value);
POBJ_LAYOUT_END(hashmap_tx);

int hm_tx_check(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap);
int hm_tx_create(PMEMobjpool *pop, TOID(struct hashmap_tx) * map, void *arg);
int hm_tx_init(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap);
int hm_tx_insert(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
                 uint64_t hash, PMEMoid value);
PMEMoid hm_tx_remove(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
                     uint64_t hash);
PMEMoid hm_tx_get(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
                  uint64_t hash);
int hm_tx_lookup(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
                 uint64_t hash);
int hm_tx_foreach(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
                  int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg);
size_t hm_tx_count(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap);
int hm_tx_cmd(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, unsigned cmd,
              uint64_t arg);
struct map_value *access_map_value(PMEMoid oid);
struct root *access_root(PMEMoid oid);
int root_needs_init(struct root *);

#endif /* HASHMAP_TX_H */
