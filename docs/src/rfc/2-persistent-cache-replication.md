- Title: Persistent Cache / Data Replication
- Status: DRAFT

# Description

A persistent cache is a way to utilize a storage device capabilities without
integrating it further into the existing storage hierarchy by keeping it
essentially in the memory layer semantically. For this we present two
approaches, either may be used as they have different drawbacks and advantages.

## Fallback Cache

Once entries are evicted from the volatile cache we remove them by eviction and
allocate resources on the underlying storage media. To prolong this process we
skip this part and write them first to the non-volatile cache and eventually
when they are evicted from there they actually get written to disk. Problems
with this approach are mostly the actual utilization (read + write traffic)
which is not optimal for some storage devices (*cough* PMem).

Copy-on-Write is another issue as we may need to ensure writes to the storage
device when modifying the entry, but this can be taken care of in the DMU.

Regardless, it is easy to implement and should remain an option we try.

## Read-heavy-cache

Another approach which promises overall a better specific device utilization is
the focus on *read* heavy data which seldomly gets modified. Most of the issues
mentioned above are offloaded are resolved by this approach by it does depend on
a *decision maker* which moves data from storage or cache to the device. Most
issues in this approach are produced by this decision, with the actual result
being completely depending on this. An idea is to use the migration policies as
done with inter-tier data migration, but for this additional classification is
required. Possible workflow node independent classification schemes are still in
need to be constructed for this, which is partially an open topic for research.

# Purpose

This RFC tries to avoid to integrate stuff like PMem into the storage hierarchy
as non-latency bound workflows do not really gain an advantage with them, rather
a separate stack is build in the cache level to reduce latency on accesses there
(if the data fits into storage).

# Drawbacks



# Alternatives

> Find atleast one alternative, this thought process helps preventing being
> stuck in certain implemenation details
