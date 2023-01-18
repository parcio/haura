- Title: Pivot Keys
- Status: *DRAFT*

# Description

```ascii
        p1      p2      p3
┌───────┬───────┬───────┬───────┐
│       │       │       │       │
│ Left  │ Right │ Right │ Right │
│ Outer │       │       │       │
└───┬───┴───┬───┴───┬───┴───┬───┘
    │       │       │       │
    ▼       ▼       ▼       ▼
```

This RFC proposes a new identification for nodes within the B-epsilon tree. We
design it to be as uninstrusive as possible to the struture and current
implemenation of the tree to avoid undesired side effects later on. This
includes reducing the dependency on state as we show in the description here.
The basis of "PivotKey" are pivot elements already present in the tree. We use
an enum indicating the position of the node based on its parent.

```rust
type Pivot = CowBytes;

enum LocalPivotKey {
    LeftOuter(Pivot),
    Right(Pivot),
    Root,
}
```

The property we are piggy-backing on is that pivots are searchable and unique,
furthermore we can structurally define Pivot Key directions which keeps the
required memory space relatively low as only a single pivot key is required.
Finally, the pivot keys are more persistent than normal keys in the collection,
as they are only refreshed when the strcture of the tree changes. This might
happen on rebalancing. Although, when we consider this than any node based
algorithm needs to reconsider decisions anyway.

To make the pivot key ready to use over all datasets in a database (which can
have overlapping key sets) we require an additional information to direct the
pivot key to the correct dataset. This can be done by adding a `DatasetId` to
the key.

```rust
type Pivot = CowBytes;

enum PivotKey {
    LeftOuter(Pivot, DatasetId),
    Right(Pivot, DatasetId),
    Root(DatasetId),
}
```

Also, as the root node of a tree does not have a parent which could index the
node by its pivot - we add another variant which simply denotes that the root
of a given dataset is to be chosen.

We propose that we internally use two kinds of pivot keys. First, the global
`PivotKey`, which is structured as shown above including the `DatasetId`.
Second, the scoped `LocalPivotKey`, which can offer of us some advantages when
designing interfaces for node operations, which do not have the knowledge in
which tree they are located in. This alleviates the need for passing around
`DatasetIds` to layers which are normally unaffected by it. The transformation
from `LocalPivotKey` to `PivotKey` is always possible, as all local keys which a
tree layer will encounter belong to the tree itself, the reverse direction is
not given to be valid and should therefore be excluded in the implementation.

# Purpose

We can use the Pivot Key of a tree node to perform operations on specific nodes
which fulfill certain conditions such as access frequency or access probability.
This is helpful in a number of scenarios such as data prefetching or
disk-to-disk migrations.

Since pivots are stored in the internal nodes we are required to read a
substantial amount of data to retrieve knowledge about all exisiting pivot keys.
This limits the efficient usage to scenarios in which we retrieve pivot keys,
for example from messages emitted by the DMU, and record these as they are used
by the user. Previously a similar scheme has been done by the migration policies
which recorded disk offsets and set hints to the DMU to which tier a node is
advised to be written.
This limited hints to often accessed nodes which are likely to be migrated to
faster storage, as not often accessed nodes would not encounter the sent hints.
With Pivot Keys we could actively migrate them downwards, which is the main
advantage of Pivot Keys. This can be useful in scenarios with additional small
layers like NVRAM where we are then more flexible to migrate granular amounts of
data for better tier utilization.

# Drawbacks

To be fully usable in multiple layers the Pivot Key is required to be stored in
each node taking up (at the moment) an arbitrary amout of extra space depending
on the pivot element. The DML can then extract Pivot Keys from each node it acts
on and report critical actions such as fetch, write and delete. This results in
more code in alot of places of the `tree` and `data_management` modules as these
will have to be adjusted to accomodate for the extra encoded member.

# Alternatives

The search for alternatives is difficult as not many characteristics are present
which allow nodes to be identifiable *and* searchable. Something like Ids for
example could make nodes identifiable but from an Id we cannot search for the
node in the tree.

An alternative to the pivot method could be the path construction which relies
on the structural uniqueness of the tree, in which we construct a path from the
root by giving directions for each step downwards until the desired node is
located. It can look similar to this:

```rust
enum Path {
    Child(usize, Path),
    Terminate,
}
```

This method does not provide many advantages and carries the disadvantage of
having to maintain additional state when constructing keys about the just
traversed distance. Arguably, this is semantic-wise not complicated but many
methods will be affected by this change. Also, misidentification may become
possible as with reconstruction of subtrees paths may be shifted around. With
Pivot Keys these will result in a failed search indicating an outdated key.
Currently the only advantage this method would have to the Pivot Key method is
that the size can be expected to remain comparatively low, with 5 bytes for
each element in the search path. A restriction of key size as discussed in [the
corresponding issue](https://github.com/julea-io/haura/issues/12) could solve
this problem and is already in discussion.
