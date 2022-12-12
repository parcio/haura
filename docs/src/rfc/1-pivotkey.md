- Title: Pivot Keys
- Status: *DRAFT*

# Description

```ascii
        p1      p2      p3
┌───────┬───────┬───────┬───────┐
│       │       │       │       │
│ Outer │  Left │  Left │  Left │
│       │       │       │       │
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

enum PivotKey {
    Outer(Pivot),
    Left(Pivot),
}
```

The property we are piggy-backing on is that pivots are searchable and
unique, furthermore we can structurally define Pivot Key directions which keeps
the required memory space relatively low as only a single pivot key is required.
Finally, the pivot keys are more persistent than normal keys in the collection,
as they are only refreshed when the strcture of the tree changes. This might
happen on rebalancing. Although when we consider this than any node based
algorithm needs to reconsider decisions anyway.

# Purpose

We can use the Pivot Key of a tree node to perform operations on specific nodes
which fulfill certain conditions such as access frequency or access probabilty,
first to abstracted prefetching, second to perform abstracted migrations of
data.

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
that the size can be expected to remain comparatively low, with 5 bytes for each
element in the search path. A restriction of key size as discussed in
[Issue](https://github.com/julea-io/haura/issues/12) could solve this problem
and is already in discussion.
