# B-epsilon Tree Storage Stack

This crate is the main event of the repository as it contains all algorithmic logic required in the actual implementation of the B-epsilon tree storage.
We give you a quick introduction into the structure of the repository here and give you starting points where to start your implementation.


## Building

To build the storage stack on it's own navigate to `betree/` and execute:

```sh
$ cargo build
```

This should build the storage stack in _Debug_ after a few minutes.

### Tests

We perform a number of tests which partially take some time to successfully
complete on your system. 

#### Internal

Navigate to `betree/` and execute:

```sh
$ cargo test
```

> Some of the unit tests take a considerable amount of time, which is still
> under investigation.

#### Integration

Due to the implementatio of tests a large amount of memory is taken up during
the integration tests affecting the remaining system considerably, please be
aware that the tests will consume several GiB of memory of available space

Navigate to `betree/tests/` and execute:

```sh
$ ./scripts/test.sh
```

> The provided script limits the usage of test threads proportionally to the
> available system memory with each test thread at maximum requiring 4 GiB in
> memory.

Additionally to the memory usage two files will be created with 2 GiB in size
each. They allow us to test some persistency guarantees our storage stack gives.

## Overview

![An overview of the different layers defined in the betree architecture](./assets/concept.svg)

### Database, Dataset and Object store

At the top of the overview, see above, we can see the _user interface_ part
of _Haura_. The `Database` provides the main introduction here for the rest of
the modules to be used. From an active `Database` we can then initiate a
`Dataset` or `Objectstore`. Also notable from the graphic, the `Database`
creates the `AllocationHandler` which is subsequently used in the
`DataManagement`. The `Database` also contains due to this also the storage
configuration which is then initiated in the `StoragePool`. The implementation
of the `Objectstore` is a wrapper around `Dataset` whereas the keys are chunk
ids and the value is there chunk content.

### Bε-tree

![Example Representation of an Bε-tree showing relevant structures and their functions](./assets/tree_semantics.svg)

The `Dataset` interacts mainly with the actual Bε-tree, which
receives through its root node messages from the `Dataset`. By default these
implement _insert_, _remove_ and _upsert_, although this can be exchanged if
required. Solely the `MessageAction` trait is required to be implemented on the
chosen type to allow for its use in the tree. An example for this can be seen in
the `MetaMessage` of the `Objectstore`.
Once passed, the tree propagates the message down the tree until it reaches a
leaf node where the message will be applied. Though this, might not happen
instantaneously and multiple buffers (`ChildBuffer`s) might be encountered which
momentarily hold the message at internal nodes. This way we avoid additional
deep traversals and might be able to flush multiple messages at once from one
buffer node.

Vital to understanding the handling and movement of nodes and their content
within _Haura_ is the object state cycle, this is illustrated at the leaf nodes
and in more detail in the following figure.

![State Diagram of the object lifecycle](./assets/object_lifecycle.svg)

Notable, is the additional state here labeled "On Disk", this is _strictly_
speaking not a state in the implementation of `ObjectRef` as-is but a state one
might differentiate two `ObjectRef`s by, as its simply denoting if the data has
to be read or is already present in the `Cache`.


![Constructed trees in an example of 3 open datasets](./assets/trees.svg)

Adjacent to the internals and construction of Bε-trees are the commonalities
between existing trees in an open database.  Mainly non-transparent to the user
another tree is opened to store internal information concerning the created
datasets (their `DatasetId`s and `ObjectPointer`s) and `Segment`s information.
`Segment`s are previously not mentioned here as they are considered in the Data
Management Layer, but can be thought of as containers organizing the allocation
bitmap for a range of blocks.  Additionally to avoid conflicts with another, all
trees share the same Data Management Unit to assure that no irregular state is
reached in handling critical on-disk management such as allocation of blocks and
updating of bitmaps.

### Data Management

On-disk operations and storage allocation are handled by the Data Management
layer. This layer also implements the copy-on-write semantics required for
snapshots, done in delayed deallocation and accounting of a dead-list of blocks.

Most important here is the `Dml` traits (`Dml`, `DmlBase`, `DmlWithHandler`,
`DmlWithCache`, `DmlWithSpl`) and the `Handler` trait. (`Cache` and `Spl` are
part of different modules and layers).

The `Handler` manages the actual bitmap handling for all allocations and
deallocations and is also responsible for tracking the number of blocks
distributed (Space Accounting).

To keep track of specific locations of allocated blocks, or free ranges of
blocks rather, bitmaps are used.  Wrapped around `SegmentAllocator`s, these can
be used to allocate block ranges at any position in a specific `SegmentId` or
request specific allocations at given offsets.

`SegementId`s refer to 1 GiB large ranges of blocks on a storage tier, though
the Id is unique over all storage tiers.

#### Copy on Write

The Data Management Layer is also responsible to handle copy on write
preservation.  This is handle by checking if any snapshots of the dataset
contain the afflicted node (via `Generation`), if this is the case the
`dead_list` contained in the root tree is updated, to contain the storage
location of the old version of the node on the next sync.

### Storage Pool

As the abstraction over specific hardware types and raid configurations the data
management unit interacts for all IO operation with the storage pool layer.
Notable here is the division of the layer into (of course) storage tiers, `Vdev`
and `LeafVdevs`.  There are 4 storage tiers available
(`FASTEST`,`FAST`,`SLOW`,`SLOWEST`) with each at maximum 1024 `Vdev`s.  Each
`Vdev` can be one of four variants. First, a singular `LeafVdev`, this is the
equivalent of a disk or any other file path backed interface, for example a
truncated file or a disk `dev/...`. Second, a RAID-1 like mirrored
configuration. Third, a RAID-5 like striping and parity based setup with
multiple disks. Fourth and last, a main memory backed buffer, simply hold as a
vector.

## Implementation

There are multiple modules with interweaving functionality
in `betree`, we will quickly name and describe them here.

| Name            | Description                                                                                                        |
|:----------------|:-------------------------------------------------------------------------------------------------------------------|
| cache           | Clock Cache implementation used internally                                                                         |
| compression     | Compression logic for indication and usage of compression algorithm (zstd only atm)                                |
| data_management | Allocation and Copy on Write logic for underlying storage space                                                    |
| database        | The Database layer & Dataset implementation with snapshots                                                         |
| metrics         | Basic Metric collections                                                                                           |
| object          | The object store wrapper around the dataset store                                                                  |
| storage\_pool   | The storage pool layer which manages different vdevs                                                               |
| tree            | The actual b-epsilon tree                                                                                          |
| vdev            | Implement the use of different devices for storage (Block, File, Memory) with different modes parity, raid, single |

> Note that traits are heavily used to allow interaction between objects of different modules, traits implemented by one module might be located in multiple other modules.

# Root Tree

The root tree is used in several places to store information relevant to the
functionality of the storage stack. This is done by defining a number of
messages with their own prefixes which we will explain shortly here.

| Prefix | Format                                     | Content             | Name                    | Function                                        |
|:-------|:-------------------------------------------|:--------------------|:------------------------|:------------------------------------------------|
| `00`   | `00`                                       | `<DatasetId>`       | DatasetId counter       | The last DatasetId used, stack unique.          |
| `00`   | `00 <SegementId>`                          | `<Bitmap>`          | Allocation bitmap       | The block bitmap for 1GiB storage segment.      |
| `01`   | `01 <Data set name>`                       | `<DatasetId>`       | Dataset name lookup     | Map name to unique DatasetId.                   |
| `02`   | `02 <DatasetId>`                           | `<DatasetData>`     | Dataset metadata        | Retrieve tree pointer and snapshot information. |
| `03`   | `03 <DatasetId> <Snapshot name>`           | `<SnapshotId>`      | Snapshot name lookup    | Map name to unique SnapshotId.                  |
| `04`   | `04 <DatasetId> <SnapshotId>`              | `<SnapshotData>`    | Snapshot metadata       | Retrieve tree pointer and snapshot information. |
| `05`   | `05 <DatasetId> <Generation> <DiskOffset>` | `<DeadListData>`    | Dead list               | Information about dead blocks (size and birth)  |
| `06`   | `06`                                       | `<ObjectStoreId>`   | ObjectStore counter     | The last used ObjectStoreId, stack unique.      |
| `06`   | `07 <Object store name>`                   | `<ObjectStoreId>`   | ObjectStore name lookup | Map name to unique ObjectStoreId.               |
| `07`   | `08 <ObjectStoreId>`                       | `<ObjectStoreData>` | ObjectStore metadata    | Retrieve DatsetIds of data and meta.            |


## Known bugs
- On large write operations (easy to achieve with `Objectstore`) which overfill the storage can return unexpected errors, this has been reduced by the introduction of space accounting but some errors might still occur as not all checks have been implemented yet.
- Not all tests finish successfully at the moment; both in internal and integration tests
  - Integration: Overwriting existent data fails due to semantics of the tree and intermediate message buffering in internal nodes which may overstep the actual available storage size
  - Internal: `parity1` tests fail, due for investigation