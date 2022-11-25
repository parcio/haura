# Known Bugs

- On large write operations (easy to achieve with `Objectstore`) which overfill the storage can return unexpected errors, this has been reduced by the introduction of space accounting but some errors might still occur as not all checks have been implemented yet.
- Not all tests finish successfully at the moment; both in internal and integration tests
  - Integration: Overwriting existent data fails due to semantics of the tree and intermediate message buffering in internal nodes which may overstep the actual available storage size
