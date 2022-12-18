# Known Bugs

- On large write operations (easy to achieve with `Objectstore`) which overfill
  the storage, unexpected errors can be returned. This has been reduced by the
  introduction of space accounting but some errors might still occur as not all
  checks have been implemented yet.
- Some tests are expected to fail in the current state of *Haura*. These tests
  are marked as `should_panic` in the `cargo test` output. These tests are
  largely concerned with the storage space utilization and test extreme
  situations where no storage space is left on the available devices.
