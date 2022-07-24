v0.2.2
======

Add
---

- AutoMigration interface and Message Design (WIP)
- AtomicStorageInfo



Change
------

- the free space tracking now includes an atomic value for the total space available
- Database interface reworked
        - previously even when specifying the explicit configuration of SyncMode it was not enacted expect when with_sync was called
        - for continuity `build` creates the same type of database as before, but `with_sync` has been removed and replaced with `build_threaded`, which allows for functions such as periodic syncing and auto migration to spawn their own thread


Fixes
-----

- Persistence for Space Accounting
