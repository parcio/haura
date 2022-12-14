#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    foreign_links {
        VdevError(crate::vdev::Error);
        StoragePoolError(crate::storage_pool::Error);
        TreeError(crate::tree::TreeError);
        SerializationError(::bincode::Error);
        ConfigurationError(crate::storage_pool::configuration::Error);
        Io(std::io::Error);
    }
    errors {
        Closed
        InvalidSuperblock
        DoesNotExist
        AlreadyExists
        InUse
        InDestruction
        MessageTooLarge
        SerializeFailed
        MigrationWouldExceedStorage
        MigrationNotPossible
        KeyContainsNullByte
    }
}
