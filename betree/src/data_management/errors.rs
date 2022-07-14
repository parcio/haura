#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        VdevError(crate::vdev::Error);
    }
    links {
        Compression(crate::compression::Error, crate::compression::ErrorKind);
    }
    errors {
        DecompressionError
        DeserializationError
        SerializationError
        HandlerError
        CannotWriteBackError
        OutOfSpaceError
        CallbackError
    }
}
