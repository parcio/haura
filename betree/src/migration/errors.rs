#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    foreign_links {
        DmuError(crate::data_management::errors::Error);
    }
    errors {
        ConstructionFailed
        MigrationFailed
    }
}
