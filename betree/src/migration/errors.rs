#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    foreign_links {
        DmuError(crate::data_management::errors::Error);
        DbError(crate::database::errors::Error);
        Io(std::io::Error);
        SerializeJson(serde_json::Error);
    }
    errors {
        ConstructionFailed
        MigrationFailed
    }
}
