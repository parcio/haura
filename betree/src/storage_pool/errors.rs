#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    foreign_links {
        Io(std::io::Error);
    }
}
