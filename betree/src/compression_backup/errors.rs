#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }
    foreign_links {
        Io(::std::io::Error);
    }

    skip_msg_variant
}
