//! Serialization utilities of a node pointer type.
use crate::tree::imp::RwLock;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<N, S>(np: &RwLock<N>, serializer: S) -> Result<S::Ok, S::Error>
where
    N: Serialize,
    S: Serializer,
{
    np.read().serialize(serializer)
}

pub fn deserialize<'de, N, D>(deserializer: D) -> Result<RwLock<N>, D::Error>
where
    N: Deserialize<'de>,
    D: Deserializer<'de>,
{
    N::deserialize(deserializer).map(RwLock::new)
}
