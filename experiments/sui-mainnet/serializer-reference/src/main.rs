//! Test-only schema oracle: actual BCS/bincode serializers, without a Sui build.
//! Opaque canonical payloads are serialized as fixed tuples, not byte vectors.
use bincode::Options;
use serde::ser::SerializeTuple;
use serde::{Serialize, Serializer};
use serde_json::{json, Value};
use std::error::Error;
use std::io::{self, Read};

type Result<T, E = Box<dyn Error>> = std::result::Result<T, E>;

#[derive(Clone)]
struct Raw(Vec<u8>);
impl Serialize for Raw {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(self.0.len())?;
        for byte in &self.0 {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }
}

#[derive(Clone)]
struct Digest(Vec<u8>);
impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

#[allow(dead_code)]
#[derive(Serialize)]
enum StoreData {
    Move(Raw),
    Package(Raw),
    IndirectObjectDeprecated,
    Coin(u64),
}
#[derive(Serialize)]
struct Stored {
    data: StoreData,
    owner: Raw,
    previous_transaction: Digest,
    storage_rebate: u64,
}
#[derive(Serialize)]
enum StoreObject {
    Value(Stored),
    Deleted,
    Wrapped,
}
#[derive(Serialize)]
enum Wrapper {
    V1(StoreObject),
}
#[derive(Clone, Serialize)]
struct Signed {
    intent: [u8; 3],
    transaction_data: Raw,
    signatures: Vec<Vec<u8>>,
}
#[derive(Clone, Serialize)]
struct Trusted {
    data: Vec<Signed>,
    empty_authentication: (),
}
#[derive(Serialize)]
struct Execution {
    transaction: Trusted,
    effects: Raw,
}
#[derive(Serialize)]
struct FullV1 {
    transactions: Vec<Execution>,
    signatures: Vec<Vec<Vec<u8>>>,
}
#[derive(Serialize)]
struct FullV2 {
    transactions: Vec<Execution>,
    signatures: Vec<Vec<(Vec<u8>, Option<u64>)>>,
}
#[derive(Serialize)]
enum Full {
    V1(FullV1),
    V2(FullV2),
}
#[derive(Serialize)]
struct Quorum {
    epoch: u64,
    signature: Raw,
    signers_bitmap: Vec<u8>,
}
#[derive(Serialize)]
struct Certified {
    summary: Raw,
    quorum: Quorum,
}
#[derive(Serialize)]
enum Marker {
    Received,
    FastpathStreamEnded,
    ConsensusStreamEnded(Digest),
}

fn bytes(value: &Value) -> Result<Vec<u8>> {
    Ok(serde_json::from_value(value.clone())?)
}
fn array(value: &Value) -> Result<[u8; 32]> {
    Ok(bytes(value)?.try_into().map_err(|_| "expected 32 bytes")?)
}
fn integer(value: &Value) -> Result<u64> {
    value.as_u64().ok_or_else(|| "expected unsigned integer".into())
}
fn be<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(value)?)
}

fn main() -> Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let root: Value = serde_json::from_str(&input)?;
    let mut objects = Vec::new();
    for object in root["objects"].as_array().ok_or("objects missing")? {
        let raw_data = bytes(&object["data"])?;
        let data = if object["gas"] == true {
            StoreData::Coin(integer(&object["balance"])?)
        } else {
            match raw_data.first() {
                Some(0) => StoreData::Move(Raw(raw_data[1..].to_vec())),
                Some(1) => StoreData::Package(Raw(raw_data[1..].to_vec())),
                _ => return Err("unknown object data".into()),
            }
        };
        let stored = Wrapper::V1(StoreObject::Value(Stored {
            data,
            owner: Raw(bytes(&object["owner"])?),
            previous_transaction: Digest(bytes(&object["previous"])?),
            storage_rebate: integer(&object["rebate"])?,
        }));
        objects.push(json!({
            "key": be(&(array(&object["key"])?, integer(&object["version"]) ?))?,
            "value": bcs::to_bytes(&stored)?
        }));
    }
    let mut trusted_values = Vec::new();
    let mut executions = Vec::new();
    let mut plain_groups = Vec::new();
    let mut alias_groups = Vec::new();
    for transaction in root["transactions"].as_array().ok_or("transactions missing")? {
        let signatures: Vec<Vec<u8>> = bcs::from_bytes(&bytes(&transaction["signatures"])?)?;
        let trusted = Trusted {
            data: vec![Signed {
                intent: [0, 0, 0],
                transaction_data: Raw(bytes(&transaction["data"])?),
                signatures: signatures.clone(),
            }],
            empty_authentication: (),
        };
        trusted_values.push(bcs::to_bytes(&trusted)?);
        executions.push(Execution {
            transaction: trusted,
            effects: Raw(bytes(&transaction["effects"])?),
        });
        plain_groups.push(signatures);
        if root["contents_version"] == 1 {
            alias_groups.push(bcs::from_bytes::<Vec<(Vec<u8>, Option<u64>)>>(
                &bytes(&transaction["signature_group"])?,
            )?);
        }
    }
    let full = if root["contents_version"] == 0 {
        Full::V1(FullV1 { transactions: executions, signatures: plain_groups })
    } else if root["contents_version"] == 1 {
        Full::V2(FullV2 { transactions: executions, signatures: alias_groups })
    } else {
        return Err("unknown contents version".into());
    };
    let epoch = integer(&root["epoch"])?;
    let certified = Certified {
        summary: Raw(bytes(&root["summary"])?),
        quorum: Quorum {
            epoch,
            signature: Raw(bytes(&root["signature"])?),
            signers_bitmap: bytes(&root["bitmap"])?,
        },
    };
    let digest = Digest(bytes(&root["transaction_digest"])?);
    let oid = array(&root["object_id"])?;
    let version = integer(&root["object_version"])?;
    let start_version = integer(&root["start_version"])?;
    let encoded_digest = be(&digest)?;
    let output = json!({
        "objects": objects,
        "trusted_transactions": trusted_values,
        "full_contents": bcs::to_bytes(&full)?,
        "certified_checkpoint": bcs::to_bytes(&certified)?,
        "digest_key_before_prefix_removal": encoded_digest,
        "epoch_digest_key": be(&(epoch, &digest))?,
        "sequence_key": be(&integer(&root["sequence"])?)?,
        "unit_key": be(&())?,
        "unit_value": bcs::to_bytes(&())?,
        "effects_digest_value": bcs::to_bytes(&Digest(bytes(&root["effects_digest"])?))?,
        "fastpath_marker_key": be(&(epoch, oid, 0_u64, version))?,
        "consensus_marker_key": be(&(epoch | (1 << 63), oid, start_version, version))?,
        "deleted": bcs::to_bytes(&Wrapper::V1(StoreObject::Deleted))?,
        "wrapped": bcs::to_bytes(&Wrapper::V1(StoreObject::Wrapped))?,
        "received": bcs::to_bytes(&Marker::Received)?,
        "fastpath_ended": bcs::to_bytes(&Marker::FastpathStreamEnded)?,
        "consensus_ended": bcs::to_bytes(&Marker::ConsensusStreamEnded(digest))?,
    });
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
