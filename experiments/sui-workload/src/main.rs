use std::io::{self, BufRead, Write};
use sui_sdk_types::{
    CheckpointSummary, Object, Transaction, TransactionEffects, TransactionEvents,
};

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd hex length".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}
fn convert<T: serde::de::DeserializeOwned + serde::Serialize>(
    data: &[u8],
) -> Result<serde_json::Value, String> {
    let value: T = bcs::from_bytes(data).map_err(|e| e.to_string())?;
    serde_json::to_value(value).map_err(|e| e.to_string())
}
fn process(line: &str) -> Result<serde_json::Value, String> {
    let (kind, hex) = line.split_once('\t').ok_or("missing separator")?;
    let data = decode_hex(hex)?;
    match kind {
        "summary" => convert::<CheckpointSummary>(&data),
        "object" => convert::<Object>(&data),
        "events" => {
            let events: TransactionEvents = bcs::from_bytes(&data).map_err(|e| e.to_string())?;
            let sizes: Vec<usize> = events
                .0
                .iter()
                .map(bcs::serialized_size)
                .collect::<Result<_, _>>()
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"events": events, "sizes": sizes}))
        }
        "effects" => convert::<TransactionEffects>(&data),
        "transaction" => {
            let transaction: Transaction = bcs::from_bytes(&data).map_err(|e| e.to_string())?;
            Ok(
                serde_json::json!({"digest": transaction.digest().to_string(), "value": transaction}),
            )
        }
        _ => Err("unknown type".into()),
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut out = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let result = process(&line?).map_err(io::Error::other)?;
        serde_json::to_writer(&mut out, &result)?;
        writeln!(out)?;
        out.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_and_bcs_boundaries() {
        assert_eq!(decode_hex("00ff").ok(), Some(vec![0, 255]));
        assert!(decode_hex("0").is_err());
        assert!(decode_hex("zz").is_err());
        assert!(process("transaction\t").is_err());
        assert!(process("unknown\t00").is_err());
    }

    #[test]
    fn event_sizes_match_serialized_bytes() {
        let events = TransactionEvents(
            [0, 1, 127, 128]
                .into_iter()
                .map(|size| sui_sdk_types::Event {
                    package_id: "0x2".parse().unwrap(),
                    module: "module".parse().unwrap(),
                    sender: "0x3".parse().unwrap(),
                    type_: "0x2::module::Event".parse().unwrap(),
                    contents: vec![7; size],
                })
                .collect(),
        );
        let sizes: Vec<_> = events
            .0
            .iter()
            .map(|event| bcs::to_bytes(event).unwrap().len())
            .collect();
        let hex: String = bcs::to_bytes(&events)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(
            process(&format!("events\t{hex}")).unwrap(),
            serde_json::json!({"events": events, "sizes": sizes})
        );
    }
}
