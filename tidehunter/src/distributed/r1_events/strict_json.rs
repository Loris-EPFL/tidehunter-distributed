//! Test-only strict reader for r1 event streams: every JSON object, at any
//! depth, is parsed with duplicate keys rejected (`serde_json::Value` keeps
//! the last duplicate silently).
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use std::fmt;
use std::path::Path;

/// A JSON value whose objects never contain a duplicate key.
pub(crate) struct Strict(pub Value);

impl<'de> serde::Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(StrictVisitor).map(Strict)
    }
}

struct StrictVisitor;

impl<'de> Visitor<'de> for StrictVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("JSON without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(Strict(item)) = sequence.next_element()? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut object = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let Strict(value) = map.next_value()?;
            if object.insert(key.clone(), value).is_some() {
                return Err(de::Error::custom(format!("duplicate key `{key}`")));
            }
        }
        Ok(Value::Object(object))
    }
}

/// One stream: header, events and the terminal object if present.
pub(crate) struct Stream {
    pub header: Value,
    pub events: Vec<Value>,
    pub terminal: Option<Value>,
}

/// Read a stream strictly. Every line must parse; `seq` must be contiguous
/// from 0; the terminal object, if present, must be the last line.
pub(crate) fn read_stream(path: &Path) -> Stream {
    let text = std::fs::read_to_string(path).unwrap();
    let mut lines = text.lines();
    let Strict(header) = serde_json::from_str(lines.next().unwrap()).unwrap();
    let mut events = Vec::new();
    let mut terminal = None;
    for line in lines {
        assert!(
            terminal.is_none(),
            "{path:?}: line after the terminal object"
        );
        let Strict(value) = serde_json::from_str(line).unwrap();
        if let Some(end) = value.get("terminal") {
            terminal = Some(end.clone());
            continue;
        }
        assert_eq!(value["seq"], Value::from(events.len()), "{path:?}: seq gap");
        events.push(value);
    }
    Stream {
        header,
        events,
        terminal,
    }
}
