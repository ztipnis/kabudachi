//! Where each shard name's data lives. Every key of a name carries the same
//! hash tag, so a cluster keeps them in one slot and one transaction may
//! touch all of them.

use kabudachi_core::protocol::ids::ShardName;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Part {
    Shard,
    Regs,
    Fence,
    Hint,
}

impl Part {
    fn suffix(self) -> &'static str {
        match self {
            Part::Shard => "shard",
            Part::Regs => "regs",
            Part::Fence => "fence",
            Part::Hint => "hint",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Keys {
    prefix: String,
    tag: String,
}

impl Keys {
    pub(crate) fn new(prefix: &str, name: &ShardName) -> Self {
        Self {
            prefix: prefix.to_string(),
            tag: escape_tag(name.as_str()),
        }
    }

    pub(crate) fn key(&self, part: Part) -> String {
        self.with_suffix(part.suffix())
    }

    pub(crate) fn sentinel(&self) -> String {
        self.with_suffix("sentinel")
    }

    /// The cluster slot of every key of this name.
    pub(crate) fn slot(&self) -> u16 {
        crc16::State::<crc16::XMODEM>::calculate(self.tag.as_bytes()) % 16384
    }

    fn with_suffix(&self, suffix: &str) -> String {
        format!("{}{{{}}}:{suffix}", self.prefix, self.tag)
    }
}

/// A name may hold braces, which would end the hash tag early, and may be
/// empty, which would make the tag empty and hash the whole key.
fn escape_tag(name: &str) -> String {
    if name.is_empty() {
        return "%".to_string();
    }
    name.replace('%', "%25").replace('{', "%7B").replace('}', "%7D")
}

/// Encodes fields as netstrings, `<len>:<text>,` each, so a field may hold any text.
pub(crate) fn join(fields: &[&str]) -> String {
    fields
        .iter()
        .map(|field| format!("{}:{field},", field.len()))
        .collect()
}

pub(crate) fn split(mut text: &str) -> Option<Vec<&str>> {
    let mut fields = Vec::new();
    while !text.is_empty() {
        let (len, rest) = text.split_once(':')?;
        let len: usize = len.parse().ok()?;
        fields.push(rest.get(..len)?);
        text = rest.get(len..)?.strip_prefix(',')?;
    }
    Some(fields)
}
