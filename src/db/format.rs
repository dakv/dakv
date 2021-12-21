/// Value types encoded as the last component of internal keys.
/// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
/// data structures.
#[derive(PartialOrd, PartialEq, Debug, Copy, Clone)]
pub enum ValueType {
    TypeDeletion = 0x0,
    TypeValue = 0x1,
}

/// VALUE_TYPE_FOR_SEEK defines the ValueType that should be passed when
/// constructing a ParsedInternalKey object for seeking to a particular
/// sequence number (since we sort sequence numbers in decreasing order
/// and the value type is embedded as the low 8 bits in the sequence
/// number in internal keys, we need to use the highest-numbered
/// ValueType, not the lowest).
pub(crate) const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::TypeValue;

impl Default for ValueType {
    fn default() -> Self {
        ValueType::TypeValue
    }
}

impl From<u64> for ValueType {
    fn from(v: u64) -> Self {
        match v {
            0 => ValueType::TypeDeletion,
            1 => ValueType::TypeValue,
            _ => panic!("Cannot convert to ValueType"),
        }
    }
}

impl From<ValueType> for u64 {
    fn from(v: ValueType) -> Self {
        match v {
            ValueType::TypeDeletion => 0,
            ValueType::TypeValue => 1,
        }
    }
}

impl From<ValueType> for char {
    fn from(v: ValueType) -> char {
        match v {
            ValueType::TypeDeletion => 0 as char,
            ValueType::TypeValue => 1 as char,
        }
    }
}

macro_rules! impl_from {
    ($t:ty) => {
        impl From<$t> for ValueType {
            fn from(v: $t) -> Self {
                (v as u64).into()
            }
        }
    };
}

macro_rules! impl_into {
    ($t:ty) => {
        impl From<ValueType> for $t {
            fn from(v: ValueType) -> $t {
                let tmp: u64 = v.into();
                tmp as $t
            }
        }
    };
}

impl_from!(u8);
impl_from!(u16);
impl_from!(u32);
impl_from!(usize);

impl_into!(u8);
impl_into!(u16);
impl_into!(u32);
impl_into!(usize);

pub type SequenceNumber = u64;
