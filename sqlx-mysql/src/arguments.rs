use crate::encode::{Encode, IsNull};
use crate::types::Type;
use crate::{MySql, MySqlTypeInfo};
pub(crate) use sqlx_core::arguments::*;
use sqlx_core::encode_owned::{EncodeOwned, IntoEncode};
use sqlx_core::error::BoxDynError;
use sqlx_core::placeholders::{ArgumentKind, Placeholder};
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

/// Implementation of [`Arguments`] for MySQL.
#[derive(Debug, Default, Clone)]
pub struct MySqlArguments {
    positional: Vec<Arc<dyn EncodeOwned<MySql>>>,
    named: BTreeMap<String, Arc<dyn EncodeOwned<MySql>>>,
}

impl MySqlArguments {
    #[allow(clippy::borrowed_box)]
    pub(crate) fn get(&self, index: &ArgumentIndex<'_>) -> Option<&dyn EncodeOwned<MySql>> {
        let arc_opt = match index {
            ArgumentIndex::Positioned(i) => self.positional.get(*i),
            ArgumentIndex::Named(n) => self.named.get(n.as_ref()),
        };

        arc_opt.map(|x| x.as_ref())
    }

    pub(crate) fn get_kind(
        &self,
        index: &ArgumentIndex<'_>,
        place: &Placeholder<'_>,
        has_expansion: &mut bool,
    ) -> Result<ArgumentKind, String> {
        let arg = self.get(index).ok_or("unknown argument")?;

        let kind = if place.kleene.is_some() {
            let len = arg.vector_len().ok_or("expected vector for argument")?;

            *has_expansion = true;

            ArgumentKind::Vector(len)
        } else {
            ArgumentKind::Scalar
        };

        Ok(kind)
    }
}

#[derive(Debug, Default, Clone)]
pub struct MySqlArgumentsPositional {
    pub(crate) values: Vec<u8>,
    pub(crate) types: Vec<MySqlTypeInfo>,
    pub(crate) null_bitmap: NullBitMap,
}

impl MySqlArgumentsPositional {
    pub(crate) fn add<'q, T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: Encode<'q, MySql> + Type<MySql>,
    {
        let ty = value.produces().unwrap_or_else(T::type_info);

        let value_length_before_encoding = self.values.len();
        let is_null = match value.encode(&mut self.values) {
            Ok(is_null) => is_null,
            Err(error) => {
                // reset the value buffer to its previous value if encoding failed so we don't leave a half-encoded value behind
                self.values.truncate(value_length_before_encoding);
                return Err(error);
            }
        };

        self.types.push(ty);
        self.null_bitmap.push(is_null);

        Ok(())
    }
}

impl Arguments for MySqlArguments {
    type Database = MySql;

    fn reserve(&mut self, len: usize, _size: usize) {
        self.positional.reserve(len);
    }

    fn add<T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: IntoEncode<Self::Database> + Type<Self::Database>,
    {
        self.positional.push(Arc::new(value.into_encode_owned()));

        Ok(())
    }

    fn add_named<T>(&mut self, name: &str, value: T) -> Result<(), BoxDynError>
    where
        T: IntoEncode<Self::Database> + Type<Self::Database>,
    {
        self.named
            .insert(name.to_owned(), Arc::new(value.into_encode_owned()));

        Ok(())
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.positional.len()
    }
}

impl<'q> PositionalArguments<'q> for MySqlArgumentsPositional {
    type Database = MySql;

    fn reserve(&mut self, len: usize, size: usize) {
        self.types.reserve(len);
        self.values.reserve(size);
    }

    fn add<'t, T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: Encode<'t, Self::Database> + Type<Self::Database>,
    {
        self.add(value)
    }

    fn len(&self) -> usize {
        self.types.len()
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct NullBitMap {
    bytes: Vec<u8>,
    length: usize,
}

impl NullBitMap {
    fn push(&mut self, is_null: IsNull) {
        let byte_index = self.length / (u8::BITS as usize);
        let bit_offset = self.length % (u8::BITS as usize);

        if bit_offset == 0 {
            self.bytes.push(0);
        }

        self.bytes[byte_index] |= u8::from(is_null.is_null()) << bit_offset;
        self.length += 1;
    }
}

impl Deref for NullBitMap {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn null_bit_map_should_push_is_null() {
        let mut bit_map = NullBitMap::default();

        bit_map.push(IsNull::Yes);
        bit_map.push(IsNull::No);
        bit_map.push(IsNull::Yes);
        bit_map.push(IsNull::No);
        bit_map.push(IsNull::Yes);
        bit_map.push(IsNull::No);
        bit_map.push(IsNull::Yes);
        bit_map.push(IsNull::No);
        bit_map.push(IsNull::Yes);

        assert_eq!([0b01010101, 0b1].as_slice(), bit_map.deref());
    }
}
