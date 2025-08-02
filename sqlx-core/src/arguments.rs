//! Types and traits for passing arguments to SQL queries.

use crate::database::Database;
use crate::encode::Encode;
use crate::encode_owned::IntoEncode;
use crate::error::BoxDynError;
use crate::types::Type;
use std::borrow::Cow;
use std::fmt::{self, Display, Formatter, Write};

// This lint is designed for general collections, but `Arguments` is not meant to be as such.
#[allow(clippy::len_without_is_empty)]
pub trait Arguments: Send + Sized + Default {
    type Database: Database;

    /// Reserves the capacity for at least `additional` more positional values (of `size` total bytes) to
    /// be added to the arguments without a reallocation.
    fn reserve(&mut self, additional: usize, size: usize);

    /// Add the value to the end of the positional arguments.
    fn add<T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: IntoEncode<Self::Database> + Type<Self::Database>;

    fn add_named<T>(&mut self, name: &str, value: T) -> Result<(), BoxDynError>
    where
        T: IntoEncode<Self::Database> + Type<Self::Database>;

    /// The number of positional arguments that were already added.
    fn len(&self) -> usize;

    /// todo: writes incorrect positional placeholders when named arguments are present
    fn format_placeholder<W: Write>(&self, writer: &mut W) -> fmt::Result {
        writer.write_str("?")
    }
}

#[allow(clippy::len_without_is_empty)]
pub trait PositionalArguments<'q>: Send + Sized + Default {
    type Database: Database;

    /// Reserves the capacity for at least `additional` more values (of `size` total bytes) to
    /// be added to the arguments without a reallocation.
    fn reserve(&mut self, additional: usize, size: usize);

    /// Add the value to the end of the arguments.
    fn add<'t, T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: Encode<'t, Self::Database> + Type<Self::Database>;

    /// The number of arguments that were already added.
    fn len(&self) -> usize;

    fn format_placeholder<W: Write>(&self, writer: &mut W) -> fmt::Result {
        writer.write_str("?")
    }
}

pub trait IntoArguments<DB: Database>: Sized + Send {
    fn into_arguments(self) -> <DB as Database>::Arguments;
}

// NOTE: required due to lack of lazy normalization
#[macro_export]
macro_rules! impl_into_arguments_for_arguments {
    ($Arguments:path) => {
        impl
            $crate::arguments::IntoArguments<<$Arguments as $crate::arguments::Arguments>::Database>
            for $Arguments
        {
            fn into_arguments(self) -> $Arguments {
                self
            }
        }
    };
}

/// used by the query macros to prevent supernumerary `.bind()` calls
pub struct ImmutableArguments<DB: Database>(pub <DB as Database>::Arguments);

impl<DB: Database> IntoArguments<DB> for ImmutableArguments<DB> {
    fn into_arguments(self) -> <DB as Database>::Arguments {
        self.0
    }
}

/// The index for a given bind argument; either positional or named.
#[derive(Debug, PartialEq, Eq)]
pub enum ArgumentIndex<'a> {
    Positioned(usize),
    Named(Cow<'a, str>),
}

impl ArgumentIndex<'_> {
    pub(crate) fn into_static(self) -> ArgumentIndex<'static> {
        match self {
            Self::Positioned(pos) => ArgumentIndex::Positioned(pos),
            Self::Named(named) => ArgumentIndex::Named(named.into_owned().into()),
        }
    }
}

impl<'a> From<&'a str> for ArgumentIndex<'a> {
    fn from(name: &'a str) -> Self {
        ArgumentIndex::Named(name.into())
    }
}

impl<'a> From<&'a String> for ArgumentIndex<'a> {
    fn from(name: &'a String) -> Self {
        ArgumentIndex::Named(name.into())
    }
}

impl From<usize> for ArgumentIndex<'static> {
    fn from(position: usize) -> Self {
        ArgumentIndex::Positioned(position)
    }
}

impl Display for ArgumentIndex<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Positioned(pos) => Display::fmt(pos, f),
            Self::Named(named) => Display::fmt(named, f),
        }
    }
}
