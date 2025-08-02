use crate::any::{
    AnyArgumentBuffer, AnyArguments, AnyColumn, AnyConnection, AnyQueryResult, AnyRow,
    AnyStatement, AnyTransactionManager, AnyTypeInfo, AnyValue, AnyValueRef,
};
use crate::database::{Database, HasStatementCache};
use crate::placeholders::ParamIndexing;

/// Opaque database driver. Capable of being used in place of any SQLx database driver. The actual
/// driver used will be selected at runtime, from the connection url.
#[derive(Debug)]
pub struct Any;

impl Database for Any {
    type Connection = AnyConnection;

    type TransactionManager = AnyTransactionManager;

    type Row = AnyRow;

    type QueryResult = AnyQueryResult;

    type Column = AnyColumn;

    type TypeInfo = AnyTypeInfo;

    type Value = AnyValue;
    type ValueRef<'r> = AnyValueRef<'r>;

    type Arguments = AnyArguments;
    type ArgumentBuffer = AnyArgumentBuffer;

    type Statement = AnyStatement;

    const NAME: &'static str = "Any";

    // Should this be constant for `Any` or should it be configurable
    // at runtime to be aligned with the database driver?
    const PLACEHOLDER_CHAR: char = 'X';
    const PARAM_INDEXING: ParamIndexing = ParamIndexing::Implicit;

    const URL_SCHEMES: &'static [&'static str] = &[];
}

// This _may_ be true, depending on the selected database
impl HasStatementCache for Any {}
