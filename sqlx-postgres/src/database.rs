use crate::arguments::PgArgumentBuffer;
use crate::value::{PgValue, PgValueRef};
use crate::{
    PgArguments, PgColumn, PgConnection, PgQueryResult, PgRow, PgStatement, PgTransactionManager,
    PgTypeInfo,
};

pub(crate) use sqlx_core::database::{Database, HasStatementCache};
use sqlx_core::placeholders;

/// PostgreSQL database driver.
#[derive(Debug)]
pub struct Postgres;

impl Database for Postgres {
    type Connection = PgConnection;

    type TransactionManager = PgTransactionManager;

    type Row = PgRow;

    type QueryResult = PgQueryResult;

    type Column = PgColumn;

    type TypeInfo = PgTypeInfo;

    type Value = PgValue;
    type ValueRef<'r> = PgValueRef<'r>;

    type Arguments = PgArguments;
    type ArgumentBuffer = PgArgumentBuffer;

    type Statement = PgStatement;

    const NAME: &'static str = "PostgreSQL";

    const PLACEHOLDER_CHAR: char = '$';
    const PARAM_INDEXING: placeholders::ParamIndexing = placeholders::ParamIndexing::OneIndexed;

    const URL_SCHEMES: &'static [&'static str] = &["postgres", "postgresql"];
}

impl HasStatementCache for Postgres {}
