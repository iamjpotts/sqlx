pub(crate) use sqlx_core::database::{Database, HasStatementCache};

use crate::arguments::SqliteArgumentsBuffer;
use crate::{
    SqliteArguments, SqliteColumn, SqliteConnection, SqliteQueryResult, SqliteRow, SqliteStatement,
    SqliteTransactionManager, SqliteTypeInfo, SqliteValue, SqliteValueRef,
};
use sqlx_core::placeholders;

/// Sqlite database driver.
#[derive(Debug)]
pub struct Sqlite;

impl Database for Sqlite {
    type Connection = SqliteConnection;

    type TransactionManager = SqliteTransactionManager;

    type Row = SqliteRow;

    type QueryResult = SqliteQueryResult;

    type Column = SqliteColumn;

    type TypeInfo = SqliteTypeInfo;

    type Value = SqliteValue;
    type ValueRef<'r> = SqliteValueRef<'r>;

    type Arguments = SqliteArguments;
    type ArgumentBuffer = SqliteArgumentsBuffer;

    type Statement = SqliteStatement;

    const NAME: &'static str = "SQLite";

    const PLACEHOLDER_CHAR: char = '?';
    const PARAM_INDEXING: placeholders::ParamIndexing = placeholders::ParamIndexing::Implicit;

    const URL_SCHEMES: &'static [&'static str] = &["sqlite"];
}

impl HasStatementCache for Sqlite {}
