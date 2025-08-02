use crate::{
    Either, PgArgumentsInner, PgColumn, PgConnectOptions, PgConnection, PgQueryResult, PgRow,
    PgTransactionManager, PgTypeInfo, Postgres,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{FutureExt, TryStreamExt};
use sqlx_core::sql_str::{AssertSqlSafe, SqlSafeStr, SqlStr};
use std::pin::pin;

use sqlx_core::any::{
    Any, AnyArguments, AnyColumn, AnyConnectOptions, AnyConnectionBackend, AnyQueryResult, AnyRow,
    AnyStatement, AnyTypeInfo, AnyTypeInfoKind,
};

use crate::arguments::PgArguments;
use crate::type_info::PgType;
use sqlx_core::connection::Connection;
use sqlx_core::database::Database;
use sqlx_core::describe::Describe;
use sqlx_core::executor::Executor;
use sqlx_core::ext::ustr::UStr;
use sqlx_core::placeholders::parse_query;
use sqlx_core::transaction::TransactionManager;

sqlx_core::declare_driver_with_optional_migrate!(DRIVER = Postgres);

impl AnyConnectionBackend for PgConnection {
    fn name(&self) -> &str {
        <Postgres as Database>::NAME
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self).boxed()
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self).boxed()
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::ping(self).boxed()
    }

    fn begin(&mut self, statement: Option<SqlStr>) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::begin(self, statement).boxed()
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::commit(self).boxed()
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        PgTransactionManager::rollback(self).boxed()
    }

    fn start_rollback(&mut self) {
        PgTransactionManager::start_rollback(self)
    }

    fn get_transaction_depth(&self) -> usize {
        PgTransactionManager::get_transaction_depth(self)
    }

    fn shrink_buffers(&mut self) {
        Connection::shrink_buffers(self);
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self).boxed()
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    #[cfg(feature = "migrate")]
    fn as_migrate(
        &mut self,
    ) -> sqlx_core::Result<&mut (dyn sqlx_core::migrate::Migrate + Send + 'static)> {
        Ok(self)
    }

    fn fetch_many(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxStream<sqlx_core::Result<Either<AnyQueryResult, AnyRow>>> {
        let persistent = persistent && arguments.is_some();

        Box::pin(try_stream! {
            let (sql, arguments_inner) = sql_and_args(query, arguments)?;

            let mut s = pin!(self.run(sql, arguments_inner, persistent, None).await?);

            while let Some(v) = s.try_next().await? {
                let v = match v {
                    Either::Left(result) => Either::Left(map_result(result)),
                    Either::Right(row) => Either::Right(AnyRow::try_from(&row)?),
                };

                r#yield!(v);
            }

            Ok(())
        })
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: SqlStr,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<AnyStatement>> {
        Box::pin(async move {
            let statement = Executor::prepare_with(self, sql, &[]).await?;
            let colunn_names = statement.metadata.column_names.clone();
            AnyStatement::try_from_statement(statement, colunn_names)
        })
    }

    fn describe<'c>(&mut self, sql: SqlStr) -> BoxFuture<'_, sqlx_core::Result<Describe<Any>>> {
        Box::pin(async move {
            let describe = Executor::describe(self, sql).await?;

            let columns = describe
                .columns
                .iter()
                .map(AnyColumn::try_from)
                .collect::<Result<Vec<_>, _>>()?;

            let parameters = match describe.parameters {
                Some(Either::Left(parameters)) => Some(Either::Left(
                    parameters
                        .iter()
                        .enumerate()
                        .map(|(i, type_info)| {
                            AnyTypeInfo::try_from(type_info).map_err(|_| {
                                sqlx_core::Error::AnyDriverError(
                                    format!(
                                        "Any driver does not support type {type_info} of parameter {i}"
                                    )
                                    .into(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                Some(Either::Right(count)) => Some(Either::Right(count)),
                None => None,
            };

            Ok(Describe {
                columns,
                parameters,
                nullable: describe.nullable,
            })
        })
    }
}

#[allow(unused)]
fn sql_and_args_parsing(
    query: SqlStr,
    arguments: Option<AnyArguments>,
) -> sqlx_core::Result<(SqlStr, Option<PgArgumentsInner>)> {
    let arguments: Option<PgArguments> = arguments
        .map(AnyArguments::convert_into)
        .transpose()
        .map_err(sqlx_core::Error::Encode)?;

    let (expanded_sql, expanded_args) = match &arguments {
        None => (query.as_str().to_string(), None),
        Some(args) => {
            let parsed = parse_query(query.as_str())?;

            let mut _has_expansion = false;

            let (expanded_sql, expanded_args) = parsed.expand::<Postgres, _, _, _>(
                |idx, place| args.get_kind(idx, place, &mut _has_expansion),
                PgArgumentsInner::default,
            )?;

            (expanded_sql.to_string(), Some(expanded_args))
        }
    };

    let expanded_sql = AssertSqlSafe(expanded_sql).into_sql_str();

    Ok((expanded_sql, expanded_args))
}

fn sql_and_args(
    query: SqlStr,
    arguments: Option<AnyArguments>,
) -> sqlx_core::Result<(SqlStr, Option<PgArgumentsInner>)> {
    let arguments: Option<PgArguments> = arguments
        .map(AnyArguments::convert_into)
        .transpose()
        .map_err(sqlx_core::Error::Encode)?;
    
    let expanded_args = match arguments {
        None => None,
        Some(args) => {
            let args = args.try_into_only_positional().map_err(sqlx_core::Error::Encode)?;
            
            Some(args)
        }
    };
    

    Ok((query, expanded_args))
}

impl<'a> TryFrom<&'a PgTypeInfo> for AnyTypeInfo {
    type Error = sqlx_core::Error;

    fn try_from(pg_type: &'a PgTypeInfo) -> Result<Self, Self::Error> {
        Ok(AnyTypeInfo {
            kind: match &pg_type.0 {
                PgType::Bool => AnyTypeInfoKind::Bool,
                PgType::Void => AnyTypeInfoKind::Null,
                PgType::Int2 => AnyTypeInfoKind::SmallInt,
                PgType::Int4 => AnyTypeInfoKind::Integer,
                PgType::Int8 => AnyTypeInfoKind::BigInt,
                PgType::Float4 => AnyTypeInfoKind::Real,
                PgType::Float8 => AnyTypeInfoKind::Double,
                PgType::Bytea => AnyTypeInfoKind::Blob,
                PgType::Text | PgType::Varchar => AnyTypeInfoKind::Text,
                PgType::DeclareWithName(UStr::Static("citext")) => AnyTypeInfoKind::Text,
                _ => {
                    return Err(sqlx_core::Error::AnyDriverError(
                        format!("Any driver does not support the Postgres type {pg_type:?}").into(),
                    ))
                }
            },
        })
    }
}

impl<'a> TryFrom<&'a PgColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(col: &'a PgColumn) -> Result<Self, Self::Error> {
        let type_info =
            AnyTypeInfo::try_from(&col.type_info).map_err(|e| sqlx_core::Error::ColumnDecode {
                index: col.name.to_string(),
                source: e.into(),
            })?;

        Ok(AnyColumn {
            ordinal: col.ordinal,
            name: col.name.clone(),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a PgRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a PgRow) -> Result<Self, Self::Error> {
        AnyRow::map_from(row, row.metadata.column_names.clone())
    }
}

impl<'a> TryFrom<&'a AnyConnectOptions> for PgConnectOptions {
    type Error = sqlx_core::Error;

    fn try_from(value: &'a AnyConnectOptions) -> Result<Self, Self::Error> {
        let mut opts = PgConnectOptions::parse_from_url(&value.database_url)?;
        opts.log_settings = value.log_settings.clone();
        Ok(opts)
    }
}

fn map_result(res: PgQueryResult) -> AnyQueryResult {
    AnyQueryResult {
        rows_affected: res.rows_affected(),
        last_insert_id: None,
    }
}
