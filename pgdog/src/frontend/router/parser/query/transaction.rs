use crate::frontend::client::TransactionType;

use super::*;

impl QueryParser {
    /// Handle transaction control statements, e.g. BEGIN, ROLLBACK, COMMIT.
    ///
    /// # Arguments
    ///
    /// * `stmt`: Transaction statement from pg_query.
    /// * `context`: Query parser context.
    ///
    pub(super) fn transaction(
        &mut self,
        stmt: &TransactionStmt,
        context: &mut QueryParserContext,
    ) -> Result<Command, Error> {
        let extended = !context.query()?.simple();
        let mut rollback_savepoint = false;

        if context.rw_conservative() && !context.read_only {
            self.write_override = true;
        }

        match stmt.kind() {
            TransactionStmtKind::TransStmtCommit => {
                return Ok(Command::CommitTransaction {
                    extended,
                    reopen: None,
                })
            }
            TransactionStmtKind::TransStmtRollback => {
                return Ok(Command::RollbackTransaction {
                    extended,
                    reopen: None,
                })
            }
            TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart => {
                let transaction_type = Self::transaction_type(&stmt.options).unwrap_or_default();
                return Ok(Command::StartTransaction {
                    query: context.query()?.clone(),
                    transaction_type,
                    extended,
                    route: Route::write(context.shards_calculator.shard())
                        .with_read(transaction_type == TransactionType::ReadOnly),
                });
            }
            TransactionStmtKind::TransStmtRollbackTo => rollback_savepoint = true,
            TransactionStmtKind::TransStmtPrepare
            | TransactionStmtKind::TransStmtCommitPrepared
            | TransactionStmtKind::TransStmtRollbackPrepared => {
                if context.router_context.two_pc {
                    return Err(Error::NoTwoPc);
                }
            }
            _ => (),
        }

        context
            .shards_calculator
            .push(ShardWithPriority::new_table(Shard::All));

        Ok(Command::Query(
            Route::write(context.shards_calculator.shard())
                .with_rollback_savepoint(rollback_savepoint),
        ))
    }

    #[inline]
    fn transaction_type(options: &[Node]) -> Option<TransactionType> {
        for option_node in options {
            let node_enum = option_node.node.as_ref()?;
            if let NodeEnum::DefElem(def_elem) = node_enum {
                if def_elem.defname == "transaction_read_only" {
                    let arg_node = def_elem.arg.as_ref()?.node.as_ref()?;
                    if let NodeEnum::AConst(ac) = arg_node {
                        // 1 => read-only, 0 => read-write
                        if let Some(a_const::Val::Ival(i)) = ac.val.as_ref() {
                            if i.ival != 0 {
                                return Some(TransactionType::ReadOnly);
                            }
                        }
                    }
                }
            }
        }

        Some(TransactionType::ReadWrite)
    }

    /// Detect a `COMMIT; BEGIN` / `ROLLBACK; BEGIN` simple-query batch - the
    /// "commit (or rollback) and immediately reopen" form autocommit-off drivers
    /// (e.g. ADBC) send. PgDog otherwise routes on the first statement only
    /// (see `stmts.first()` in the parser), silently dropping the trailing BEGIN.
    /// On the backend-less commit path (a transaction that only touched
    /// PgDog-intercepted state, e.g. a tracked `SET`) that means the reopened
    /// transaction is never established, so the next statements run unpinned and
    /// transaction-local state (e.g. an RLS `set_config`) is lost. Capturing the
    /// reopen lets the commit handler re-establish the deferred transaction.
    pub(super) fn try_transaction_reopen(
        stmts: &[RawStmt],
        context: &QueryParserContext,
    ) -> Result<Option<Command>, Error> {
        let txn = |i: usize| -> Option<&TransactionStmt> {
            match stmts.get(i)?.stmt.as_ref()?.node.as_ref()? {
                NodeEnum::TransactionStmt(stmt) => Some(stmt),
                _ => None,
            }
        };

        let first_kind = match txn(0).map(|s| s.kind()) {
            Some(
                k @ (TransactionStmtKind::TransStmtCommit | TransactionStmtKind::TransStmtRollback),
            ) => k,
            _ => return Ok(None),
        };

        // A trailing BEGIN/START anywhere after the leading COMMIT/ROLLBACK is a
        // reopen; carry its transaction type so the new transaction routes correctly.
        let mut reopen = None;
        for i in 1..stmts.len() {
            if let Some(stmt) = txn(i) {
                if matches!(
                    stmt.kind(),
                    TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart
                ) {
                    reopen = Some(Self::transaction_type(&stmt.options).unwrap_or_default());
                    break;
                }
            }
        }

        if reopen.is_none() {
            return Ok(None);
        }

        let extended = !context.query()?.simple();
        Ok(Some(match first_kind {
            TransactionStmtKind::TransStmtCommit => Command::CommitTransaction { extended, reopen },
            _ => Command::RollbackTransaction { extended, reopen },
        }))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_detect_transaction_type() {
        let read_write_queries = vec![
            "BEGIN",
            "BEGIN;",
            "begin",
            "bEgIn",
            "BEGIN WORK",
            "BEGIN TRANSACTION",
            "BEGIN READ WRITE",
            "BEGIN WORK READ WRITE",
            "BEGIN TRANSACTION READ WRITE",
            "START TRANSACTION",
            "START TRANSACTION;",
            "start transaction",
            "START TRANSACTION READ WRITE",
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE DEFERRABLE",
        ];

        let read_only_queries = vec![
            "BEGIN READ ONLY",
            "BEGIN WORK READ ONLY",
            "BEGIN TRANSACTION READ ONLY",
            "START TRANSACTION READ ONLY",
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY",
            "START TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY NOT DEFERRABLE",
        ];

        for q in read_write_queries {
            let binding = pg_query::parse(q).unwrap();
            let stmt = binding
                .protobuf
                .stmts
                .first()
                .as_ref()
                .unwrap()
                .stmt
                .as_ref()
                .unwrap();

            match stmt.node {
                Some(NodeEnum::TransactionStmt(ref stmt)) => {
                    let t = QueryParser::transaction_type(&stmt.options);
                    assert_eq!(t, Some(TransactionType::ReadWrite));
                }
                _ => panic!("not a transaction"),
            }
        }

        for q in read_only_queries {
            let binding = pg_query::parse(q).unwrap();
            let stmt = binding
                .protobuf
                .stmts
                .first()
                .as_ref()
                .unwrap()
                .stmt
                .as_ref()
                .unwrap();

            match stmt.node {
                Some(NodeEnum::TransactionStmt(ref stmt)) => {
                    let t = QueryParser::transaction_type(&stmt.options);
                    assert_eq!(t, Some(TransactionType::ReadOnly));
                }
                _ => panic!("not a transaction"),
            }
        }
    }
}
