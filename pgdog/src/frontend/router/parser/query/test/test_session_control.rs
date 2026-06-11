use pgdog_config::QueryParserLevel;

use crate::{config::config, frontend::client::TransactionType, frontend::Command};

use super::setup::*;

fn setup() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::SessionControl;
    QueryParserTest::new_single_primary(&config)
}

#[test]
fn test_set() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("SET statement_timeout TO 1").into()]);
    assert!(
        matches!(command, Command::Set { .. }),
        "expected Command::Set, got {command:#?}",
    );
}

#[test]
fn test_reset() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("RESET statement_timeout").into()]);
    assert!(
        matches!(command, Command::Set { .. }),
        "expected Command::Set, got {command:#?}",
    );
}

#[test]
fn test_reset_all() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("RESET ALL").into()]);
    assert!(
        matches!(command, Command::ResetAll),
        "expected Command::ResetAll, got {command:#?}",
    );
}

#[test]
fn test_begin() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("BEGIN").into()]);
    assert!(
        matches!(command, Command::StartTransaction { .. }),
        "expected Command::StartTransaction, got {command:#?}",
    );
}

#[test]
fn test_commit() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("COMMIT").into()]);
    assert!(
        matches!(command, Command::CommitTransaction { .. }),
        "expected Command::CommitTransaction, got {command:#?}",
    );
}

#[test]
fn test_rollback() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("ROLLBACK").into()]);
    assert!(
        matches!(command, Command::RollbackTransaction { .. }),
        "expected Command::RollbackTransaction, got {command:#?}",
    );
}

#[test]
fn test_plain_commit_has_no_reopen() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("COMMIT").into()]);
    assert!(
        matches!(command, Command::CommitTransaction { reopen: None, .. }),
        "expected CommitTransaction with reopen None, got {command:#?}",
    );
}

#[test]
fn test_commit_then_begin_captures_reopen() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("COMMIT; BEGIN").into()]);
    assert!(
        matches!(
            command,
            Command::CommitTransaction {
                reopen: Some(TransactionType::ReadWrite),
                ..
            }
        ),
        "expected CommitTransaction reopening a read-write transaction, got {command:#?}",
    );
}

#[test]
fn test_commit_then_begin_read_only_captures_type() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("COMMIT; BEGIN READ ONLY").into()]);
    assert!(
        matches!(
            command,
            Command::CommitTransaction {
                reopen: Some(TransactionType::ReadOnly),
                ..
            }
        ),
        "expected CommitTransaction reopening a read-only transaction, got {command:#?}",
    );
}

#[test]
fn test_rollback_then_begin_captures_reopen() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("ROLLBACK; BEGIN").into()]);
    assert!(
        matches!(
            command,
            Command::RollbackTransaction {
                reopen: Some(_),
                ..
            }
        ),
        "expected RollbackTransaction with reopen, got {command:#?}",
    );
}

#[test]
fn test_select_bypassed() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("SELECT 1").into()]);
    assert!(
        matches!(command, Command::Query(_)),
        "expected Command::Query (bypass), got {command:#?}",
    );
}

#[test]
fn test_insert_bypassed() {
    let mut test = setup();
    let command = test.execute(vec![Query::new("INSERT INTO users VALUES (1)").into()]);
    assert!(
        matches!(command, Command::Query(_)),
        "expected Command::Query (bypass), got {command:#?}",
    );
}

fn setup_with_locks() -> QueryParserTest {
    let mut config = (*config()).clone();
    config.config.general.query_parser = QueryParserLevel::SessionControlAndLocks;
    QueryParserTest::new_single_primary(&config)
}

#[test]
fn test_advisory_lock_detected() {
    let lock_queries = [
        "SELECT pg_advisory_lock(1)",
        "SELECT pg_advisory_lock_shared(1)",
        "SELECT pg_try_advisory_lock(1)",
        "SELECT pg_try_advisory_lock_shared(1)",
    ];

    for query in lock_queries {
        let mut test = setup_with_locks();
        let command = test.execute(vec![Query::new(query).into()]);
        match command {
            Command::Query(route) => assert!(
                route.is_lock_session(),
                "expected lock_session for '{query}', got {route:#?}"
            ),
            _ => panic!("expected Command::Query for '{query}', got {command:#?}"),
        }
    }

    let unlock_queries = [
        "SELECT pg_advisory_unlock(1)",
        "SELECT pg_advisory_unlock_all()",
    ];

    for query in unlock_queries {
        let mut test = setup_with_locks();
        let command = test.execute(vec![Query::new(query).into()]);
        match command {
            Command::Query(route) => assert!(
                route.is_unlock_session(),
                "expected unlock_session for '{query}', got {route:#?}"
            ),
            _ => panic!("expected Command::Query for '{query}', got {command:#?}"),
        }
    }

    // xact variants pin the backend for the life of the transaction, but the
    // query engine drops them at COMMIT/ROLLBACK rather than treating them as
    // session-persistent locks.
    let xact_queries = [
        "SELECT pg_advisory_xact_lock(1)",
        "SELECT pg_advisory_xact_lock_shared(1)",
        "SELECT pg_try_advisory_xact_lock(1)",
        "SELECT pg_try_advisory_xact_lock_shared(1)",
    ];

    for query in xact_queries {
        let mut test = setup_with_locks();
        let command = test.execute(vec![Query::new(query).into()]);
        match command {
            Command::Query(route) => {
                assert!(
                    route.is_lock_session(),
                    "xact locks still need to pin the backend for '{query}'"
                );
                assert!(!route.is_unlock_session());
            }
            _ => panic!("expected Command::Query for '{query}', got {command:#?}"),
        }
    }
}

#[test]
fn test_advisory_lock_not_detected_without_locks_level() {
    use crate::frontend::router::parser::route::{OverrideReason, ShardSource};

    let mut test = setup();
    let command = test.execute(vec![Query::new("SELECT pg_advisory_lock(1)").into()]);
    match command {
        Command::Query(route) => {
            assert!(
                !route.is_lock_session(),
                "SessionControl level should not classify advisory locks",
            );
            assert_eq!(
                route.shard_with_priority().source(),
                &ShardSource::Override(OverrideReason::ParserDisabled),
                "advisory lock should bypass the parser at SessionControl level"
            );
        }
        _ => panic!("expected Command::Query, got {command:#?}"),
    }
}
