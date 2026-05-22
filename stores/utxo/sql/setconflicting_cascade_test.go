package sql

// Tests for the SetConflicting cascade bug live in:
//   - stores/utxo/setconflicting_cascade_bug_test.go (mock-based, proves the cascade)
//   - stores/utxo/aerospike/setconflicting_cascade_test.go (Aerospike TestContainer)
//
// SQLite cannot be tested here: SetConflicting (sql.go:3537) opens a write
// transaction via s.db.Begin(), then calls s.Get/s.GetSpend (lines 3548, 3595)
// on the store's connection pool — not on the open transaction. In SQLite
// serialized mode with a single-connection test pool, this deadlocks.
//
// This is also a potential production-latency concern for the SQL store under
// high concurrency, not just a test limitation.
