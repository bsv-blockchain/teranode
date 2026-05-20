package usql

// PostgreSQL error codes used across the codebase.
// See https://www.postgresql.org/docs/current/errcodes-appendix.html
const (
	PgErrUniqueViolation   = "23505" // unique_violation
	PgErrSerializationFail = "40001" // serialization_failure
	PgErrDeadlockDetected  = "40P01" // deadlock_detected
	PgErrLockNotAvailable  = "55P03" // lock_not_available
	PgErrCannotConnectNow  = "57P03" // cannot_connect_now
)
