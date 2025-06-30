package time

import (
	"database/sql/driver"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
)

// The following code implements custom time handling to accommodate differences between
// database engines. SQLite stores timestamps as TEXT fields while PostgreSQL uses native
// TIMESTAMP fields. This abstraction layer ensures consistent behavior across different
// database backends, which is essential for Teranode's database engine agnostic design.

const SQLiteTimestampFormat = "2006-01-02 15:04:05"

// CustomTime is a wrapper around time.Time that implements custom database serialization.
// This type provides database engine abstraction by handling the different timestamp
// formats used by PostgreSQL (native TIMESTAMP) and SQLite (TEXT fields).
type CustomTime struct {
	time.Time
}

// Scan implements the sql.Scanner interface for CustomTime.
//
// This method provides database engine abstraction by handling different timestamp
// representations from various SQL backends. It supports scanning time values from:
// - Native time.Time objects (used by PostgreSQL)
// - String representations (used by SQLite)
// - Byte array representations
//
// The method parses string and byte array values using the SQLiteTimestampFormat constant,
// ensuring consistent behavior regardless of the underlying database engine. This abstraction
// is critical for Teranode's database-agnostic design, allowing the same code to work with
// both PostgreSQL and SQLite backends without modification.
//
// Parameters:
//   - value: The database value to scan, which may be a time.Time, []byte, or string
//
// Returns:
//   - error: Any error encountered during scanning or parsing, specifically:
//   - ProcessingError for unsupported value types
//   - Time parsing errors if the string format is invalid
func (ct *CustomTime) Scan(value interface{}) error {
	switch v := value.(type) {
	case time.Time:
		ct.Time = v
		return nil
	case []byte:
		t, err := time.Parse(SQLiteTimestampFormat, string(v))
		if err != nil {
			return err
		}

		ct.Time = t

		return nil
	case string:
		t, err := time.Parse(SQLiteTimestampFormat, v)
		if err != nil {
			return err
		}

		ct.Time = t

		return nil
	}

	return errors.NewProcessingError("unsupported type: %T", value)
}

// Value implements the driver.Valuer interface for CustomTime.
//
// This method provides the database serialization functionality for CustomTime values,
// converting them to a format that can be stored in the database. It returns the underlying
// time.Time value, which will be handled appropriately by the database driver:
// - PostgreSQL driver will store it as a native TIMESTAMP
// - SQLite driver will convert it to a string using the appropriate format
//
// This abstraction is essential for Teranode's database-agnostic design, ensuring that
// timestamp values are consistently handled regardless of the underlying database engine.
//
// Returns:
//   - driver.Value: The time.Time value to be stored in the database
//   - error: Any error encountered during conversion (always nil for this implementation)
func (ct CustomTime) Value() (driver.Value, error) {
	return ct.Time, nil
}
