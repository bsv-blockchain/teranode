// Package settings provide utilities for managing and displaying application settings.
// It is intended for debugging and inspection of application configuration and runtime behavior.
//
// Usage:
//
// This package is typically used to print application settings in JSON format,
// along-with-version and commit information, for debugging and documentation purposes.
//
// Functions:
//   - PrintSettings: Prints application settings, version, and commit information in a structured format.
//
// Side effects:
//
// Functions in this package may print to stdout and log errors if they occur.
package settings

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
)

// marshalSortedJSON marshals a struct to JSON with sorted keys at all levels.
// It recursively sorts struct fields alphabetically but leaves arrays/slices unchanged.
func marshalSortedJSON(v interface{}, indent string) ([]byte, error) {
	sorted := sortValue(v)
	return json.MarshalIndent(sorted, "", indent)
}

// sortValue recursively sorts struct fields but leaves slices/arrays unchanged
func sortValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	val := reflect.ValueOf(v)
	typ := reflect.TypeOf(v)

	// Check if the type implements json.Marshaler - if so, preserve it as-is
	// to maintain custom JSON marshaling behavior (like hash types that output as hex strings)
	if _, ok := v.(json.Marshaler); ok {
		return v
	}

	// Also check if pointer to this type implements json.Marshaler
	if val.CanAddr() {
		if _, ok := val.Addr().Interface().(json.Marshaler); ok {
			return v
		}
	}

	// Additional check: if the pointer type implements json.Marshaler, create a pointer
	// This matches Go's JSON marshaling behavior which automatically takes addresses when needed
	ptrType := reflect.PtrTo(typ)
	if ptrType.Implements(reflect.TypeOf((*json.Marshaler)(nil)).Elem()) {
		// Create a new pointer to this value to enable custom marshaling
		newVal := reflect.New(typ)
		newVal.Elem().Set(val)
		return newVal.Interface()
	}

	// Check if this type has a method set that implements json.Marshaler
	// This handles cases where the interface check above might fail
	if typ.Implements(reflect.TypeOf((*json.Marshaler)(nil)).Elem()) {
		return v
	}

	// Handle pointers
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		return sortValue(val.Elem().Interface())
	}

	// Handle arrays - preserve byte arrays as-is for potential hex encoding
	if val.Kind() == reflect.Array {
		// Special case: byte arrays (like [32]byte hashes) should be preserved as-is
		if val.Type().Elem().Kind() == reflect.Uint8 {
			return v
		}

		// For other arrays, recursively process elements but don't sort the array itself
		result := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			result[i] = sortValue(val.Index(i).Interface())
		}
		return result
	}

	// Handle slices - leave them unchanged, only process nested structs
	if val.Kind() == reflect.Slice {
		if val.Len() == 0 {
			return v
		}

		// Special case: convert byte slices to hex strings instead of base64
		if val.Type().Elem().Kind() == reflect.Uint8 {
			bytes := make([]byte, val.Len())
			for i := 0; i < val.Len(); i++ {
				bytes[i] = uint8(val.Index(i).Uint())
			}
			return fmt.Sprintf("%x", bytes)
		}

		// For other slices, recursively process elements but don't sort the slice itself
		result := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			result[i] = sortValue(val.Index(i).Interface())
		}
		return result
	}

	// Handle structs
	if val.Kind() == reflect.Struct {
		result := make(map[string]interface{})

		for i := 0; i < val.NumField(); i++ {
			field := typ.Field(i)
			fieldVal := val.Field(i)

			// Skip unexported fields
			if !fieldVal.CanInterface() {
				continue
			}

			// Get JSON tag name, fallback to field name
			jsonTag := field.Tag.Get("json")
			fieldName := field.Name
			if jsonTag != "" && jsonTag != "-" {
				// Handle json:",omitempty" and similar
				if idx := strings.Index(jsonTag, ","); idx != -1 {
					fieldName = jsonTag[:idx]
				} else {
					fieldName = jsonTag
				}
			}

			result[fieldName] = sortValue(fieldVal.Interface())
		}
		return result
	}

	// For all other types, return as-is
	return v
}

// PrintSettings prints the application settings, version, and commit information in a structured format.
//
// This function is used to display the current application configuration and runtime details
// for debugging and documentation purposes. It outputs the settings in JSON format along with
// version and commit metadata.
//
// Parameters:
//   - logger: A logger instance for logging errors.
//   - settings: The application settings to be displayed.
//   - version: The version of the application.
//   - commit: The commit hash of the application.
//
// Side effects:
//   - Prints the settings, version, and commit information to stdout.
//   - Logs errors if the settings cannot be marshaled to JSON.
func PrintSettings(logger ulogger.Logger, settings *settings.Settings, version, commit string) {
	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	settingsJSON, err := marshalSortedJSON(settings, "  ")
	if err != nil {
		logger.Errorf("Failed to marshal settings: %v", err)
	} else {
		logger.Infof("SETTINGS JSON\n-------------\n%s\n\n", string(settingsJSON))
	}
}
