package settings

import (
	"encoding/json"
	"net/url"
	"reflect"
)

// redactedValue is defined in export.go; both export and the log-safe
// Redact() helper share the same placeholder so consumers see a
// consistent marker.

// Redact returns a deep clone of s with every field tagged `redact:"true"`
// replaced by a placeholder, and the credentials inside every URL-typed field
// and every string (or string-slice element) that holds a URL removed by the
// same structural redaction the settings portal uses. The clone is safe to
// marshal to JSON for logging. A nil input returns nil with no error.
//
// Implementation note: the deep clone uses a JSON round-trip, so any fields
// that do not survive json.Marshal/Unmarshal — function pointers, channels,
// unexported state, *chaincfg.Params methods, big.Int internal representation —
// are NOT preserved in the returned struct. The returned value is intended
// solely for logging the user-configurable surface of Settings; do not feed
// it back into runtime code that depends on those non-JSON-marshalable
// fields.
func Redact(s *Settings) (*Settings, error) {
	if s == nil {
		return nil, nil
	}

	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	var clone Settings
	if err := json.Unmarshal(data, &clone); err != nil {
		return nil, err
	}

	redactValue(reflect.ValueOf(&clone).Elem())

	return &clone, nil
}

func redactValue(v reflect.Value) {
	if !v.IsValid() {
		return
	}

	switch v.Kind() {
	case reflect.Struct:
		// A url.URL keeps its credentials in two places, and the JSON round-trip above handles
		// neither well (bitcoin-sv/teranode#4844). The userinfo PASSWORD vanishes by accident,
		// because url.Userinfo's fields are all unexported - but what comes back is a non-nil
		// EMPTY Userinfo, which URL.String() renders as a stray "//@host". RawQuery, by contrast,
		// is an exported string, so a credential carried as a query parameter survives the
		// round-trip intact. Apply the same structural redaction the settings portal uses, drop
		// the empty userinfo, and do not descend into the struct's own fields.
		if v.Type() == reflect.TypeOf(url.URL{}) {
			if v.CanSet() {
				original, ok := v.Interface().(url.URL)
				if !ok {
					return
				}

				redacted := redactURL(&original)
				if redacted.User != nil {
					password, _ := redacted.User.Password()
					if redacted.User.Username() == "" && password == "" {
						redacted.User = nil
					}
				}

				v.Set(reflect.ValueOf(*redacted))
			}

			return
		}

		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}

			if f.Tag.Get("redact") == "true" {
				zeroSecret(v.Field(i))
				continue
			}

			redactValue(v.Field(i))
		}
	case reflect.String:
		// A connection string held as a plain string (Coinbase.DB) carries its credentials in the
		// same positions a url.URL does (bitcoin-sv/teranode#4844). Tagged fields never reach here:
		// the struct case above sends them to zeroSecret first. Slice elements reach this case
		// through the slice loop below.
		if v.CanSet() {
			v.SetString(redactURLString(v.String()))
		}
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			redactValue(v.Elem())
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			redactValue(v.Index(i))
		}
	}
}

// extractSensitiveKeys walks the Settings struct via reflection and returns
// a map of config keys (the `key:"X"` tag value) for every field tagged
// `redact:"true"`. Single source of truth for sensitive field identification —
// add `redact:"true"` to mark new fields. Used by export.go to identify
// settings whose values must be redacted in the settings portal.
func extractSensitiveKeys() map[string]bool {
	out := map[string]bool{}
	walkSensitiveTags(reflect.TypeOf(Settings{}), out)

	return out
}

// walkSensitiveTags is a recursive helper for extractSensitiveKeys. It only
// descends into struct types (directly or through a pointer-to-struct); it
// does not recurse into slices, arrays, maps, or interfaces. This is correct
// for the current Settings shape because every `redact:"true"` field lives
// at struct depth — no secret is held inside a map value, slice element type,
// or interface. Extend this walker if that ever changes.
func walkSensitiveTags(t reflect.Type, out map[string]bool) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}

		if f.Tag.Get("redact") == "true" {
			if k := f.Tag.Get("key"); k != "" {
				out[k] = true
			}

			continue
		}

		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		if ft.Kind() == reflect.Struct {
			walkSensitiveTags(ft, out)
		}
	}
}

func zeroSecret(v reflect.Value) {
	if !v.CanSet() {
		return
	}

	switch v.Kind() {
	case reflect.String:
		v.SetString(redactedValue)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.String {
			for i := 0; i < v.Len(); i++ {
				v.Index(i).SetString(redactedValue)
			}

			return
		}

		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	default:
		v.Set(reflect.Zero(v.Type()))
	}
}
