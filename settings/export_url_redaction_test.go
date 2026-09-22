package settings

import (
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// auditSecretMarker is the value planted in every credential position. Nothing exported may
// contain it (bitcoin-sv/teranode#4844).
const auditSecretMarker = "audit-secret-marker"

// hierarchicalCredentialURL carries the marker in BOTH credential positions of an ordinary
// authority-form URL - the userinfo password and a `password=` query parameter.
const hierarchicalCredentialURL = "scheme://audit-user:" + auditSecretMarker +
	"@db.internal:5432/chain?password=" + auditSecretMarker + "&partitions=4"

// opaqueCredentialURL is the shape net/url declines to decompose: no `//` authority, so everything
// after the scheme lands in URL.Opaque and User, Host and Path are all empty. The credential is
// still right there in the text, and the structural redaction has nothing to bite on.
const opaqueCredentialURL = "postgres:audit-user:" + auditSecretMarker + "@db.internal:5432/chain"

// populateEveryURLField walks a *Settings and sets every *url.URL and url.URL field to the given
// URL. Returns how many fields were populated.
//
// The count matters: without it a walker bug makes the caller's "no marker anywhere" assertion
// pass while proving nothing at all. Taking the URL as a parameter matters for the same reason:
// sweeping every field with one SHAPE cannot detect a shape the redactor mishandles.
func populateEveryURLField(t *testing.T, s *Settings, rawURL string) int {
	t.Helper()

	populated := 0

	var walk func(v reflect.Value)

	walk = func(v reflect.Value) {
		if !v.IsValid() {
			return
		}

		switch v.Kind() {
		case reflect.Pointer:
			if v.Type() == reflect.TypeOf((*url.URL)(nil)) {
				if !v.CanSet() {
					return
				}

				u, err := url.Parse(rawURL)
				require.NoError(t, err)

				v.Set(reflect.ValueOf(u))
				populated++

				return
			}

			if v.IsNil() {
				if v.Type().Elem().Kind() != reflect.Struct || !v.CanSet() {
					return
				}

				v.Set(reflect.New(v.Type().Elem()))
			}

			walk(v.Elem())
		case reflect.Struct:
			if v.Type() == reflect.TypeOf(url.URL{}) {
				if !v.CanSet() {
					return
				}

				u, err := url.Parse(rawURL)
				require.NoError(t, err)

				v.Set(reflect.ValueOf(*u))
				populated++

				return
			}

			for i := 0; i < v.NumField(); i++ {
				if !v.Type().Field(i).IsExported() {
					continue
				}

				walk(v.Field(i))
			}
		}
	}

	walk(reflect.ValueOf(s).Elem())

	return populated
}

// TestExportMetadata_RedactsCredentialsInEveryURLSetting is the structural guarantee: the fix lives
// in the one formatter every URL setting goes through, so it covers all of them and every one added
// later, rather than only the fields someone remembered to tag.
func TestExportMetadata_RedactsCredentialsInEveryURLSetting(t *testing.T) {
	t.Run("authority form", func(t *testing.T) {
		s := NewSettings()

		populated := populateEveryURLField(t, s, hierarchicalCredentialURL)
		require.GreaterOrEqual(t, populated, 25,
			"the walker must actually have set the URL fields, or this test proves nothing")

		registry := s.ExportMetadata()
		require.NotNil(t, registry)

		byKey := map[string]SettingMetadata{}

		for _, setting := range registry.Settings {
			require.NotContains(t, setting.CurrentValue, auditSecretMarker,
				"setting %s leaked a credential", setting.Key)
			byKey[setting.Key] = setting
		}

		// Redaction is surgical: operators still need to see which backend a node points at.
		store, ok := byKey["blockchain_store"]
		require.True(t, ok, "blockchain_store must be exported")
		require.Contains(t, store.CurrentValue, "audit-user", "the user NAME is not a secret and stays visible")
		require.Contains(t, store.CurrentValue, "db.internal:5432")
		require.Contains(t, store.CurrentValue, "/chain")
		require.Contains(t, store.CurrentValue, "partitions=4", "non-credential query parameters are preserved")
	})

	t.Run("opaque form", func(t *testing.T) {
		// The same sweep over the shape net/url declines to decompose. Running the walk with one
		// URL shape is what let this case through the first time: every field was populated, every
		// field was checked, and the shape the redactor could not see was never presented to it.
		s := NewSettings()

		populated := populateEveryURLField(t, s, opaqueCredentialURL)
		require.GreaterOrEqual(t, populated, 25,
			"the walker must actually have set the URL fields, or this test proves nothing")

		registry := s.ExportMetadata()
		require.NotNil(t, registry)

		for _, setting := range registry.Settings {
			require.NotContains(t, setting.CurrentValue, auditSecretMarker,
				"setting %s leaked a credential carried in an opaque URL", setting.Key)
		}
	})
}

// TestRedactURL_OpaqueURLFailsSafe covers the shape the structural redaction cannot decompose
// (bitcoin-sv/teranode#4844).
//
// `postgres:user:password@host/db` has no `//` authority, so net/url puts everything after the
// scheme into URL.Opaque and leaves User, Host, Path and RawQuery empty. The userinfo and
// query-parameter rules therefore have nothing to bite on, and String() renders the credential back
// verbatim. There is no safe way to pick the secret out of a form the standard parser itself
// declined to interpret, so the whole component is replaced.
func TestRedactURL_OpaqueURLFailsSafe(t *testing.T) {
	u, err := url.Parse(opaqueCredentialURL)
	require.NoError(t, err)

	// Fixture preconditions: this really is the undecomposable shape, not an authority URL in
	// disguise. If net/url ever starts decomposing it, this test should say the premise moved
	// rather than pass for the wrong reason.
	require.NotEmpty(t, u.Opaque, "fixture precondition: the URL is opaque")
	require.Nil(t, u.User, "fixture precondition: there is no userinfo to redact")
	require.Empty(t, u.Host, "fixture precondition: there is no host to redact")
	require.Contains(t, u.String(), auditSecretMarker, "fixture precondition: the raw form leaks")

	redacted := redactURL(u)
	require.NotContains(t, redacted.String(), auditSecretMarker)
	require.Equal(t, redactedURLValue, redacted.Opaque, "the fail-safe must have fired")
	require.Equal(t, "postgres:"+redactedURLValue, redacted.String(),
		"the scheme stays visible so an operator can still see which backend was named")

	require.Equal(t, opaqueCredentialURL, u.String(), "the caller's URL must never be mutated")

	// An opaque URL can also carry a query, and that half is still redacted surgically.
	withQuery, err := url.Parse("postgres:host/db?password=" + auditSecretMarker + "&partitions=4")
	require.NoError(t, err)
	require.NotEmpty(t, withQuery.Opaque)

	redactedWithQuery := redactURL(withQuery)
	require.NotContains(t, redactedWithQuery.String(), auditSecretMarker)
	require.Equal(t, "password="+redactedURLValue+"&partitions=4", redactedWithQuery.RawQuery)
}

// TestExportMetadata_BlockchainStorePasswordRedacted reproduces the audit's settings-export proof
// directly: a datastore password in the documented production syntax must not reach the exported
// metadata, while the fields that were already tagged stay redacted as before.
func TestExportMetadata_BlockchainStorePasswordRedacted(t *testing.T) {
	s := NewSettings()

	storeURL, err := url.Parse("postgres://audit-user:" + auditSecretMarker + "@db.internal:5432/chain")
	require.NoError(t, err)

	s.BlockChain.StoreURL = storeURL
	s.GRPCAdminAPIKey = auditSecretMarker

	registry := s.ExportMetadata()

	byKey := map[string]SettingMetadata{}
	for _, setting := range registry.Settings {
		byKey[setting.Key] = setting
	}

	store, ok := byKey["blockchain_store"]
	require.True(t, ok)
	require.NotContains(t, store.CurrentValue, auditSecretMarker,
		"the datastore password must not survive into the exported metadata")
	// The placeholder inside a URL is deliberately URL-safe: url.URL.String() percent-encodes the
	// userinfo, so `********` would render as `%2A%2A%2A%2A%2A%2A%2A%2A` and tell the operator
	// nothing. Pin the readable form.
	require.Equal(t, "postgres://audit-user:"+redactedURLValue+"@db.internal:5432/chain", store.CurrentValue)
	require.NotContains(t, store.CurrentValue, "%2A", "the placeholder must survive URL encoding unchanged")

	adminKey, ok := byKey["grpc_admin_api_key"]
	require.True(t, ok)
	require.Equal(t, redactedValue, adminKey.CurrentValue,
		"the existing tag-based redaction is unchanged")
}

// TestRedactURL_MalformedQueryFailsSafe pins the two-branch design.
//
// Anything the standard parser rejects has its WHOLE query replaced - the assertion is that the
// fail-safe actually fired, not merely that the marker vanished, so a future change cannot satisfy
// this test by accident. Anything it accepts is redacted surgically, byte-for-byte elsewhere.
func TestRedactURL_MalformedQueryFailsSafe(t *testing.T) {
	t.Run("the standard parser rejects it: the whole query is replaced", func(t *testing.T) {
		for _, rawQuery := range []string{
			"password=%ZZ",
			"password=%",
			"PASSWORD=" + auditSecretMarker + "%2",
			// The semicolon separator, rejected since Go 1.17. Split on `&` alone this is a single
			// segment whose key parses as `a`, so it would be preserved byte-for-byte.
			"a=1;password=" + auditSecretMarker,
		} {
			t.Run(rawQuery, func(t *testing.T) {
				u := &url.URL{Scheme: "postgres", Host: "db.internal:5432", Path: "/chain", RawQuery: rawQuery}

				redacted := redactURL(u)
				require.NotContains(t, redacted.String(), auditSecretMarker)
				require.Equal(t, redactedURLValue, redacted.RawQuery, "the fail-safe must have fired")

				require.Equal(t, rawQuery, u.RawQuery, "the caller's URL must never be mutated")
			})
		}
	})

	t.Run("well-formed: surgical redaction", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			rawQuery string
			want     string
		}{
			{
				name:     "credential and non-credential",
				rawQuery: "password=" + auditSecretMarker + "&partitions=4",
				want:     "password=" + redactedURLValue + "&partitions=4",
			},
			{
				name:     "repeated key: both values redacted",
				rawQuery: "password=&password=" + auditSecretMarker,
				want:     "password=" + redactedURLValue + "&password=" + redactedURLValue,
			},
			{
				name:     "no credential key: preserved byte-for-byte, no reordering",
				rawQuery: "partitions=4&MaxRetries=30",
				want:     "partitions=4&MaxRetries=30",
			},
			{
				name:     "valueless key carries no credential, so nothing is invented",
				rawQuery: "password",
				want:     "password",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				u := &url.URL{Scheme: "postgres", Host: "db.internal:5432", Path: "/chain", RawQuery: tc.rawQuery}

				redacted := redactURL(u)
				require.Equal(t, tc.want, redacted.RawQuery)
				require.NotContains(t, redacted.String(), auditSecretMarker)

				require.Equal(t, tc.rawQuery, u.RawQuery, "the caller's URL must never be mutated")
			})
		}
	})

	t.Run("a bare username is left alone", func(t *testing.T) {
		// postgres://user@host is a common non-secret configuration; mangling it would hurt more
		// operators than it helps.
		u, err := url.Parse("postgres://audit-user@db.internal:5432/chain")
		require.NoError(t, err)

		require.Equal(t, "postgres://audit-user@db.internal:5432/chain", redactURL(u).String())
	})

	t.Run("nil in, nil out", func(t *testing.T) {
		require.Nil(t, redactURL(nil))
	})
}

// TestExportMetadata_MalformedQueryFailsSafeEndToEnd runs the malformed cases through the real
// exporter, so the fail-safe is proved on the path operators actually see.
func TestExportMetadata_MalformedQueryFailsSafeEndToEnd(t *testing.T) {
	for _, rawQuery := range []string{
		"password=%ZZ",
		"password=%",
		"PASSWORD=" + auditSecretMarker + "%2",
		"a=1;password=" + auditSecretMarker,
		"password=" + auditSecretMarker + "&partitions=4",
		"password=&password=" + auditSecretMarker,
	} {
		t.Run(rawQuery, func(t *testing.T) {
			s := NewSettings()
			s.BlockChain.StoreURL = &url.URL{
				Scheme:   "postgres",
				Host:     "db.internal:5432",
				Path:     "/chain",
				RawQuery: rawQuery,
			}

			registry := s.ExportMetadata()

			for _, setting := range registry.Settings {
				require.NotContains(t, setting.CurrentValue, auditSecretMarker,
					"setting %s leaked a credential", setting.Key)
			}
		})
	}
}

// TestRedactRawQuery_PreservesEncoding guards the decision not to use Encode(), which would reorder
// parameters alphabetically and re-percent-encode them, churning every kafka and aerospike URL in
// the settings portal for no benefit.
func TestRedactRawQuery_PreservesEncoding(t *testing.T) {
	rawQuery := "SleepBetweenRetries=50ms&MaxRetries=30&TotalTimeout=30s"
	require.Equal(t, rawQuery, redactRawQuery(rawQuery))
	require.False(t, strings.HasPrefix(redactRawQuery(rawQuery), "MaxRetries"),
		"parameters must not be reordered")
}
