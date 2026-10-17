package rpc

// Empty-credentials behaviour, asserted once and for all.
//
// The longdesc of rpc_user/rpc_pass says "Leave empty to disable
// authentication (development only)". The code never had that behaviour:
// the stored comparison hash is computed only when BOTH halves of a
// credential exist, otherwise it stays the all-zero array; checkAuth is
// called with require=true on every request path and no Authorization header
// hashes to the zero array. Empty credentials lock the door - fail-closed -
// and an operator who trusted the prose found a listening port answering 401
// to everything.
//
// These tests pin the fail-closed behaviour at the two places it is decided:
// authShaForCredentials (the assembly) and checkAuth (the decision), with a
// positive control so the rejections cannot be a test artifact.

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/require"
)

func TestAuthShaForCredentials_EmptyEitherHalfIsZeroArray(t *testing.T) {
	zero := [sha256.Size]byte{}

	require.Equal(t, zero, authShaForCredentials("", ""), "both halves empty")
	require.Equal(t, zero, authShaForCredentials("alice", ""), "pass empty")
	require.Equal(t, zero, authShaForCredentials("", "secret"), "user empty")

	require.NotEqual(t, zero, authShaForCredentials("alice", "secret"),
		"a complete credential must produce a real hash - otherwise every rejection below is vacuous")
}

func TestCheckAuth_EmptyCredentialsRejectEverything(t *testing.T) {
	s := &RPCServer{
		logger: mocklogger.NewTestLogger(),
		// exactly the state NewServer lands in when both settings are empty:
		authsha:      authShaForCredentials("", ""),
		limitauthsha: authShaForCredentials("", ""),
		settings:     &settings.Settings{},
	}

	request := func(header string) *http.Request {
		r := httptest.NewRequest("POST", "/rpc", nil)
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		return r
	}

	// The request path always requires authentication.
	t.Run("no header", func(t *testing.T) {
		ok, admin, err := s.checkAuth(request(""), true)
		require.Error(t, err, "no credentials configured must never wave an unauthenticated request through")
		require.False(t, ok)
		require.False(t, admin)
	})

	t.Run("plausible header against zero stored hash", func(t *testing.T) {
		ok, _, err := s.checkAuth(request("Basic "+base64.StdEncoding.EncodeToString([]byte("anyone:anything"))), true)
		require.Error(t, err)
		require.False(t, ok)
	})

	t.Run("empty-login header", func(t *testing.T) {
		// "Basic Og==" is base64(":") - what an operator might expect to be
		// the credential of empty settings. Its hash is not the zero array.
		ok, _, err := s.checkAuth(request("Basic "+base64.StdEncoding.EncodeToString([]byte(":"))), true)
		require.Error(t, err)
		require.False(t, ok)
	})
}

// Positive control: once credentials exist, the same code path accepts the
// matching header. Without this, the rejections above could be explained by
// a broken harness rather than by fail-closed assembly.
func TestCheckAuth_CompleteCredentialsAccept(t *testing.T) {
	user, pass := "operator", "correct horse"
	header := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))

	s := &RPCServer{
		logger:       mocklogger.NewTestLogger(),
		authsha:      authShaForCredentials(user, pass),
		limitauthsha: authShaForCredentials("", ""),
		settings:     &settings.Settings{},
	}

	r := httptest.NewRequest("POST", "/rpc", nil)
	r.Header.Set("Authorization", header)

	ok, admin, err := s.checkAuth(r, true)
	require.NoError(t, err)
	require.True(t, ok, "matching credentials must pass - the door is locked by emptiness, not broken")
	require.True(t, admin, "the admin hash grants admin level")

	wrong := httptest.NewRequest("POST", "/rpc", nil)
	wrong.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":wrong")))
	ok, _, err = s.checkAuth(wrong, true)
	require.Error(t, err)
	require.False(t, ok)
}
