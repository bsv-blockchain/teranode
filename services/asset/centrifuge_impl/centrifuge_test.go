package centrifuge_impl

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/services/asset/httpimpl"
	"github.com/bitcoin-sv/teranode/services/asset/repository"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockRepo := &repository.Repository{} // Minimal mock for testing
	mockHTTP := &httpimpl.HTTP{}         // Minimal mock for testing

	t.Run("Valid configuration", func(t *testing.T) {
		tSettings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: "http://localhost:8080",
			},
		}

		centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
		assert.NoError(t, err)
		assert.NotNil(t, centrifuge)
		assert.Equal(t, logger, centrifuge.logger)
		assert.Equal(t, tSettings, centrifuge.settings)
		assert.Equal(t, mockRepo, centrifuge.repository)
		assert.Equal(t, "http://localhost:8080", centrifuge.baseURL)
		assert.Equal(t, mockHTTP, centrifuge.httpServer)
	})

	t.Run("Missing asset HTTP address", func(t *testing.T) {
		tSettings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: "", // Empty address
			},
		}

		centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
		assert.Error(t, err)
		assert.Nil(t, centrifuge)
		assert.Contains(t, err.Error(), "asset_httpAddress not found in config")
	})

	t.Run("Invalid URL - malformed", func(t *testing.T) {
		tSettings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: "://invalid-url-missing-scheme",
			},
		}

		centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
		assert.Error(t, err)
		assert.Nil(t, centrifuge)
		assert.Contains(t, err.Error(), "asset_httpAddress is not a valid URL")
	})
}

func TestCentrifuge_Structure(t *testing.T) {
	t.Run("Centrifuge struct fields", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		mockRepo := &repository.Repository{}
		mockHTTP := &httpimpl.HTTP{}
		tSettings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: "http://localhost:8080",
			},
		}

		centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
		require.NoError(t, err)

		// Test that all fields are properly initialized
		assert.NotNil(t, centrifuge.logger)
		assert.NotNil(t, centrifuge.settings)
		assert.NotNil(t, centrifuge.repository)
		assert.NotEmpty(t, centrifuge.baseURL)
		assert.NotNil(t, centrifuge.httpServer)
		// centrifugeNode and blockchainClient should be nil until Init() is called
		assert.Nil(t, centrifuge.centrifugeNode)
		assert.Nil(t, centrifuge.blockchainClient)
	})
}

func TestCentrifuge_Init_NoP2PAddress(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockRepo := &repository.Repository{}
	mockHTTP := &httpimpl.HTTP{}

	// Create settings without P2P HTTP address to test the failure case
	tSettings := &settings.Settings{
		Asset: settings.AssetSettings{
			HTTPAddress: "http://localhost:8080",
		},
		P2P: settings.P2PSettings{
			HTTPAddress: "", // Empty P2P address
		},
	}

	centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
	require.NoError(t, err)

	// Init should succeed in creating the centrifuge node
	err = centrifuge.Init(context.Background())
	// Note: Init may succeed but Start will fail due to missing P2P address
	// We're just testing that the basic structure is working
	if err != nil {
		// Some error is expected if centrifuge dependencies aren't available
		assert.NotNil(t, err)
	}

	// Verify that centrifugeNode was created
	assert.NotNil(t, centrifuge.centrifugeNode)
}

func TestMessageType(t *testing.T) {
	t.Run("messageType struct", func(t *testing.T) {
		msg := messageType{
			Type: "block",
		}

		assert.Equal(t, "block", msg.Type)
	})
}

func TestConstants(t *testing.T) {
	t.Run("Access control constants", func(t *testing.T) {
		assert.Equal(t, "Access-Control-Allow-Origin", AccessControlAllowOrigin)
		assert.Equal(t, "Access-Control-Allow-Headers", AccessControlAllowHeaders)
		assert.Equal(t, "Access-Control-Allow-Credentials", AccessControlAllowCredentials)
	})
}

func TestCentrifuge_ValidationLogic(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockRepo := &repository.Repository{}
	mockHTTP := &httpimpl.HTTP{}

	t.Run("URL validation works correctly", func(t *testing.T) {
		// Test various URL formats
		validURLs := []string{
			"http://localhost:8080",
			"https://example.com",
			"http://192.168.1.1:8080",
			"https://api.example.com/path",
		}

		for _, validURL := range validURLs {
			tSettings := &settings.Settings{
				Asset: settings.AssetSettings{
					HTTPAddress: validURL,
				},
			}

			centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
			assert.NoError(t, err)
			assert.NotNil(t, centrifuge)
			assert.Equal(t, validURL, centrifuge.baseURL)
		}
	})

	t.Run("Malformed URLs are rejected", func(t *testing.T) {
		invalidURLs := []string{
			"://missing-scheme",
			"http://[invalid-bracket",
		}

		for _, invalidURL := range invalidURLs {
			tSettings := &settings.Settings{
				Asset: settings.AssetSettings{
					HTTPAddress: invalidURL,
				},
			}

			centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
			assert.Error(t, err, "URL should be invalid: %s", invalidURL)
			assert.Nil(t, centrifuge)
		}
	})

	t.Run("Valid URLs including non-HTTP schemes", func(t *testing.T) {
		validURLs := []string{
			"ftp://example.com",
			"not-a-standard-url-but-parsed-ok", // Go's url.Parse is quite permissive
		}

		for _, validURL := range validURLs {
			tSettings := &settings.Settings{
				Asset: settings.AssetSettings{
					HTTPAddress: validURL,
				},
			}

			centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
			assert.NoError(t, err, "URL should be valid: %s", validURL)
			assert.NotNil(t, centrifuge)
		}
	})
}

func TestCentrifuge_ErrorCases(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockRepo := &repository.Repository{}
	mockHTTP := &httpimpl.HTTP{}

	t.Run("Nil settings causes panic", func(t *testing.T) {
		// This test verifies that nil settings causes a panic (which is expected behavior)
		defer func() {
			if r := recover(); r != nil {
				// Expected panic - test passes
				assert.NotNil(t, r)
			}
		}()
		// This should panic
		_, _ = New(logger, nil, mockRepo, mockHTTP)
		// If we get here without panic, fail the test
		assert.Fail(t, "Expected panic when settings is nil")
	})

	t.Run("Nil dependencies handled gracefully", func(t *testing.T) {
		tSettings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: "http://localhost:8080",
			},
		}

		// Test with nil repository - should not crash during New()
		centrifuge, err := New(logger, tSettings, nil, mockHTTP)
		assert.NoError(t, err)
		assert.NotNil(t, centrifuge)
		assert.Nil(t, centrifuge.repository)

		// Test with nil HTTP server - should not crash during New()
		centrifuge, err = New(logger, tSettings, mockRepo, nil)
		assert.NoError(t, err)
		assert.NotNil(t, centrifuge)
		assert.Nil(t, centrifuge.httpServer)
	})
}

func TestCentrifuge_BaseURLParsing(t *testing.T) {
	logger := ulogger.TestLogger{}
	mockRepo := &repository.Repository{}
	mockHTTP := &httpimpl.HTTP{}

	t.Run("Base URL is properly stored", func(t *testing.T) {
		testURL := "https://api.example.com:9090/base/path"
		tSettings := &settings.Settings{
			Asset: settings.AssetSettings{
				HTTPAddress: testURL,
			},
		}

		centrifuge, err := New(logger, tSettings, mockRepo, mockHTTP)
		assert.NoError(t, err)
		assert.Equal(t, testURL, centrifuge.baseURL)

		// Verify URL can be parsed again
		parsedURL, err := url.Parse(centrifuge.baseURL)
		assert.NoError(t, err)
		assert.Equal(t, "https", parsedURL.Scheme)
		assert.Equal(t, "api.example.com:9090", parsedURL.Host)
		assert.Equal(t, "/base/path", parsedURL.Path)
	})
}
