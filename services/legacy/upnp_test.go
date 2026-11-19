package legacy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A gateway that accepts the connection and never answers does not park a UPnP call for good.
// upnpUpdateThread makes these calls and holds the server's wait group, so a call with no timeout
// hung shutdown with it.
func TestUPnPHTTPCalls_GiveUpOnAGatewayThatNeverAnswers(t *testing.T) {
	require.NotZero(t, upnpHTTPClient.Timeout, "UPnP HTTP calls need a timeout")

	release := make(chan struct{})

	gateway := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))

	// Cleanups run last-in first-out: release the handler, then close the server.
	t.Cleanup(gateway.Close)
	t.Cleanup(func() { close(release) })

	origClient := upnpHTTPClient
	upnpHTTPClient = &http.Client{Timeout: 50 * time.Millisecond}

	t.Cleanup(func() { upnpHTTPClient = origClient })

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{
			name: "the description fetch",
			call: func() error {
				_, err := getServiceURL(gateway.URL)
				return err
			},
		},
		{
			name: "a SOAP request",
			call: func() error {
				_, err := soapRequest(gateway.URL, "GetExternalIPAddress", "")
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errc := make(chan error, 1)

			go func() { errc <- tc.call() }()

			select {
			case err := <-errc:
				require.Error(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("a UPnP call must give up on a gateway that never answers")
			}
		})
	}
}
