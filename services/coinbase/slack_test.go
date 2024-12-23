package coinbase

import (
	"testing"

	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestPostMessageToSlack(t *testing.T) {
	// For this test to work, you need to set the following environment variables:
	// slack_token
	// slack_channel
	// This can also be done by adding these items to a .env file in the same folder as this test
	t.SkipNow()

	tSettings := test.CreateBaseTestSettings()

	channel := tSettings.Coinbase.SlackChannel

	err := postMessageToSlack(channel, "test - please ignore", tSettings.Coinbase.SlackToken)
	require.NoError(t, err)
}
