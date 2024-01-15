package coinbase

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestPostMessageToSlack(t *testing.T) {
	// For this test to work, you need to set the following environment variables:
	// slack_token
	// slack_channel

	// This can also be done by adding these items to a .env file in the same folder as this test

	t.SkipNow()

	channel, _ := gocore.Config().Get("slack_channel")

	err := postMessageToSlack(channel, "test - please ignore")
	require.NoError(t, err)
}
