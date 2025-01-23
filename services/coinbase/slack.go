package coinbase

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

type slackPayload struct {
	Channel string `json:"channel"`
	Text    string `json:"text"`
}

func postMessageToSlack(channel, text string, slackToken string) error {
	payload := slackPayload{
		Channel: channel,
		Text:    text,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("POST", "https://slack.com/api/chat.postMessage", body)
	if err != nil {
		return err
	}

	if slackToken != "" {
		req.Header.Set("Authorization", "Bearer "+slackToken)
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if _, err := io.ReadAll(resp.Body); err != nil {
		return err
	}

	return nil
}
