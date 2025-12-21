package chaos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// ToxiproxyClient manages toxiproxy API interactions
type ToxiproxyClient struct {
	BaseURL string
	client  *http.Client
}

// NewToxiproxyClient creates a new toxiproxy client
func NewToxiproxyClient(baseURL string) *ToxiproxyClient {
	return &ToxiproxyClient{
		BaseURL: baseURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Toxic represents a toxiproxy toxic configuration
type Toxic struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Stream     string                 `json:"stream"`
	Toxicity   float64                `json:"toxicity"`
	Attributes map[string]interface{} `json:"attributes"`
}

// Proxy represents a toxiproxy proxy configuration
type Proxy struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	Enabled  bool   `json:"enabled"`
}

// AddLatency adds latency to a proxy
func (c *ToxiproxyClient) AddLatency(proxyName string, latencyMs int, stream string) error {
	toxic := Toxic{
		Name:     "latency_" + stream,
		Type:     "latency",
		Stream:   stream,
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"latency": latencyMs,
		},
	}
	return c.addToxic(proxyName, toxic)
}

// AddBandwidthLimit limits bandwidth on a proxy
func (c *ToxiproxyClient) AddBandwidthLimit(proxyName string, rateKBps int, stream string) error {
	toxic := Toxic{
		Name:     "bandwidth_" + stream,
		Type:     "bandwidth",
		Stream:   stream,
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"rate": rateKBps,
		},
	}
	return c.addToxic(proxyName, toxic)
}

// AddTimeout adds timeout toxic (connection drops)
func (c *ToxiproxyClient) AddTimeout(proxyName string, timeoutMs int, toxicity float64, stream string) error {
	toxic := Toxic{
		Name:     "timeout_" + stream,
		Type:     "timeout",
		Stream:   stream,
		Toxicity: toxicity,
		Attributes: map[string]interface{}{
			"timeout": timeoutMs,
		},
	}
	return c.addToxic(proxyName, toxic)
}

// AddSlicer adds slicer toxic (slow data transmission)
func (c *ToxiproxyClient) AddSlicer(proxyName string, avgSize, sizeVariation, delayMs int, stream string) error {
	toxic := Toxic{
		Name:     "slicer_" + stream,
		Type:     "slicer",
		Stream:   stream,
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"average_size":   avgSize,
			"size_variation": sizeVariation,
			"delay":          delayMs,
		},
	}
	return c.addToxic(proxyName, toxic)
}

// addToxic sends a toxic to the toxiproxy API
func (c *ToxiproxyClient) addToxic(proxyName string, toxic Toxic) error {
	data, err := json.Marshal(toxic)
	if err != nil {
		return errors.NewProcessingError("failed to marshal toxic: %w", err)
	}

	url := fmt.Sprintf("%s/proxies/%s/toxics", c.BaseURL, proxyName)
	resp, err := c.client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return errors.NewProcessingError("failed to add toxic: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return errors.NewProcessingError("toxiproxy API error (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// RemoveToxic removes a specific toxic
func (c *ToxiproxyClient) RemoveToxic(proxyName, toxicName string) error {
	url := fmt.Sprintf("%s/proxies/%s/toxics/%s", c.BaseURL, proxyName, toxicName)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return errors.NewProcessingError("failed to create delete request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return errors.NewProcessingError("failed to remove toxic: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.NewProcessingError("toxiproxy API error (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// RemoveAllToxics removes all toxics from a proxy
func (c *ToxiproxyClient) RemoveAllToxics(proxyName string) error {
	toxics, err := c.ListToxics(proxyName)
	if err != nil {
		return errors.NewProcessingError("failed to list toxics: %w", err)
	}

	for _, toxic := range toxics {
		if err := c.RemoveToxic(proxyName, toxic.Name); err != nil {
			return errors.NewProcessingError("failed to remove toxic %s: %w", toxic.Name, err)
		}
	}

	return nil
}

// ListToxics lists all toxics for a proxy
func (c *ToxiproxyClient) ListToxics(proxyName string) ([]Toxic, error) {
	url := fmt.Sprintf("%s/proxies/%s/toxics", c.BaseURL, proxyName)
	resp, err := c.client.Get(url)
	if err != nil {
		return nil, errors.NewProcessingError("failed to list toxics: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, errors.NewProcessingError("toxiproxy API error (status %d): %s", resp.StatusCode, string(body))
	}

	var toxics []Toxic
	if err := json.NewDecoder(resp.Body).Decode(&toxics); err != nil {
		return nil, errors.NewProcessingError("failed to decode toxics: %w", err)
	}

	return toxics, nil
}

// EnableProxy enables a proxy
func (c *ToxiproxyClient) EnableProxy(proxyName string) error {
	return c.setProxyEnabled(proxyName, true)
}

// DisableProxy disables a proxy
func (c *ToxiproxyClient) DisableProxy(proxyName string) error {
	return c.setProxyEnabled(proxyName, false)
}

// setProxyEnabled enables or disables a proxy
func (c *ToxiproxyClient) setProxyEnabled(proxyName string, enabled bool) error {
	data, err := json.Marshal(map[string]bool{"enabled": enabled})
	if err != nil {
		return errors.NewProcessingError("failed to marshal proxy state: %w", err)
	}

	url := fmt.Sprintf("%s/proxies/%s", c.BaseURL, proxyName)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return errors.NewProcessingError("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return errors.NewProcessingError("failed to set proxy state: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.NewProcessingError("toxiproxy API error (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetProxy gets proxy information
func (c *ToxiproxyClient) GetProxy(proxyName string) (*Proxy, error) {
	url := fmt.Sprintf("%s/proxies/%s", c.BaseURL, proxyName)
	resp, err := c.client.Get(url)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get proxy: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, errors.NewProcessingError("toxiproxy API error (status %d): %s", resp.StatusCode, string(body))
	}

	var proxy Proxy
	if err := json.NewDecoder(resp.Body).Decode(&proxy); err != nil {
		return nil, errors.NewProcessingError("failed to decode proxy: %w", err)
	}

	return &proxy, nil
}

// ResetProxy removes all toxics and re-enables the proxy
func (c *ToxiproxyClient) ResetProxy(proxyName string) error {
	if err := c.RemoveAllToxics(proxyName); err != nil {
		return errors.NewProcessingError("failed to remove toxics: %w", err)
	}

	if err := c.EnableProxy(proxyName); err != nil {
		return errors.NewProcessingError("failed to enable proxy: %w", err)
	}

	return nil
}

// WaitForProxy waits for toxiproxy to be available
func (c *ToxiproxyClient) WaitForProxy(proxyName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := c.GetProxy(proxyName)
		if err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errors.NewProcessingError("timeout waiting for proxy %s", proxyName)
}
