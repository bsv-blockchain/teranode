package coinbase_api

import (
	"encoding/json"
	"time"
)

// The following code wraps a DistributeTransactionResponse struct and then creates a string-based timestamp
func (d *DistributeTransactionResponse) MarshalJSON() ([]byte, error) {
	type Alias DistributeTransactionResponse

	isoTimestamp := ""
	if d.Timestamp != nil {
		isoTimestamp = d.Timestamp.AsTime().Format(time.RFC3339)
	}

	return json.Marshal(&struct {
		*Alias
		Timestamp string `json:"timestamp,omitempty"`
	}{
		Alias:     (*Alias)(d),
		Timestamp: isoTimestamp,
	})
}
