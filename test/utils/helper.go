package helper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Function to call the RPC endpoint with any method and parameters, returning the response and error
func CallRPC(url string, method string, params []interface{}) (string, error) {

	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %v", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	// Set the appropriate headers
	req.SetBasicAuth("bitcoin", "bitcoin")
	req.Header.Set("Content-Type", "application/json")

	// Perform the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to perform request: %v", err)
	}
	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("expected status code 200, got %v", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	// Return the response as a string
	return string(body), nil
}

// Example usage of the function
// func main() {
// 	// Call the function with the "getblock" method and specific parameters
// 	response, err := callRPC("http://localhost:19292", "getblock", []interface{}{"003e8c9abde82685fdacfd6594d9de14801c4964e1dbe79397afa6299360b521", 1})
// 	if err != nil {
// 		fmt.Printf("Error: %v\n", err)
// 	} else {
// 		fmt.Printf("Response: %s\n", response)
// 	}
// }
