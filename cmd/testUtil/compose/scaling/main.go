package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"path/filepath"
	"time"
)

func main() {
	// Define the path to the Docker Compose file
	composeFilePath := filepath.Join("..", "..", "..", "..", "docker-compose-ci-template.yml")

	// Start the Docker services in the background
	startService(fmt.Sprintf("docker-compose -f %s up -d p2p-bootstrap-1", composeFilePath))
	startService(fmt.Sprintf("docker-compose -f %s up -d postgres ubsv-1 ubsv-2", composeFilePath))

	// Check if nodes are in sync and have 300 blocks each
	checkBlocks("http://localhost:18090", 300)
	checkBlocks("http://localhost:28090", 300)

	// Continue with other services and checks...
}

func startService(command string) {
	cmd := exec.Command("bash", "-c", command)
	err := cmd.Start()
	if err != nil {
		fmt.Printf("Error starting service: %s\n", err)
		return
	}
	fmt.Printf("Started service: %s\n", command)
}

func checkBlocks(url string, desiredHeight int) {
	for {
		fmt.Printf("Checking block height at %s\n", url)
		height, err := getBlockHeight(url)
		if err != nil {
			fmt.Printf("Error getting block height: %s\n", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if height >= desiredHeight {
			fmt.Printf("Node at %s reached block height %d\n", url, height)
			break
		}

		fmt.Printf("Current height at %s is %d, waiting...\n", url, height)
		time.Sleep(5 * time.Second)
	}
}

func getBlockHeight(url string) (int, error) {
	resp, err := http.Get(url + "/lastblocks?n=1")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	fmt.Printf("Body: %s\n", body)

	// Decode the JSON response
	var blocks []struct {
		Height int `json:"height"`
	}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&blocks); err != nil {
		panic("Error: " + err.Error())
	}
	height := blocks[0].Height
	fmt.Printf("Height: %d\n", height)
	return height, nil
}
