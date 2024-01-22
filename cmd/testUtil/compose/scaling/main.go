package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func main() {
	// Define command-line flags
	upFlag := flag.Bool("up", false, "Start all services")
	cleanFlag := flag.Bool("clean", false, "Clean up and tear down services")

	// Parse the flags
	flag.Parse()

	// Execute based on flags
	if *upFlag {
		startServices()
	} else if *cleanFlag {
		cleanup()
	} else {
		fmt.Println("No valid command provided. Use --up to start services or --clean for cleanup.")
	}
}

func startServices() {
	// Define the path to the Docker Compose file
	composeFilePath := filepath.Join("..", "..", "..", "..", "docker-compose-ci-template.yml")

	// Start the Docker services in the background
	startService(fmt.Sprintf("docker-compose -f %s up -d p2p-bootstrap-1", composeFilePath))
	time.Sleep(10 * time.Second)
	startService(fmt.Sprintf("docker-compose -f %s up -d postgres", composeFilePath))
	time.Sleep(10 * time.Second)
	startService(fmt.Sprintf("docker-compose -f %s up -d ubsv-1 ubsv-2 ubsv-3", composeFilePath))
	time.Sleep(10 * time.Second)

	// Check if nodes are in sync and have 300 blocks each
	checkBlocks("http://localhost:18090", 300)
	checkBlocks("http://localhost:28090", 305)
	checkBlocks("http://localhost:38090", 305)

	// Start the Coinbase services in the background
	startService(fmt.Sprintf("docker-compose -f %s up -d ubsv-1-coinbase ubsv-2-coinbase", composeFilePath))
	time.Sleep(10 * time.Second)
	checkBlocks("http://localhost:18090", 310)
	checkBlocks("http://localhost:28090", 310)
	checkBlocks("http://localhost:38090", 310)
}

func cleanup() {
	fmt.Println("Starting cleanup...")

	// Define the path to the project root
	projectRoot := filepath.Join("..", "..", "..", "..")

	// Remove the data directory
	dataDir := filepath.Join(projectRoot, "data")
	if err := os.RemoveAll(dataDir); err != nil {
		fmt.Printf("Error removing data directory: %s\n", err)
	} else {
		fmt.Println("Removed data directory.")
	}

	// Stop and remove all services
	composeFilePath := filepath.Join("..", "..", "..", "..", "docker-compose-ci-template.yml")
	if err := execCommand(fmt.Sprintf("docker-compose -f %s down", composeFilePath)); err != nil {
		fmt.Println(err)
		return
	}

	// Remove Docker volumes (if you know their names, replace 'docker volume prune' with 'docker volume rm <volume_name>')
	if err := execCommand("docker", "volume", "prune", "-f"); err != nil {
		fmt.Println(err)
		return
	}
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
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var blocks []struct {
		Height int `json:"height"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&blocks); err != nil {
		return 0, err
	}

	if len(blocks) == 0 {
		return 0, fmt.Errorf("no blocks found in response")
	}

	return blocks[0].Height, nil
}

func execCommand(name string, args ...string) error {
	cmd := exec.Command("bash", "-c", name)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("error executing command: %w", err)
	}
	return nil
}
