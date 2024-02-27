package runner

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	buildFlag bool
	upFlag    bool
	cleanFlag bool
)

var composeFileName = "docker-compose-generated.yml"
var composeFilePath = filepath.Join("../../../", composeFileName)

func AddRunCommand(rootCmd *cobra.Command) {
	var runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run Docker Compose file",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Run executed \n")
			err := Run()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	runCmd.Flags().BoolVarP(&buildFlag, "build", "b", false, "Build all services")
	runCmd.Flags().BoolVarP(&upFlag, "up", "u", false, "Start all services")
	runCmd.Flags().BoolVarP(&cleanFlag, "clean", "c", false, "Clean up and tear down services")
	rootCmd.AddCommand(runCmd)
}

func Run() error {

	// Execute based on flags
	if upFlag {
		startServices()
	} else if cleanFlag {
		cleanup()
	} else if buildFlag {
		// Build the Docker services
		exec.Command("bash", "-c", fmt.Sprintf("docker-compose -f %s build", composeFilePath))
	} else {
		return fmt.Errorf("No valid command provided. Use --up to start services or --clean for cleanup.")
	}
	return nil
}

func startServices() {
	// Start the Docker services in the background
	startService(fmt.Sprintf("docker-compose -f %s up -d p2p-bootstrap-1", composeFilePath))
	time.Sleep(10 * time.Second)
	startService(fmt.Sprintf("docker-compose -f %s up -d postgres", composeFilePath))
	time.Sleep(10 * time.Second)
	ubsvServices, err := getServices("ubsv")
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	startService(fmt.Sprintf("docker-compose -f %s up -d %s", composeFilePath, ubsvServices))
	time.Sleep(10 * time.Second)

	time.Sleep(10 * time.Second)
	checkBlocks("http://localhost:18090", 301)

	// Start the TxBlaster services in the background
	txBlasterServices, err := getServices("tx-blaster")
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	startService(fmt.Sprintf("docker-compose -f %s up -d %s", composeFilePath, txBlasterServices))
}

func cleanup() {
	fmt.Println("Starting cleanup...")

	// Define the path to the project root
	rootPath := filepath.Join("../../../")

	// Remove the data directory
	dataDir := filepath.Join(rootPath, "data")
	if err := os.RemoveAll(dataDir); err != nil {
		fmt.Printf("Error removing data directory: %s\n", err)
	} else {
		fmt.Println("Removed data directory.")
	}

	// Stop and remove all services
	composeFilePath := filepath.Join(rootPath, composeFileName)
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
	resp, err := http.Get(url + "/api/v1/lastblocks?n=1")
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

func getServices(prefix string) (string, error) {

	cmd := exec.Command("bash", "-c", fmt.Sprintf("docker-compose -f %s config --services", composeFilePath))
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var servicesList []string
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			servicesList = append(servicesList, line)
		}
	}
	services := strings.Join(servicesList, " ")
	return services, nil
}
