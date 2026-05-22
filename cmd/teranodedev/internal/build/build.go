package build

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
	"github.com/bsv-blockchain/teranode/errors"
)

// Build compiles the teranode binary with the appropriate build tags.
func Build(projectRoot string, cfg *config.Config) error {
	tags := buildTags(cfg)
	args := []string{"build", "-o", "teranode.run"}

	if len(tags) > 0 {
		args = append(args, "-tags", strings.Join(tags, ","))
	}

	args = append(args, ".")

	fmt.Printf("  go %s\n", strings.Join(args, " "))

	cmd := exec.Command("go", args...)
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return errors.NewProcessingError("build failed", err)
	}

	fmt.Println("  Built teranode.run")

	return nil
}

func buildTags(cfg *config.Config) []string {
	var tags []string

	if cfg.UTXOBackend == "aerospike" {
		tags = append(tags, "aerospike")
	}

	return tags
}
