package prereq

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// Result represents one prerequisite check.
type Result struct {
	Name    string
	OK      bool
	Message string
}

// CheckAll runs all prerequisite checks and returns the results.
func CheckAll() []Result {
	return []Result{
		checkGo(),
		checkDocker(),
		checkDockerRunning(),
		checkPython(),
	}
}

// HasFailures returns true if any required check failed (Python is optional).
func HasFailures(results []Result) bool {
	for _, r := range results {
		if !r.OK && r.Name != "Python + PyYAML" {
			return true
		}
	}

	return false
}

// PrintResults prints the check results to stdout.
func PrintResults(results []Result) {
	for _, r := range results {
		status := "OK"
		if !r.OK {
			status = "FAIL"
		}

		fmt.Printf("  [%s] %s: %s\n", status, r.Name, r.Message)
	}
}

func checkGo() Result {
	out, err := exec.Command("go", "version").Output()
	if err != nil {
		return Result{Name: "Go", OK: false, Message: "not found - install Go 1.26+ from https://go.dev"}
	}

	version := string(out)

	// Parse version like "go version go1.26.0 linux/amd64"
	re := regexp.MustCompile(`go(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(version)

	if len(matches) < 3 {
		return Result{Name: "Go", OK: false, Message: "could not parse version: " + strings.TrimSpace(version)}
	}

	major, _ := strconv.Atoi(matches[1])
	minor, _ := strconv.Atoi(matches[2])

	if major < 1 || (major == 1 && minor < 26) {
		return Result{
			Name:    "Go",
			OK:      false,
			Message: fmt.Sprintf("version %d.%d found, need 1.26+", major, minor),
		}
	}

	return Result{Name: "Go", OK: true, Message: fmt.Sprintf("%d.%d", major, minor)}
}

func checkDocker() Result {
	out, err := exec.Command("docker", "--version").Output()
	if err != nil {
		return Result{Name: "Docker", OK: false, Message: "not found - install Docker or OrbStack"}
	}

	return Result{Name: "Docker", OK: true, Message: strings.TrimSpace(string(out))}
}

func checkDockerRunning() Result {
	err := exec.Command("docker", "info").Run()
	if err != nil {
		return Result{Name: "Docker running", OK: false, Message: "Docker daemon is not running"}
	}

	return Result{Name: "Docker running", OK: true, Message: "running"}
}

func checkPython() Result {
	err := exec.Command("python3", "-c", "import yaml").Run()
	if err != nil {
		return Result{
			Name:    "Python + PyYAML",
			OK:      false,
			Message: "not found (optional, needed for some build scripts)",
		}
	}

	return Result{Name: "Python + PyYAML", OK: true, Message: "available"}
}
