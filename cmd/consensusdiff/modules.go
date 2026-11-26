package main

import (
	"bufio"
	"io"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/mod/modfile"
)

// none stands for a module that a go.mod does not require.
const none = "(none)"

// pinChange is a consensus module whose effective version differs between the
// base and the head go.mod.
type pinChange struct {
	path string
	base string
	head string
}

// readModules parses one module path per line. Text after '#' is a comment.
func readModules(r io.Reader) ([]string, error) {
	var modules []string

	seen := map[string]bool{}
	scanner := bufio.NewScanner(r)

	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := scanner.Text()
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		if len(fields) != 1 {
			return nil, errors.NewConfigurationError("line %d: want one module path, got %q", lineNo, scanner.Text())
		}

		if seen[fields[0]] {
			return nil, errors.NewConfigurationError("line %d: module %s is listed twice", lineNo, fields[0])
		}

		seen[fields[0]] = true

		modules = append(modules, fields[0])
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(modules) == 0 {
		return nil, errors.NewConfigurationError("no modules listed")
	}

	return modules, nil
}

// parseGoMod parses go.mod content. name is used in error messages only.
func parseGoMod(name string, data []byte) (*modfile.File, error) {
	f, err := modfile.Parse(name, data, nil)
	if err != nil {
		return nil, errors.NewConfigurationError("parsing %s", name, err)
	}

	return f, nil
}

// effectiveVersion returns what a go.mod pins a module to: the required
// version, followed by " => <path> <version>" when a replace directive applies.
// It returns none when the module is not required.
func effectiveVersion(f *modfile.File, path string) string {
	required := ""

	for _, r := range f.Require {
		if r.Mod.Path == path {
			required = r.Mod.Version
			break
		}
	}

	if required == "" {
		return none
	}

	for _, r := range f.Replace {
		if r.Old.Path == path && r.Old.Version == required {
			return strings.TrimSpace(required + " => " + r.New.Path + " " + r.New.Version)
		}
	}

	for _, r := range f.Replace {
		if r.Old.Path == path && r.Old.Version == "" {
			return strings.TrimSpace(required + " => " + r.New.Path + " " + r.New.Version)
		}
	}

	return required
}

// changedPins lists the modules whose effective version differs between base
// and head, in the order of modules.
func changedPins(base, head *modfile.File, modules []string) []pinChange {
	var changes []pinChange

	for _, path := range modules {
		b, h := effectiveVersion(base, path), effectiveVersion(head, path)
		if b != h {
			changes = append(changes, pinChange{path: path, base: b, head: h})
		}
	}

	return changes
}
