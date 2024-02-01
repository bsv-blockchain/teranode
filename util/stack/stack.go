package stack

import (
	"bufio"
	"bytes"
	"runtime/debug"
	"strings"
)

/* Stack returns a string containing the file path, line number and function
 * name of the caller.  This is useful for debugging.
 *
 * Example:
 * logger.Errorf("my error %v\n%s\n", err, stack.Stack())
 */
func Stack() string {
	stack := debug.Stack()
	scanner := bufio.NewScanner(bytes.NewReader(stack))
	var simplifiedStack []string
	var lastFuncName string
	skip := 2

	for scanner.Scan() {
		line := scanner.Text()

		// Check if the line contains a file path and line number
		if strings.Contains(line, ".go:") {
			// Extract the file path and line number
			parts := strings.Split(line, " ")
			for _, part := range parts {
				if strings.Contains(part, ".go:") {
					if skip > 0 {
						skip--
						break
					}
					// Get the function name from the last line
					funcName := lastFuncName
					// Trim the package name from the function name
					if lastSlashIndex := strings.LastIndex(funcName, "/"); lastSlashIndex != -1 {
						funcName = funcName[lastSlashIndex+1:]
					}

					simplifiedStack = append(simplifiedStack, "["+part+"] "+funcName)
					break
				}
			}
		} else {
			// Store the last function name
			lastFuncName = line
		}
	}

	return strings.Join(simplifiedStack, "\n")
}

/* Stack returns a string containing the file path and line number of the
 * caller.  This is useful for debugging.
 *
 * Example:
 * logger.Errorf("my error %v\n%s\n", err, stack.StackSimple())
 */
func StackSimple() string {
	stack := debug.Stack()
	scanner := bufio.NewScanner(bytes.NewReader(stack))
	var simplifiedStack []string
	skip := 2

	for scanner.Scan() {
		line := scanner.Text()
		// Identify lines with file path and line number
		if strings.Contains(line, ".go:") && !strings.Contains(line, "debug.Stack()") {
			// Extract the file path and line number
			parts := strings.Split(line, " ")
			for _, part := range parts {
				if strings.Contains(part, ".go:") {
					if skip > 0 {
						skip--
						break
					}
					simplifiedStack = append(simplifiedStack, strings.TrimSpace(part))
					break
				}
			}
		}
	}

	return strings.Join(simplifiedStack, "\n")
}
