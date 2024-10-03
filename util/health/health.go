package health

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type Check struct {
	Name  string
	Check func(context.Context) (int, string, error)
}

func CheckAll(ctx context.Context, checks []Check) (int, string, error) {
	var (
		overallStatus = http.StatusOK
		messages      = make([]string, 0, len(checks))
	)

	for _, check := range checks {
		status, message, err := check.Check(ctx)
		if err != nil || status != http.StatusOK {
			overallStatus = http.StatusServiceUnavailable
		}

		var msg string

		if message[0] == '{' && message[len(message)-1] == '}' {
			msg = fmt.Sprintf(`{"resource": "%s", "status": "%d", "error": "%v", "dependencies": [%s]}`, check.Name, status, err, message)
		} else {
			msg = fmt.Sprintf(`{"resource": "%s", "status": "%d", "error": "%v", "message": "%s"}`, check.Name, status, err, message)
		}

		messages = append(messages, msg)
	}

	return overallStatus, strings.Join(messages, ",\n "), nil
}
