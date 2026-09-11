package recoverreplayedtransactions

import "fmt"

func commandError(format string, args ...any) error {
	//nolint:forbidigo // Preserve sentinel identity used by documented CLI exit codes.
	return fmt.Errorf(format, args...)
}
func combineErrors(primary, secondary error) error {
	if secondary == nil {
		return primary
	}
	if primary == nil {
		return secondary
	}
	return commandError("%w (earlier result: %v)", secondary, primary)
}
