package guard

import (
	"fmt"
	"os"
	"strings"
)

type Recorder interface {
	Panicked(component string, value any)
}

type RecorderFunc func(component string, value any)

func (f RecorderFunc) Panicked(component string, value any) {
	f(component, value)
}

func Recover(rec Recorder, component string) {
	v := recover()
	if v == nil {
		return
	}

	if rec == nil {
		_, _ = fmt.Fprintf(os.Stderr, "[Probe] %s panicked: %s\n", component, strings.Join(strings.Fields(fmt.Sprint(v)), " "))

		return
	}

	rec.Panicked(component, v)
}
