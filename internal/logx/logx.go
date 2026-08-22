// Package logx provides the small amount of terminal colouring the demo
// binaries share. Keeping it in one place stops the three services from
// drifting apart on which escape code means "error".
package logx

const (
	Reset   = "\033[0m"
	Red     = "\033[31m"
	Green   = "\033[32m"
	Yellow  = "\033[33m"
	Blue    = "\033[34m"
	Magenta = "\033[35m"
	Cyan    = "\033[36m"
)

// Paint wraps s in the given colour. Safe to nest inside a format string
// because it always resets afterwards.
func Paint(color, s string) string {
	return color + s + Reset
}
