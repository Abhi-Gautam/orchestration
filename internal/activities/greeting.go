package activities

import "fmt"

// FormatGreeting is deliberately small: its log/output will be visible from a
// Temporal Workflow execution, proving that a Workflow dispatched an Activity.
func FormatGreeting(name string) (string, error) {
	return fmt.Sprintf("Hello, %s! Temporal completed the workflow.", name), nil
}
