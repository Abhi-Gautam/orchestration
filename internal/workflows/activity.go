package workflows

import "go.temporal.io/sdk/workflow"

// scheduledActivity keeps a Future with the identity a Workflow needs when it settles.
// Selector callbacks fire in completion order, so the branch index and Activity ID have to
// travel with the Future rather than being inferred from the order results arrive in.
type scheduledActivity struct {
	index      int
	activityID string
	future     workflow.Future
}

// withActivityID copies shared Activity options for one Activity, so a Workflow states its
// deadline and retry policy once instead of restating every field per Activity.
func withActivityID(base workflow.ActivityOptions, activityID string) workflow.ActivityOptions {
	base.ActivityID = activityID
	return base
}
