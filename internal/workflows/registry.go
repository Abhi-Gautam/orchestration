package workflows

import "orchestration/internal/workflowcatalog"

// ChildExecutions are Workflows a product never starts directly. They are registered with
// the Worker but deliberately absent from the catalog, so the catalog stays the list of
// operations a caller can ask for.
func ChildExecutions() map[string]any {
	return map[string]any{
		workflowcatalog.TrainingShardWorkflowName: TrainingShardWorkflow,
	}
}

func Executions() map[string]any {
	return map[string]any{
		workflowcatalog.GreetingWorkflowName:          GreetingWorkflow,
		workflowcatalog.SimpleDiamondWorkflowName:     SimpleDiamondWorkflow,
		workflowcatalog.DynamicFanOutWorkflowName:     DynamicFanOutWorkflow,
		workflowcatalog.ReusableArtifactWorkflowName:  ReusableArtifactWorkflow,
		workflowcatalog.DurableReportWorkflowName:     DurableReportWorkflow,
		workflowcatalog.FanOutPolicyWorkflowName:      FanOutPolicyWorkflow,
		workflowcatalog.ConditionalBranchWorkflowName: ConditionalBranchWorkflow,
		workflowcatalog.TrainingJobWorkflowName:       TrainingJobWorkflow,
	}
}
