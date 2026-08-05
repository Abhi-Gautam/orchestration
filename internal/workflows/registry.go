package workflows

import "orchestration/internal/workflowcatalog"

func Executions() map[string]any {
	return map[string]any{
		workflowcatalog.GreetingWorkflowName:          GreetingWorkflow,
		workflowcatalog.SimpleDiamondWorkflowName:     SimpleDiamondWorkflow,
		workflowcatalog.DynamicFanOutWorkflowName:     DynamicFanOutWorkflow,
		workflowcatalog.ReusableArtifactWorkflowName:  ReusableArtifactWorkflow,
		workflowcatalog.DurableReportWorkflowName:     DurableReportWorkflow,
		workflowcatalog.FanOutPolicyWorkflowName:      FanOutPolicyWorkflow,
		workflowcatalog.ConditionalBranchWorkflowName: ConditionalBranchWorkflow,
	}
}
