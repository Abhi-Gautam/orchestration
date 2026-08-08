package workflows

import (
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestrationv1 "orchestration/gen/orchestration/v1"
	"orchestration/internal/activities"
)

func ConditionalBranchWorkflow(ctx workflow.Context, input *orchestrationv1.ConditionalBranchRequest) (*orchestrationv1.ConditionalBranchResult, error) {
	if input == nil {
		return nil, invalidRequest("INVALID_CONDITIONAL_BRANCH_REQUEST", "input is required")
	}
	orderID := strings.TrimSpace(input.OrderId)
	sku := strings.TrimSpace(input.Sku)
	if orderID == "" {
		return nil, invalidRequest("INVALID_CONDITIONAL_BRANCH_REQUEST", "order_id is required")
	}
	if sku == "" {
		return nil, invalidRequest("INVALID_CONDITIONAL_BRANCH_REQUEST", "sku is required")
	}
	if input.Quantity <= 0 {
		return nil, invalidRequest("INVALID_CONDITIONAL_BRANCH_REQUEST", "quantity must be greater than zero")
	}
	if input.AvailableStock < 0 {
		return nil, invalidRequest("INVALID_CONDITIONAL_BRANCH_REQUEST", "available_stock must not be negative")
	}

	status, err := newStatusTracker(ctx, 2, "checking-inventory", "check-inventory", "Checking inventory")
	if err != nil {
		return nil, err
	}

	startedAt := workflow.Now(ctx)
	status.scheduleWork(1)
	baseOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}

	// Runtime condition: inventory observed by an Activity.
	var inventory activities.CheckInventoryResult
	checkCtx := workflow.WithActivityOptions(ctx, withActivityID(baseOptions, "check-inventory"))
	if err := workflow.ExecuteActivity(checkCtx, activities.CheckInventory, activities.CheckInventoryInput{
		SKU:               sku,
		RequestedQuantity: input.Quantity,
		AvailableStock:    input.AvailableStock,
	}).Get(ctx, &inventory); err != nil {
		return nil, err
	}

	result := &orchestrationv1.ConditionalBranchResult{
		OrderId: orderID,
		Inventory: &orchestrationv1.InventorySnapshot{
			Sku:               inventory.SKU,
			RequestedQuantity: inventory.RequestedQuantity,
			AvailableStock:    inventory.AvailableStock,
			InStock:           inventory.InStock,
		},
	}

	// Conditional branch: exactly one of two different Activity types runs.
	if inventory.InStock {
		status.recordSucceeded()
		status.scheduleWork(1)
		status.setRunning("fulfilling", "fulfill-order", "Fulfilling in-stock order")
		var fulfillment activities.FulfillOrderResult
		fulfillCtx := workflow.WithActivityOptions(ctx, withActivityID(baseOptions, "fulfill-order"))
		if err := workflow.ExecuteActivity(fulfillCtx, activities.FulfillOrder, activities.FulfillOrderInput{
			OrderID:  orderID,
			SKU:      sku,
			Quantity: input.Quantity,
		}).Get(ctx, &fulfillment); err != nil {
			return nil, err
		}
		result.Path = orchestrationv1.OrderPath_ORDER_PATH_FULFILL
		result.Fulfillment = &orchestrationv1.FulfillmentOutcome{
			ShipmentId: fulfillment.ShipmentID,
			Status:     fulfillment.Status,
		}
	} else {
		status.recordSucceeded()
		status.scheduleWork(1)
		status.setRunning("backordering", "backorder-order", "Backordering out-of-stock order")
		shortfall := input.Quantity - inventory.AvailableStock
		var backorder activities.BackorderOrderResult
		backorderCtx := workflow.WithActivityOptions(ctx, withActivityID(baseOptions, "backorder-order"))
		if err := workflow.ExecuteActivity(backorderCtx, activities.BackorderOrder, activities.BackorderOrderInput{
			OrderID:   orderID,
			SKU:       sku,
			Quantity:  input.Quantity,
			Shortfall: shortfall,
		}).Get(ctx, &backorder); err != nil {
			return nil, err
		}
		result.Path = orchestrationv1.OrderPath_ORDER_PATH_BACKORDER
		result.Backorder = &orchestrationv1.BackorderOutcome{
			BackorderId: backorder.BackorderID,
			Status:      backorder.Status,
			Shortfall:   backorder.Shortfall,
		}
	}

	finishedAt := workflow.Now(ctx)
	status.recordSucceeded()
	status.setSucceeded("completed", "Conditional branch completed")
	result.StartedAt = timestamppb.New(startedAt)
	result.FinishedAt = timestamppb.New(finishedAt)
	result.Elapsed = durationpb.New(finishedAt.Sub(startedAt))
	return result, nil
}

func withActivityID(base workflow.ActivityOptions, activityID string) workflow.ActivityOptions {
	base.ActivityID = activityID
	return base
}
