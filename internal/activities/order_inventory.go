package activities

import "fmt"

// CheckInventoryInput is what the Workflow asks inventory to evaluate.
// AvailableStock is a lab fixture simulating a warehouse read.
type CheckInventoryInput struct {
	SKU               string
	RequestedQuantity int32
	AvailableStock    int32
}

// CheckInventoryResult is the runtime condition the Workflow branches on.
type CheckInventoryResult struct {
	SKU               string
	RequestedQuantity int32
	AvailableStock    int32
	InStock           bool
}

// CheckInventory observes stock for a SKU and reports whether the order can ship now.
func CheckInventory(input CheckInventoryInput) (CheckInventoryResult, error) {
	return CheckInventoryResult{
		SKU:               input.SKU,
		RequestedQuantity: input.RequestedQuantity,
		AvailableStock:    input.AvailableStock,
		InStock:           input.AvailableStock >= input.RequestedQuantity,
	}, nil
}

type FulfillOrderInput struct {
	OrderID  string
	SKU      string
	Quantity int32
}

type FulfillOrderResult struct {
	OrderID    string
	ShipmentID string
	Status     string
}

// FulfillOrder is the in-stock path. Distinct Activity type from BackorderOrder.
func FulfillOrder(input FulfillOrderInput) (FulfillOrderResult, error) {
	return FulfillOrderResult{
		OrderID:    input.OrderID,
		ShipmentID: fmt.Sprintf("ship-%s", input.OrderID),
		Status:     "fulfilled",
	}, nil
}

type BackorderOrderInput struct {
	OrderID   string
	SKU       string
	Quantity  int32
	Shortfall int32
}

type BackorderOrderResult struct {
	OrderID     string
	BackorderID string
	Status      string
	Shortfall   int32
}

// BackorderOrder is the out-of-stock path. Distinct Activity type from FulfillOrder.
func BackorderOrder(input BackorderOrderInput) (BackorderOrderResult, error) {
	return BackorderOrderResult{
		OrderID:     input.OrderID,
		BackorderID: fmt.Sprintf("bo-%s", input.OrderID),
		Status:      "backordered",
		Shortfall:   input.Shortfall,
	}, nil
}
