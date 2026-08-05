package main

import (
	"log"

	"google.golang.org/protobuf/encoding/protojson"

	"orchestration/internal/workflowcatalog"
)

func workflowCatalog() []catalogWorkflow {
	definitions := workflowcatalog.Definitions()
	catalog := make([]catalogWorkflow, 0, len(definitions))
	for _, definition := range definitions {
		example, err := protojson.Marshal(definition.Example)
		if err != nil {
			log.Printf("marshal example for workflow %q: %v", definition.ID, err)
			continue
		}
		catalog = append(catalog, catalogWorkflow{
			ID:           definition.ID,
			Name:         definition.Name,
			Description:  definition.Description,
			ExampleInput: example,
		})
	}
	return catalog
}
