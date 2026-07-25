package main

import (
	"fmt"
	"os"
	"strings"
)

type config struct {
	webAddress        string
	temporalAddress   string
	temporalUI        string
	temporalNamespace string
	taskQueue         string
}

func loadConfig() (config, error) {
	values := make(map[string]string)
	for _, key := range []string{"WEB_ADDRESS", "TEMPORAL_ADDRESS", "TEMPORAL_UI_ADDRESS", "TEMPORAL_NAMESPACE", "TASK_QUEUE"} {
		value := strings.TrimSpace(os.Getenv(key))
		if value == "" {
			return config{}, fmt.Errorf("required environment variable %s is not set", key)
		}
		values[key] = value
	}

	return config{
		webAddress:        values["WEB_ADDRESS"],
		temporalAddress:   values["TEMPORAL_ADDRESS"],
		temporalUI:        strings.TrimRight(values["TEMPORAL_UI_ADDRESS"], "/"),
		temporalNamespace: values["TEMPORAL_NAMESPACE"],
		taskQueue:         values["TASK_QUEUE"],
	}, nil
}
