package workflows

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

const artifactActivityTimeoutMargin = 30 * time.Second

func artifactActivityID(index int) string {
	return fmt.Sprintf("artifact-%03d", index)
}

func artifactActivityIDs(count int) string {
	ids := make([]string, count)
	for index := range ids {
		ids[index] = artifactActivityID(index)
	}
	return strings.Join(ids, ",")
}

func validateArtifactGenerationInput(experimentID, activityVersion string, heavyWork *durationpb.Duration) (time.Duration, error) {
	if !validExperimentID(experimentID) {
		return 0, errors.New("experiment ID must be 1-128 characters using letters, numbers, dot, underscore, or hyphen")
	}
	if activityVersion == "" || activityVersion != strings.TrimSpace(activityVersion) {
		return 0, errors.New("activity version is required and must not have surrounding whitespace")
	}
	if heavyWork == nil || heavyWork.CheckValid() != nil {
		return 0, errors.New("heavy-work duration must be a valid protobuf duration")
	}
	heavyWorkDuration := heavyWork.AsDuration()
	if heavyWorkDuration <= 0 {
		return 0, errors.New("heavy-work duration must be positive")
	}
	if heavyWorkDuration > time.Duration(1<<63-1)-artifactActivityTimeoutMargin {
		return 0, errors.New("heavy-work duration is too large to configure a safe Activity timeout")
	}
	return heavyWorkDuration, nil
}

func validExperimentID(value string) bool {
	if len(value) == 0 || len(value) > 128 || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}
