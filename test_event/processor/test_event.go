package processor

import (
	"encoding/json"
	"go_kinesis_es_project/engine"
	"go_kinesis_es_project/test_event/models"
)

func ProcessTestEvent(contents []byte) (engine.ProcessResult, error) {
	result := engine.ProcessResult{}

	var event models.TestEvent
	err := json.Unmarshal(contents, &event)
	if err != nil {
		return engine.ProcessResult{}, err
	}

	result.Items = append(result.Items, event)

	return result, nil

}
