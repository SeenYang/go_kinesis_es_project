package processor

import (
	"encoding/json"
	"go_kinesis_es_project/engine"
	"go_kinesis_es_project/models"
)

func ProcessTestEvent(contents [][]byte) (engine.ProcessResult, error) {
	result := engine.ProcessResult{}

	for _, e := range contents {
		var event models.Event
		err := json.Unmarshal(e, &event)
		if err != nil {
			return engine.ProcessResult{}, err
		}

		result.Items = append(result.Items, event)

	}

	return result, nil

}
