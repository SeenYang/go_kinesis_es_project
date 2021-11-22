package scheduler

import "go_kinesis_es_project/engine"

type SimpleScheduler struct {
	workerChan chan engine.Request
}

func (s SimpleScheduler) Submit(request engine.Request) {
	// send request down to worker chan

	s.workerChan <- request
}
