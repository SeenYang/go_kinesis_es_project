package scheduler

import "go_kinesis_es_project/engine"

type SimpleScheduler struct {
	workerChan chan engine.Request
}

func (s *SimpleScheduler) ConfigureMasterWorkerChan(requests chan engine.Request) {
	s.workerChan = requests
}

func (s *SimpleScheduler) Submit(request engine.Request) {
	// send request down to worker chan
	// If there's no go func, `in` and `out` will circular depending.
	go func() { s.workerChan <- request }()
}
