package engine

type Request struct {
	Processor  func([][]byte) (ProcessResult, error)
}

type ProcessResult struct {
	Items []interface{}
}