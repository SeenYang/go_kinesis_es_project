package engine

type Request struct {
	StreamName string
	Processor  func([][]byte) (ProcessResult, error)
}

type ProcessResult struct {
	Items []interface{}
}
