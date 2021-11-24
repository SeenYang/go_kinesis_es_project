package engine

type Request struct {
	Processor func([]byte) (ProcessResult, error)
	Contents  []byte
}

type ProcessResult struct {
	Items []interface{}
}
