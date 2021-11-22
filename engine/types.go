package engine

type Request struct {
	StreamName string
	Processor  func([][]byte) (ProcessResult, error)
}

type ProcessResult struct {
	Items []interface{}
}

type AwsConfig struct {
	Stream          string
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}
