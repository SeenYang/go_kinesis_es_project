module go_kinesis/producer

go 1.17

require (
	github.com/aws/aws-sdk-go v1.42.6
	github.com/joho/godotenv v1.4.0
)

require github.com/jmespath/go-jmespath v0.4.0 // indirect

replace go_kinesis/models => ./../models

require (
	github.com/dustin/go-humanize v1.0.0
	go_kinesis/models v0.0.0
)
