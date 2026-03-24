module github.com/uvalib/aptrust-submit-bagger

go 1.26.0

require (
	github.com/aws/aws-sdk-go-v2/config v1.32.12
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.24
	github.com/seqsense/s3sync/v2 v2.0.0
	github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus v0.0.0-20260316181657-60b3597dab4a
	github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao v0.0.0-20260320145349-b675729b7117
)

// for local development
//replace github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao => ../../aptrust-submit-db-dao/uvaaptsdao
//replace github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus => ../../aptrust-submit-bus-definitions/uvaaptsbus

require (
	github.com/aws/aws-sdk-go-v2 v1.41.4 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.8 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.12 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.22.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudwatchevents v1.32.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.97.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.9 // indirect
	github.com/aws/smithy-go v1.24.2 // indirect
	github.com/gabriel-vasile/mimetype v1.4.13 // indirect
	github.com/lib/pq v1.12.0 // indirect
)
