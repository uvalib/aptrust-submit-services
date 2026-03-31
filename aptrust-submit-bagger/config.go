package main

import (
	"log"
)

// ServiceConfig defines all the service configuration parameters
type ServiceConfig struct {
	// queue definitions
	InQueueName   string // SQS queue name to monitor for message
	PollTimeOut   int32  // the SQS queue timeout (in seconds)
	HeartbeatTime int32  // the SQS queue heartbeat time (in seconds)

	// ingest details
	InboundBucket   string // the inbound bucket name
	LocalAssetCache string // the root of the local asset cache

	// event bus definitions
	BusName        string // the event bus name
	BusEventSource string // the source of published events

	// other definitions
	SyncWorkers int32  // the number of s3 sync workers
	SourceOrg   string // the organization as reported to APTrust

	// database configuration
	DbHost     string // database host
	DbPort     int    // database port
	DbName     string // database name
	DbUser     string // database user
	DbPassword string // database password
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	// queue definitions
	cfg.InQueueName = ensureSetAndNonEmpty("NOTIFY_IN_QUEUE")
	cfg.PollTimeOut = int32(envToInt("NOTIFY_QUEUE_POLL_TIMEOUT"))
	cfg.HeartbeatTime = int32(envToInt("NOTIFY_QUEUE_HEARTBEAT_TIME"))

	// ingest details
	cfg.InboundBucket = ensureSetAndNonEmpty("INBOUND_BUCKET")
	cfg.LocalAssetCache = ensureSetAndNonEmpty("LOCAL_ASSET_CACHE")

	// event bus definitions
	cfg.BusName = envWithDefault("EVENT_BUS_NAME", "")
	cfg.BusEventSource = envWithDefault("EVENT_SRC_NAME", "")

	// other definitions
	cfg.SyncWorkers = int32(envToInt("SYNC_WORKERS"))
	cfg.SourceOrg = ensureSetAndNonEmpty("SOURCE_ORGANIZATION")

	// database definitions
	cfg.DbHost = ensureSetAndNonEmpty("DB_HOST")
	cfg.DbPort = envToInt("DB_PORT")
	cfg.DbName = ensureSetAndNonEmpty("DB_NAME")
	cfg.DbUser = ensureSetAndNonEmpty("DB_USER")
	cfg.DbPassword = ensureSetAndNonEmpty("DB_PASSWORD")

	// queue definitions
	log.Printf("[CONFIG] InQueueName     = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] PollTimeOut     = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] HeartbeatTime   = [%d]", cfg.HeartbeatTime)

	// ingest details
	log.Printf("[CONFIG] InboundBucket   = [%s]", cfg.InboundBucket)
	log.Printf("[CONFIG] LocalAssetCache = [%s]", cfg.LocalAssetCache)

	// event bus definitions
	log.Printf("[CONFIG] BusName         = [%s]", cfg.BusName)
	log.Printf("[CONFIG] BusEventSource  = [%s]", cfg.BusEventSource)

	// other definitions
	log.Printf("[CONFIG] SyncWorkers     = [%d]", cfg.SyncWorkers)
	log.Printf("[CONFIG] SourceOrg       = [%s]", cfg.SourceOrg)

	// database definitions
	log.Printf("[CONFIG] DbHost          = [%s]", cfg.DbHost)
	log.Printf("[CONFIG] DbPort          = [%d]", cfg.DbPort)
	log.Printf("[CONFIG] DbName          = [%s]", cfg.DbName)
	log.Printf("[CONFIG] DbUser          = [%s]", cfg.DbUser)
	log.Printf("[CONFIG] DbPassword      = [REDACTED]")

	return &cfg
}
