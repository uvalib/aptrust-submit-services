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

	// event bus definitions
	BusName        string // the event bus name
	BusEventSource string // the source of published events

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

	// event bus definitions
	cfg.BusName = ensureSetAndNonEmpty("EVENT_BUS_NAME")
	cfg.BusEventSource = ensureSetAndNonEmpty("EVENT_SRC_NAME")

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

	// event bus definitions
	log.Printf("[CONFIG] BusName         = [%s]", cfg.BusName)
	log.Printf("[CONFIG] BusEventSource  = [%s]", cfg.BusEventSource)

	// database definitions
	log.Printf("[CONFIG] DbHost          = [%s]\n", cfg.DbHost)
	log.Printf("[CONFIG] DbPort          = [%d]\n", cfg.DbPort)
	log.Printf("[CONFIG] DbName          = [%s]\n", cfg.DbName)
	log.Printf("[CONFIG] DbUser          = [%s]\n", cfg.DbUser)
	log.Printf("[CONFIG] DbPassword      = [REDACTED]\n")

	return &cfg
}
