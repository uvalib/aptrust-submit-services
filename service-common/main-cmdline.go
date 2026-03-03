package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/uvalib/apts-bus-definitions/uvaaptsbus"
	//"github.com/uvalib/apts-bus-definitions/uvaaptsbus"
)

type Event struct {
	Detail json.RawMessage `json:"detail"`
}

// main entry point
func main() {

	log.Printf("===> %s service staring up (version: %s) <===", os.Args[0], Version())

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// Create an SQS client
	sqsClient := newSQSClient()
	// get the queue URL from the name
	queueUrl := queueUrl(sqsClient, cfg.InQueueName)

	// create the worker done channel
	doneChan := make(chan bool, 1)

	for {
		// start of the processing loop
	START:

		//var messages []types.Message
		log.Printf("INFO: checking for new message...")
		msg, err := getSingleMessage(sqsClient, queueUrl, cfg.PollTimeOut)
		if err != nil {
			log.Printf("ERROR: getting message (%s), sleeping and retrying", err.Error())

			// sleep for a while
			time.Sleep(5 * time.Second)

			// try again
			goto START
		}

		if msg != nil {
			log.Printf("INFO: received a messages")

			// convert to an event bridge event
			var event Event
			err := json.Unmarshal([]byte(*msg.Body), &event)
			if err != nil {
				log.Printf("ERROR: unmarshaling event bridge event (%s), continuing", err.Error())
				// try again
				goto START
			}

			// convert to uvaaptsbus event
			ev, err := uvaaptsbus.MakeBusEvent(event.Detail)
			if err != nil {
				log.Printf("ERROR: unmarshaling bus event (%s)", err.Error())
				// try again
				goto START
			}

			// ensure this is the type of event we want to process
			switch ev.EventName {
			case "workflow.submission.validate":
			default:
				log.Printf("ERROR: unexpected event type (%s), ignoring", ev.EventName)
				// try again
				goto START
			}

			// start the worker...
			go worker(doneChan, cfg, ev)

			// wait for the worker to complete, issue regular heartbeats
			for {
				select {
				case val := <-doneChan:
					if val == true {
						log.Printf("INFO: worker done, deleting message")
						err = deleteMessage(sqsClient, queueUrl, msg.ReceiptHandle)
						if err != nil {
							log.Printf("ERROR: deleting message (%s)", err.Error())
						}
					} else {
						log.Printf("WARNING: worker terminates unexpectedly")
					}
					goto START
				case <-time.After(time.Duration(cfg.HeartbeatTime) * time.Second): // Timeout after 2 seconds
					log.Printf("INFO: worker busy, issuing heartbeat")
					err = heartbeatMessage(sqsClient, queueUrl, msg.ReceiptHandle, cfg.HeartbeatTime)
					if err != nil {
						log.Printf("ERROR: issuing heartbeat (%s)", err.Error())
					}
				}
			}
		} else {
			log.Printf("INFO: no messages available")
		}
	}
}

//
// end of file
//
