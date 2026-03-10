package main

import (
	"fmt"
	"log"
	"time"

	"github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus"
	//"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func worker(done chan<- bool, cfg *ServiceConfig, busEvent *uvaaptsbus.UvaBusEvent) {

	start := time.Now()
	log.Printf("INFO: worker starting")

	// ensure this is the type of event we want to process
	switch busEvent.EventName {
	case uvaaptsbus.EventBagInitiate:
	default:
		log.Printf("ERROR: unexpected event type (%s), ignoring", busEvent.EventName)
		done <- true
		return
	}

	// make the workflow event
	wf, err := uvaaptsbus.MakeWorkflowEvent(busEvent.Detail)
	if err != nil {
		log.Printf("ERROR: unmarshaling workflow event (%s)", err.Error())
		done <- false
		return
	}

	log.Printf("INFO: EVENT %s / %s", busEvent.String(), wf.String())

	// create our event bus client
	eventBus, _ := NewEventBus(cfg.BusName, cfg.BusEventSource)

	// S3 assets in <bucket>/<clientId>/<submissionId>/...
	submissionKeyPrefix := fmt.Sprintf("%s/%s", busEvent.ClientId, wf.SubmissionId)

	// local assets in <cache root>/<clientId>/<submissionId>/...
	localAssets := fmt.Sprintf("%s/%s/%s", cfg.LocalAssetCache, busEvent.ClientId, wf.SubmissionId)

	// do the sync
	err = syncAssets(cfg.InboundBucket, submissionKeyPrefix, localAssets, cfg.SyncWorkers)
	if err != nil {
		//log.Printf("ERROR: unmarshaling workflow event (%s)", err.Error())
		done <- false
		return
	}

	// FIXME do bagging here...

	// we are done, publish the appropriate event and terminate
	_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventBagBuilt, busEvent.ClientId, wf.SubmissionId, wf.BagId)

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)", duration.Seconds())
	done <- true
}

//
// end of file
//
