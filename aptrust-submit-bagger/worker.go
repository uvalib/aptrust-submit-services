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

	// S3 assets in <bucket>/<clientId>/<submissionId>/<bag name>/...
	bagKey := fmt.Sprintf("%s/%s/%s", busEvent.ClientId, wf.SubmissionId, wf.BagId)

	// local assets in <cache root>/<clientId>/<submissionId>/<bag name>/...
	localSubmissionRoot := fmt.Sprintf("%s/%s/%s", cfg.LocalAssetCache, busEvent.ClientId, wf.SubmissionId)
	localBagRoot := fmt.Sprintf("%s/%s", localSubmissionRoot, wf.BagId)
	localBagName := fmt.Sprintf("%s/%s.tar", localSubmissionRoot, wf.BagId)

	// do the sync
	err = syncAssets(cfg.InboundBucket, bagKey, localBagRoot, cfg.SyncWorkers)
	if err != nil {
		done <- false
		return
	}

	// do the bagging
	err = bagAssets(localSubmissionRoot, wf.BagId, localBagName)
	if err != nil {
		done <- false
		return
	}

	// we are done, publish the appropriate event and terminate
	_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventBagBuilt, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)", duration.Seconds())
	done <- true
}

//
// end of file
//
