package main

import (
	"log"
	"time"

	"github.com/uvalib/apts-bus-definitions/uvaaptsbus"
)

func worker(done chan<- bool, cfg *ServiceConfig, busEvent *uvaaptsbus.UvaBusEvent) {

	start := time.Now()
	log.Printf("INFO: worker starting")

	// ensure this is the type of event we want to process
	switch busEvent.EventName {
	case uvaaptsbus.EventSubmissionValidate:
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

	log.Printf("DEBUG: worker doing lots of validate stuff")
	time.Sleep(60 * time.Second)

	// we are done, publish the appropriate event and terminate
	eventBus, _ := NewEventBus(cfg.BusName, cfg.BusEventSource)
	_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionReconcile, busEvent.ClientId, wf.SubmissionId, wf.BagId)

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)\n", duration.Seconds())
	done <- true
}

//
// end of file
//
