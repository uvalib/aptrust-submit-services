package main

import (
	"encoding/json"
	"log"

	"github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus"
)

func NewEventBus(eventBus string, eventSource string) (uvaaptsbus.UvaBus, error) {
	// we will accept bad config and return nil quietly
	if len(eventBus) == 0 {
		log.Printf("INFO: Event bus is not configured, no telemetry emitted")
		return nil, uvaaptsbus.ErrConfig
	}

	cfg := uvaaptsbus.UvaBusConfig{BusName: eventBus, Source: eventSource, Log: nil}
	return uvaaptsbus.NewUvaBus(cfg)
}

func publishWorkflowEvent(bus uvaaptsbus.UvaBus, eventName string, clientId string, submissionId string, bagId string) error {
	if bus == nil {
		return uvaaptsbus.ErrConfig
	}
	detail, _ := workflowPayload(submissionId, bagId)
	ev := uvaaptsbus.UvaBusEvent{
		EventName: eventName,
		ClientId:  clientId,
		Detail:    detail,
	}
	return bus.PublishEvent(&ev)
}

func workflowPayload(submissionId string, bagId string) (json.RawMessage, error) {
	pl := uvaaptsbus.UvaWorkflowEvent{SubmissionId: submissionId, BagId: bagId}
	return pl.Serialize()
}

//
// end of file
//
