package main

import (
	"errors"
	"log"
	"time"

	"github.com/uvalib/aptrust-submit-bus-definitions/uvaaptsbus"
	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func worker(done chan<- bool, cfg *ServiceConfig, busEvent *uvaaptsbus.UvaBusEvent) {

	start := time.Now()
	log.Printf("INFO: worker starting")

	// ensure this is the type of event we want to process
	switch busEvent.EventName {
	case uvaaptsbus.EventSubmissionReconcile:
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

	log.Printf("INFO: event %s/%s", busEvent.String(), wf.String())

	// create our event bus client
	eventBus, _ := NewEventBus(cfg.BusName, cfg.BusEventSource)

	// create the data access object
	dao, err := uvaaptsdao.NewDao(cfg.DbHost, cfg.DbPort, cfg.DbUser, cfg.DbPassword, cfg.DbName)
	if err != nil {
		log.Printf("ERROR: connecting to the database (%s)", err.Error())
		done <- false
		return
	}

	// cleanup on exit
	defer dao.Close()

	// get all the files that conflict for this submission
	conflicts, err := dao.GetConflictFilesBySubmission(wf.SubmissionId)
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			log.Printf("ERROR: getting submission conflict file set (%s)", err.Error())
			done <- false
			return
		}
	}

	if len(conflicts) > 0 {
		log.Printf("INFO: %d possible files conflicting", len(conflicts))

		// for every conflict file in the submission, determine if we can ignore it by checking on the
		// whitelist
		conflicts, err = supressWhitelisted(dao, conflicts)
		if err != nil {
			done <- false
			return
		}

		// then determine if we can ignore it because it is duplicating a previously submitted
		// bag
		conflicts, err = supressBagDuplicates(dao, conflicts)
		if err != nil {
			done <- false
			return
		}

		// if conflicts remain
		if len(conflicts) > 0 {
			for _, f := range conflicts {
				_ = recordConflict(dao, f)
			}
			_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionReconcileFail, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
		} else {
			log.Printf("INFO: all file conflicts for submission have been ignored")
			_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionApprove, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
		}
	} else {
		log.Printf("INFO: no conflicting files found for submission")
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionApprove, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
	}

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)", duration.Seconds())
	done <- true
}

//
// end of file
//
