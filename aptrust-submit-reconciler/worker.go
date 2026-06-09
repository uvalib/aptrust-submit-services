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
	log.Printf("INFO: processing submission [%s]", wf.SubmissionId)

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

	// get all the files from this submission that have hash conflicts with legacy APTrust
	// submissions
	filesWithAptConflicts, err := dao.GetAptHashConflictsBySubmission(wf.SubmissionId)
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			log.Printf("ERROR: getting APTrust submission conflict file set (%s)", err.Error())
			done <- false
			return
		}
		log.Printf("INFO: no APTrust conflicts identified for submission")
	} else {
		log.Printf("INFO: %d possible file(s) with APTrust conflicts", len(filesWithAptConflicts))
	}

	// get all the files from this submission that have hash conflicts with previous local
	// submissions
	filesWithConflicts, err := dao.GetHashConflictsBySubmission(wf.SubmissionId)
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			log.Printf("ERROR: getting local submission conflict file set (%s)", err.Error())
			done <- false
			return
		}
		log.Printf("INFO: no local conflicts identified for submission")
	} else {
		log.Printf("INFO: %d possible file(s) with local conflicts", len(filesWithConflicts))
	}

	// if we have files with conflicts
	conflictCount := len(filesWithAptConflicts) + len(filesWithConflicts)
	if conflictCount > 0 {
		log.Printf("INFO: %d possible file(s) with conflicts", conflictCount)

		// generate the conflict series
		conflictSeries, err := newConflictSeries(dao, filesWithConflicts, filesWithAptConflicts)
		if err != nil {
			done <- false
			return
		}

		// suppress conflicts because we can ignore them by checking on the hash allow list
		conflictSeries, err = ignoreHashAllow(conflictSeries)
		if err != nil {
			done <- false
			return
		}

		// suppress conflicts because they are from a previously submitted
		// bag
		conflictSeries, err = ignoreBagDuplicates(conflictSeries)
		if err != nil {
			done <- false
			return
		}

		// suppress conflicts because they come from bags we have determined are
		// garbage
		conflictSeries, err = ignoreBagAllow(conflictSeries)
		if err != nil {
			done <- false
			return
		}

		// client specific suppressions... rules can be different for conflict resolution for
		// different work types (clients)
		conflictSeries, err = ignoreClientAllow(conflictSeries, busEvent.ClientId)
		if err != nil {
			done <- false
			return
		}

		//
		// add more suppressions here
		//

		// record the conflicts
		err = conflictSeries.record()
		if err != nil {
			done <- false
			return
		}

		// if conflicts remain
		if conflictSeries.outstanding() == true {
			log.Printf("WARNING: submission [%s] FAILS reconciliation", wf.SubmissionId)
			_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionReconcileFail, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
		} else {
			log.Printf("INFO: all conflicts for submission [%s] have been ignored", wf.SubmissionId)
			_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionApprove, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
		}
	} else {
		log.Printf("INFO: no conflicts found for submission [%s]", wf.SubmissionId)
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionApprove, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
	}

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)", duration.Seconds())
	done <- true
}

//
// end of file
//
