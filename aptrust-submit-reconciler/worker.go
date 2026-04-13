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

	// get all the files from this submission that have hash conflicts
	filesWithConflicts, err := dao.GetConflictFilesBySubmission(wf.SubmissionId)
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			log.Printf("ERROR: getting submission conflict file set (%s)", err.Error())
			done <- false
			return
		}
	}

	// if we have files with conflicts
	if len(filesWithConflicts) > 0 {
		log.Printf("INFO: %d possible file(s) with conflicts", len(filesWithConflicts))

		// for every conflict file in the submission, determine if we can ignore it by
		// checking on the hash allow list
		filesWithConflicts, err = supressHashAllow(dao, filesWithConflicts)
		if err != nil {
			done <- false
			return
		}

		// do we still have conflicts
		if len(filesWithConflicts) > 0 {

			// we have a list of files where conflicts exist so now generate a complete list of
			// the conflicts. A file in our conflict list will have one or more conflicts.
			conflictSet, err := generateConflictSet(dao, filesWithConflicts)
			if err != nil {
				done <- false
				return
			}

			// suppress conflicts because they are from a previously submitted
			// bag
			conflictSet, err = supressBagDuplicates(dao, conflictSet)
			if err != nil {
				done <- false
				return
			}

			// suppress conflicts because they come from bags we have determined are
			// rubbish
			conflictSet, err = supressBagAllow(dao, conflictSet)
			if err != nil {
				done <- false
				return
			}

			//
			// more suppressions here
			//

			// if conflicts remain
			if len(conflictSet) > 0 {
				for _, f := range conflictSet {
					_ = recordConflict(dao, f)
				}
				log.Printf("WARNING: submission [%s] FAILS reconciliation", wf.SubmissionId)
				_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionReconcileFail, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
			} else {
				log.Printf("INFO: all conflicts for submission [%s] have been ignored", wf.SubmissionId)
				_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionApprove, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
			}
		} else {
			log.Printf("INFO: all conflicts for submission [%s] have been suppressed", wf.SubmissionId)
			_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionApprove, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
		}
	} else {
		log.Printf("INFO: no conflicts found for submission [%s]", wf.SubmissionId)
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionApprove, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
	}

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %d ms)", duration.Milliseconds())
	done <- true
}

//
// end of file
//
