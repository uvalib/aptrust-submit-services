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

	log.Printf("INFO: EVENT %s / %s", busEvent.String(), wf.String())

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
	files, err := dao.GetConflictFilesBySubmission(wf.SubmissionId)
	if err != nil {
		if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
			log.Printf("ERROR: getting submission conflict file set (%s)", err.Error())
			done <- false
			return
		}
	}

	log.Printf("INFO: %d possible files conflicting", len(files))

	// for every conflict file in the submission, determine if we can ignore it by checking on the
	// whitelist
	failures := make([]uvaaptsdao.File, 0)
	if files != nil {

		// get our whitelisted file set
		whitelist, err := dao.GetWhitelistedFiles()
		if err != nil {
			if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
				log.Printf("ERROR: getting whitelist fileset (%s)", err.Error())
				done <- false
				return
			}
		}

		// if we have whitelisted files, we may be able to remove files from the conflict set
		if whitelist != nil {
			for _, f := range files {
				w := inWhitelist(whitelist, f.Hash)
				if w != nil {
					log.Printf("INFO: hash found in whitelist fileset, ignoring [%s] (%s)", f.Name, w.Comment)
					continue
				}
				failures = append(failures, f)
			}
		} else {
			failures = files
		}
	}

	// we are done, publish the appropriate event and terminate
	if len(failures) > 0 {
		for _, f := range failures {
			log.Printf("WARNING: unsuppressed conflict for <%s/%s>", f.BagName, f.Name)
			err = trackConflict(dao, f)
			if err != nil {
				if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
					log.Printf("ERROR: adding conflict reference (%s)", err.Error())
					//done <- false
					//return
				}
			}
		}
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionReconcileFail, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
	} else {
		log.Printf("INFO: no conflicting files found for submission")
		_ = publishWorkflowEvent(eventBus, uvaaptsbus.EventSubmissionApprove, busEvent.ClientId, wf.SubmissionId, wf.BagId, "")
	}

	duration := time.Since(start)
	log.Printf("INFO: worker terminating (elapsed %0.2f seconds)", duration.Seconds())
	done <- true
}

func inWhitelist(whitelist []uvaaptsdao.WhitelistedFile, hash string) *uvaaptsdao.WhitelistedFile {
	for _, w := range whitelist {
		if w.Hash == hash {
			return &w
		}
	}
	return nil
}

//
// end of file
//
