package main

import (
	"log"
	"time"

	"github.com/uvalib/apts-bus-definitions/uvaaptsbus"
)

func worker(done chan<- bool, cfg *ServiceConfig, busEvent *uvaaptsbus.UvaBusEvent) {

	// make the workflow event
	wf, err := uvaaptsbus.MakeWorkflowEvent(busEvent.Detail)
	if err != nil {
		log.Printf("ERROR: unmarshaling workflow event (%s)", err.Error())
		done <- false
		return
	}

	log.Printf("INFO: EVENT %s / %s", busEvent.String(), wf.String())

	log.Println("goroutine running (very slow)")
	time.Sleep(1000 * time.Second)
	log.Println("goroutine done")
	done <- true
}

//
// end of file
//
