package main

import (
	"log"
	"time"
)

func worker(done chan<- bool, cfg *ServiceConfig) {

	log.Println("goroutine running (very slow)")
	time.Sleep(1000 * time.Second)
	log.Println("goroutine done")
	done <- true
}

//
// end of file
//
