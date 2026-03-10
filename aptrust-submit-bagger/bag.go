package main

import (
	"log"
	"os/exec"
	"time"
)

func bagAssets(root string, dirname string, outfile string) error {

	// FIXME do proper bagging here...

	return makeTarfile(root, dirname, outfile)
}

func makeTarfile(root string, dirname string, outfile string) error {

	log.Printf("INFO: creating tarfile [%s]", outfile)

	// shell the command to build a tarfile
	start := time.Now()
	cmdArray := []string{"cvf", outfile, "-C", root, dirname}
	cmd := exec.Command("tar", cmdArray...)
	//log.Printf("INFO: %+v", cmd)
	res, err := cmd.Output()
	if err != nil {
		log.Printf("ERROR: creating tarfile (%s)", err.Error())
		log.Printf("INFO: command output [%s]", string(res))
	}
	duration := time.Since(start)
	log.Printf("INFO: tar completed (elapsed %0.2f seconds)", duration.Seconds())
	return err
}

//
// end of file
//
