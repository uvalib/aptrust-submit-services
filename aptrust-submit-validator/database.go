//
//
//

package main

import (
	"log"
	"strings"
	"time"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

// create the bags in the database
func createDBBags(dao *uvaaptsdao.Dao, manifestList []string, sid string) error {

	start := time.Now()

	// create the bags
	for _, m := range manifestList {
		tok := strings.SplitN(m, "/", 2)
		err := dao.AddBag(tok[0], sid)
		if err != nil {
			log.Printf("ERROR: adding bag to database (%s)", err.Error())
			return err
		}
	}

	duration := time.Since(start)
	log.Printf("INFO: DB bags created (elapsed %d ms)", duration.Milliseconds())
	return nil
}

// create the files in the database
func createDBFiles(dao *uvaaptsdao.Dao, fileList []ManifestRow, sid string) error {

	start := time.Now()

	// create the files
	for _, mr := range fileList {
		// FIXME
		err := dao.AddFile(mr.file, mr.bag, sid, mr.hash, 9999)
		if err != nil {
			log.Printf("ERROR: adding file to database (%s)", err.Error())
			return err
		}
	}

	duration := time.Since(start)
	log.Printf("INFO: DB files created (elapsed %d ms)", duration.Milliseconds())
	return nil
}

//
// end of file
//
