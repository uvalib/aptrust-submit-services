//
//
//

package main

import (
	"log"
	"strings"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

// create the bags in the database
func createDBBags(dao *uvaaptsdao.Dao, manifestList []string, sid string) error {

	// create the bags
	for _, m := range manifestList {
		tok := strings.SplitN(m, "/", 2)
		err := dao.AddBag(tok[0], sid)
		if err != nil {
			log.Printf("ERROR: adding bag to database (%s)", err.Error())
			return err
		}
	}
	return nil
}

// create the files in the database
func createDBFiles(dao *uvaaptsdao.Dao, fileList []ManifestRow, sid string) error {

	// create the files
	for _, mr := range fileList {
		err := dao.AddFile(mr.file, mr.hash, sid, mr.bag)
		if err != nil {
			log.Printf("ERROR: adding file to database (%s)", err.Error())
			return err
		}
	}
	return nil
}

//
// end of file
//
