package main

import (
	"fmt"
	"log"
	"path/filepath"
	"slices"
	"strings"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func enumerateFailure(dao *uvaaptsdao.Dao, sid string, supplied []string, manifests []string, itemized []ManifestRow, prefix string) error {

	// create a list of strings for the supplied removing the manifest files
	suppliedList := make([]string, 0)
	for _, s := range supplied {
		fn := strings.TrimPrefix(s, prefix+string(filepath.Separator))
		if slices.Contains(manifests, fn) == false {
			//log.Printf("INFO: supplied %s", fn)
			suppliedList = append(suppliedList, fn)
		}
	}

	// create a list of strings for the itemized
	itemizedList := make([]string, 0)
	for _, i := range itemized {
		fn := filepath.Join(i.bag, i.file)
		//log.Printf("INFO: itemized %s", fn)
		itemizedList = append(itemizedList, fn)
	}

	// if the manifests specified fewer files than were actually supplied
	if len(itemizedList)+len(manifests) < len(suppliedList) {

		// go through the files supplied and log each one that was not included in the manifest(s)
		for _, s := range suppliedList {
			//log.Printf("INFO: Checking for EXTRA %s", s)
			if slices.Contains(itemizedList, s) == false {
				failureReason := fmt.Sprintf("%s was supplied but does not appear in a manifest", s)
				log.Printf("ERROR: %s", failureReason)
				_ = recordFailure(dao, sid, failureReason)
			}
		}
	} else {
		// go through the items specified and log each one that was not supplied
		for _, i := range itemizedList {
			//log.Printf("INFO: Checking for MISSING %s", i)
			if slices.Contains(suppliedList, i) == false {
				failureReason := fmt.Sprintf("%s appears in a manifest but was NOT supplied", i)
				log.Printf("ERROR: %s", failureReason)
				_ = recordFailure(dao, sid, failureReason)
			}
		}
	}

	return nil
}

func recordFailure(dao *uvaaptsdao.Dao, sid string, reason string) error {
	return dao.AddFailure(sid, reason)
}

//
// end of file
//
