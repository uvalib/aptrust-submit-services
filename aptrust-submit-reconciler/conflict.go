package main

import (
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func recordConflict(dao *uvaaptsdao.Dao, file uvaaptsdao.File) error {

	// initially assume the conflict is with an APTrust file
	aptConflicts, err := dao.GetAptFilesByHash(file.Hash)
	if err != nil {
		log.Printf("ERROR: getting apt conflicts (%s)", err.Error())
		return err
	}

	log.Printf("WARNING: %d unsuppressed conflict(s) for <%s/%s>", len(aptConflicts), file.BagName, file.Name)

	for ix, conflict := range aptConflicts {
		log.Printf("WARNING: conflict %d: <%s/%s>", ix+1, conflict.BagName, conflict.Name)
		err = dao.AddConflict(file.Submission, file.Id, "aptrust", conflict.Id)
		if err != nil {
			log.Printf("ERROR: adding conflict record (%s)", err.Error())
			return err
		}
	}

	return nil
}

//
// end of file
//
