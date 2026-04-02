package main

import (
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func recordConflict(dao *uvaaptsdao.Dao, file uvaaptsdao.File) error {

	// initially assume the conflict is with an APTrust file
	aptConflicts, err := dao.GetAptFilesByHash(file.Hash)
	if err != nil {
		//if errors.As(err, &uvaaptsdao.ErrFileNotFound) == false {
		return err
		//}
	}

	// assume the first conflict in the event that we have more than one
	conflict := aptConflicts[0]
	if len(aptConflicts) > 1 {
		log.Printf("WARNING: multiple APTrust hash conflicts for <%s/%s>, tracking the first", file.BagName, file.Name)
	}

	return dao.AddConflict(file.Submission, file.Id, "aptrust", conflict.Id)
}

//
// end of file
//
