package main

import (
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

type ConflictTuple struct {
	local     uvaaptsdao.File
	conflicts []uvaaptsdao.File
}

func generateConflictSet(dao *uvaaptsdao.Dao, conflicts []uvaaptsdao.File) ([]ConflictTuple, error) {

	// we have a list of files where conflicts exist so now generate a complete list of
	// the conflicts. A file in our conflict list will have one or more conflicts.

	conflictSet := make([]ConflictTuple, len(conflicts))

	for ix, conflict := range conflicts {

		aptConflicts, err := dao.GetAptFilesByHash(conflict.Hash)
		if err != nil {
			log.Printf("ERROR: getting apt conflicts (%s)", err.Error())
			return nil, err
		}

		conflictSet[ix] = ConflictTuple{
			local:     conflict,
			conflicts: aptConflicts,
		}
	}
	return conflictSet, nil
}

func recordConflict(dao *uvaaptsdao.Dao, conflict ConflictTuple) error {

	log.Printf("WARNING: %d conflicts for <%s:%s> (hash: %s)", len(conflict.conflicts), conflict.local.BagName, conflict.local.Name, conflict.local.Hash)

	for ix, c := range conflict.conflicts {
		log.Printf("WARNING: conflict %d: <%s:%s>", ix+1, c.BagName, c.Name)
		err := dao.AddConflict(conflict.local.Submission, conflict.local.Id, "aptrust", c.Id)
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
