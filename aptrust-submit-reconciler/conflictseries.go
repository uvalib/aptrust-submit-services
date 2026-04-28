package main

import (
	"log"

	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

type IgnorableConflict struct {
	file    uvaaptsdao.File
	ignored bool
}

type ConflictSet struct {
	localFile         IgnorableConflict
	possibleConflicts []IgnorableConflict
}

type ConflictSeries struct {
	conflicts []ConflictSet
	dao       *uvaaptsdao.Dao
}

// we have a list of files where conflicts exist so now generate a complete list of
// the conflicts. A file in our conflict list will have one or more conflicts.
func newConflictSeries(dao *uvaaptsdao.Dao, conflicts []uvaaptsdao.File) (*ConflictSeries, error) {

	res := ConflictSeries{
		conflicts: make([]ConflictSet, len(conflicts)),
		dao:       dao,
	}

	for ix, conflict := range conflicts {

		aptConflicts, err := dao.GetAptFilesByHash(conflict.Hash)
		if err != nil {
			log.Printf("ERROR: getting APTrust conflicts (%s)", err.Error())
			return nil, err
		}

		res.conflicts[ix] = ConflictSet{
			localFile:         ignorable(conflict),
			possibleConflicts: makeIgnorable(aptConflicts),
		}
	}

	return &res, nil
}

// record our conflict set
func (cs ConflictSeries) record() error {

	//log.Printf("WARNING: %d conflicts for <%s:%s> (hash: %s)", len(conflict.conflicts), conflict.local.BagName, conflict.local.Name, conflict.local.Hash)

	for _, csc := range cs.conflicts {
		log.Printf("INFO: conflicts for <%s:%s> (hash: %s)", csc.localFile.file.BagName, csc.localFile.file.Name, csc.localFile.file.Hash)
		for _, pc := range csc.possibleConflicts {

			// either the top level local file is ignored (probably in the hash allow list) or
			// the specific conflict is ignored
			ignored := csc.localFile.ignored || pc.ignored

			log.Printf("INFO: <%s:%s> (ignored: %t)", pc.file.BagName, pc.file.Name, ignored)
			err := cs.dao.AddConflict(csc.localFile.file.Submission, csc.localFile.file.Id, "aptrust", pc.file.Id, ignored)
			if err != nil {
				log.Printf("ERROR: adding conflict record (%s)", err.Error())
				return err
			}
		}
	}

	return nil
}

// are there any conflicts outstanding
func (cs ConflictSeries) outstanding() bool {

	for _, c := range cs.conflicts {
		if c.localFile.ignored == false {
			for _, p := range c.possibleConflicts {
				if p.ignored == false {
					return true
				}
			}
		}
	}
	return false
}

func makeIgnorable(files []uvaaptsdao.File) []IgnorableConflict {
	res := make([]IgnorableConflict, len(files))
	for ix, f := range files {
		res[ix] = ignorable(f)
	}
	return res
}

func ignorable(file uvaaptsdao.File) IgnorableConflict {
	return IgnorableConflict{
		file:    file,
		ignored: false,
	}
}

//
// end of file
//
