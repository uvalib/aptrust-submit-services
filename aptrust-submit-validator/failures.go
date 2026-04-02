package main

import (
	"github.com/uvalib/aptrust-submit-db-dao/uvaaptsdao"
)

func recordFailure(dao *uvaaptsdao.Dao, sid string, reason string) error {
	return dao.AddFailure(sid, reason)
}

//
// end of file
//
