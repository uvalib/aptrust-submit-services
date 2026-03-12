package main

func validateChecksum(reported string, expected string) bool {

	//log.Printf("expected [%s], actual [%s]", expected, reported)

	// easier than I thought right?
	return expected == reported
}

//
// end of file
//
