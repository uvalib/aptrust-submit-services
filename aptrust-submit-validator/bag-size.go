package main

import "log"

// largest bag, 4TB
var maxBagSize = uint64(4096 * 1024 * 1024 * 1024)

func excessiveBagSize(itemized []ManifestRow) bool {

	// go through the list and accumulate the size of each bag
	bagSizes := make(map[string]uint64)
	for _, f := range itemized {
		bagSizes[f.bag] += uint64(f.size)
	}

	// ensure no bag is larger than the maximum allowed
	excessive := false
	for n, v := range bagSizes {
		if v > maxBagSize {
			log.Printf("ERROR: bag [%s] is too large (%d bytes)", n, v)
			excessive = true
		}
	}

	return excessive == true
}

//
// end of file
//
