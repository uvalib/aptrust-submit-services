//
//
//

package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

type ManifestRow struct {
	hash string
	file string
	bag  string
}

func manifestContents(s3client *uvaS3Client, bucket string, prefix string, manifest string) ([]ManifestRow, error) {

	bag := strings.Split(manifest, "/")[0]
	manifestKey := fmt.Sprintf("%s/%s/%s", prefix, bag, manifestName)
	localName := fmt.Sprintf("%s/%s-%s", tempFilesystem, bag, manifestName)

	// get the manifest
	err := s3client.s3Get(bucket, manifestKey, localName)
	if err != nil {
		log.Printf("ERROR: getting manifest [%s] (%s)", manifestKey, err.Error())
		return nil, err
	}

	lines, err := readFile(localName)
	if err != nil {
		log.Printf("ERROR: reading manifest [%s] (%s)", manifestKey, err.Error())
		return nil, err
	}

	//
	// manifests are a hash followed by two spaces followed by a filename (which could contain spaces)
	//

	results := make([]ManifestRow, 0)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		subs := strings.SplitN(line, " ", 2)
		if len(subs) == 2 {
			hash := strings.TrimSpace(subs[0])
			name := strings.TrimSpace(subs[1])
			ml := ManifestRow{hash: hash, file: name, bag: bag}
			results = append(results, ml)
		}
	}

	return results, nil
}

func readFile(path string) ([]string, error) {

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return strings.Split(string(content), "\n"), nil
}

//
// end of file
//
