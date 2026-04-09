package main

import (
	"bytes"
	"crypto/md5"
	"embed"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"text/template"
	"time"
)

// templates holds our email templates
//
//go:embed templates/*
var templates embed.FS

// internal stuff
var descriptionFileName = "aptrust-description.txt"
var titleFileName = "aptrust-title.txt"
var defaultPermissions = os.FileMode(0744)

// bag asset names
var manifestName = "manifest-md5.txt"
var tagManifestName = "tagmanifest-md5.txt"
var aptrustInfoName = "aptrust-info.txt"
var bagInfoName = "bag-info.txt"
var bagitName = "bagit.txt"
var tagFiles = []string{aptrustInfoName, bagInfoName, manifestName}

// attributes required by the templates used in the bagging process
type BaggingAttributes struct {
	SourceOrganization string
	BagGroupIdentifier string
	Date               string
	Description        string
	SenderDescription  string
	SenderIdentifier   string
	Storage            string
	Title              string
}

//
// assets have been sync'd into the data/... folder, we need to remove and process what should not be there,
// write the remaining assets and generate the tarfile (bag) in preparation for APTrust submission
//

func bagAssets(root string, bagname string, outfile string, attribs BaggingAttributes) error {

	bagDir := path.Join(root, bagname)

	// bagit.txt file
	err := addFileFromTemplate(bagDir, bagitName, attribs)
	if err != nil {
		return err
	}

	// aptrust-info.txt file
	err = addFileFromTemplate(bagDir, aptrustInfoName, attribs)
	if err != nil {
		return err
	}

	// bag-info.txt file
	err = addFileFromTemplate(bagDir, bagInfoName, attribs)
	if err != nil {
		return err
	}

	// update the manifest file
	err = updateManifest(bagDir, manifestName)
	if err != nil {
		return err
	}

	// and add the tag manifest
	err = addTagManifest(bagDir, tagManifestName, tagFiles)
	if err != nil {
		return err
	}

	return makeTarfile(root, bagname, outfile)
}

// read the existing manifest, we will rewrite it and remove spurious info
func updateManifest(root string, filename string) error {

	existing := path.Join(root, "data", filename)
	contents, err := readFile(existing)
	if err != nil {
		return err
	}

	newContents := ""
	for _, line := range contents {
		if len(line) == 0 {
			continue
		}
		tok := strings.SplitN(line, " ", 2)
		if len(tok) == 2 {
			fp := tok[0]
			file := strings.Trim(tok[1], " ")

			// does this file belong in the manifest
			if keepInManifest(file) == true {
				newContents += fmt.Sprintf("%s data/%s\n", fp, file)
			}

		} else {
			log.Printf("WARNING: ignoring badly formed manifest line [%s]", line)
		}
	}

	log.Printf("INFO: removing [%s]", existing)
	_ = os.Remove(existing)
	log.Printf("INFO: writing [%s]", filename)
	return os.WriteFile(path.Join(root, filename), []byte(newContents), defaultPermissions)
}

// render a template and write to the specified output file
func addFileFromTemplate(root string, filename string, attribs BaggingAttributes) error {

	// read and parse the template
	name := fmt.Sprintf("%s.template", filename)
	tmpl, err := readAndParseTemplate(name)
	if err != nil {
		return err
	}

	// render the template
	var renderedBuffer bytes.Buffer
	err = tmpl.Execute(&renderedBuffer, attribs)
	if err != nil {
		log.Printf("ERROR: executing [%s] (%s)", name, err.Error())
		return err
	}

	log.Printf("INFO: writing [%s]", filename)
	return os.WriteFile(path.Join(root, filename), renderedBuffer.Bytes(), defaultPermissions)
}

// create the meta-manifest
func addTagManifest(root string, filename string, tagFiles []string) error {

	data := ""
	for _, tf := range tagFiles {
		src := path.Join(root, tf)
		fp, err := md5Checksum(src)
		if err != nil {
			return err
		}
		data += fmt.Sprintf("%s %s\n", fp, tf)
	}
	log.Printf("INFO: writing [%s]", filename)
	return os.WriteFile(path.Join(root, filename), []byte(data), defaultPermissions)
}

// generate a tarfile of the bag contents
func makeTarfile(root string, bagname string, outfile string) error {

	log.Printf("INFO: creating tarfile [%s]", outfile)

	// shell the command to build a tarfile
	start := time.Now()
	cmdArray := []string{"cvf", outfile, "-C", root, bagname}
	cmd := exec.Command("tar", cmdArray...)
	//log.Printf("INFO: %+v", cmd)
	res, err := cmd.Output()
	if err != nil {
		log.Printf("ERROR: creating tarfile (%s)", err.Error())
		log.Printf("INFO: command output [%s]", string(res))
	}
	duration := time.Since(start)
	log.Printf("INFO: tar completed (elapsed %d ms)", duration.Milliseconds())
	return err
}

// boilerplate for loading and parsing a template
func readAndParseTemplate(templateName string) (*template.Template, error) {

	// read the template
	name := path.Join("templates", templateName)
	templateStr, err := templates.ReadFile(name)
	if err != nil {
		log.Printf("ERROR: reading template [%s] (%s)", name, err.Error())
		return nil, err
	}

	// parse the templateFile
	tmpl, err := template.New(templateName).Parse(string(templateStr))
	if err != nil {
		log.Printf("ERROR: parsing template [%s] (%s)", name, err.Error())
		return nil, err
	}
	return tmpl, nil
}

// some files do not belong in the manifest because we are not sending them to APTrust
func keepInManifest(filename string) bool {
	if strings.HasSuffix(filename, descriptionFileName) {
		return false
	}

	if strings.HasSuffix(filename, titleFileName) {
		return false
	}
	return true
}

// provide md5 fingerprint of the specified file
func md5Checksum(filename string) (string, error) {
	_, err := os.Stat(filename)
	if err == nil {
		data, _ := os.ReadFile(filename)
		res := fmt.Sprintf("%x", md5.Sum(data))
		return res, nil
	}
	log.Printf("ERROR: [%s] does not exist or is not readable", filename)
	return "", err
}

// read the specified file into a slice of strings, contents separated by newlines
func readFile(path string) ([]string, error) {

	content, err := os.ReadFile(path)
	if err != nil {
		log.Printf("ERROR: [%s] does not exist or is not readable", path)
		return nil, err
	}

	return strings.Split(string(content), "\n"), nil
}

//
// end of file
//
