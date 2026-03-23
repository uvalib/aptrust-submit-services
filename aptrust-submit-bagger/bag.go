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
var descriptionFileName = "description.txt"
var titleFileName = "title.txt"
var defaultPermissions = os.FileMode(0744)

// bag asset names
var manifestName = "manifest-md5.txt"
var tagManifestName = "tagmanifest-md5.txt"
var aptrustInfoName = "aptrust-info.txt"
var bagInfoName = "bag-info.txt"
var bagitName = "baggit.txt"
var tagFiles = []string{aptrustInfoName, bagInfoName, manifestName}

// assets have been sync'd into the data/... folder, we need to remove and process what should not be there
// and write the remaining assets

func bagAssets(root string, submission string, bagname string, outfile string) error {

	bagDir := path.Join(root, bagname)

	// bagit.txt file
	err := addBagitFile(bagDir, bagitName)
	if err != nil {
		return err
	}

	// aptrust-info.txt file
	err = addAPTInfoFile(bagDir, aptrustInfoName)
	if err != nil {
		return err
	}

	// bag-info.txt file
	err = addBagInfoFile(bagDir, bagInfoName)
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

func updateManifest(root string, filename string) error {

	// read the existing manifest, we will rewrite it and remove spurious info
	existing := path.Join(root, "data", filename)
	contents, err := readFile(existing)
	if err != nil {
		return err
	}

	newContents := ""
	for _, line := range contents {
		tok := strings.SplitN(line, " ", 2)
		if len(tok) == 2 {
			fp := tok[0]
			file := strings.Trim(tok[1], " ")

			//if keepInManifest(file) == true {
			newContents += fmt.Sprintf("%s data/%s\n", fp, file)
			//}

		} else {
			log.Printf("WARNING: ignoring badly formed manifest line [%s]", line)
		}
	}

	log.Printf("INFO: removing [%s]", existing)
	_ = os.Remove(existing)
	log.Printf("INFO: writing [%s]", filename)
	return os.WriteFile(path.Join(root, filename), []byte(newContents), defaultPermissions)
}

func addBagInfoFile(root string, filename string) error {

	// read and parse the template
	name := fmt.Sprintf("%s.template", filename)
	tmpl, err := readAndParseTemplate(name)
	if err != nil {
		return err
	}

	// populate the attributes required by the templates
	type Attributes struct {
		Date               string
		SenderDescription  string
		SenderIdentifier   string
		BagGroupIdentifier string
	}
	attribs := Attributes{
		// populate
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

func addAPTInfoFile(root string, filename string) error {

	// read and parse the template
	name := fmt.Sprintf("%s.template", filename)
	tmpl, err := readAndParseTemplate(name)
	if err != nil {
		return err
	}

	// populate the attributes required by the templates
	type Attributes struct {
		Title       string
		Description string
		Storage     string
	}
	attribs := Attributes{
		// populate
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

func addBagitFile(root string, filename string) error {

	// read and parse the template
	name := fmt.Sprintf("%s.template", filename)
	tmpl, err := readAndParseTemplate(name)
	if err != nil {
		return err
	}

	// populate the attributes required by the templates (none in this case)
	type Attributes struct {
	}
	attribs := Attributes{}

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
	log.Printf("INFO: contents [%s]", data)
	return os.WriteFile(filename, []byte(data), defaultPermissions)
}

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
	log.Printf("INFO: tar completed (elapsed %0.2f seconds)", duration.Seconds())
	return err
}

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
