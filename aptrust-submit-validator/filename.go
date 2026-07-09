package main

import (
	"path/filepath"
	"regexp"
	"unicode"
)

var controlRegex = regexp.MustCompile("\\\\[Uu]00[0189][0-9A-Fa-f]|\\\\[Uu]007[Ff]")

// ensure our files are POSIX compliant
func validFileName(file string) bool {

	fn := filepath.Base(file)

	// length must be greater than 0 and no more than 255
	if len(fn) == 0 || len(fn) > 255 {
		return false
	}

	// cannot start with a 'dash', not POSIX compliant
	if fn[0] == '-' {
		return false
	}

	return ContainsControlCharacter(fn) == false && ContainsEscapedControl(fn) == false
}

// the following taken from APTrust validation code
// here: https://github.com/APTrust/preservation-services/blob/master/util/util.go#L70

// ContainsControlCharacter returns true if string str contains a
// Unicode control character. We use this to test file names, which
// should not contain control characters.
//
// This also catches the unicode non-breaking space, \xc2\xa0,
// which Go does not consider a control character, but does cause
// problems because S3 will not accept files or metadata containing
// this character.
func ContainsControlCharacter(str string) bool {
	nbSpace := []rune(" ")[0] // unicode non-breaking space \xc2\xa0
	for _, _rune := range str {
		if unicode.IsControl(_rune) || _rune == nbSpace {
			return true
		}
	}
	return false
}

// ContainsEscapedControl returns true if string str contains
// something that looks like an escaped UTF-8 control character.
// The Mac OS file system seems to silently escape UTF-8 control
// characters. That causes problems when we try to copy a file
// over to another file system that won't accept the control
// character in a file name. The bag validator looks for file names
// matching these patterns and rejects them.
func ContainsEscapedControl(str string) bool {
	return controlRegex.MatchString(str)
}

//
// end of file
//
