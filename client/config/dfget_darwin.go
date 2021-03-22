// +build darwin

package config

var DfgetConfig = ClientOption{
	URL:           "",
	LockFile:      "/tmp/dfget.lock",
	Output:        "",
	Timeout:       0,
	Md5:           "",
	DigestMethod:  "",
	DigestValue:   "",
	Identifier:    "",
	CallSystem:    "",
	Pattern:       "",
	Cacerts:       nil,
	Filter:        nil,
	Header:        nil,
	NotBackSource: false,
	Insecure:      false,
	ShowBar:       false,
	Console:       false,
	Verbose:       false,
}
