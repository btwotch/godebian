package godebian

import (
	"testing"
)

func TestDebianPackageSearch(t *testing.T) {
	var d SqliteDb

	d.Open()

	dc := NewDebianContents("stable", &d)
	testDebianPackageSearch(t, &dc)
}

func TestUbuntuPackageSearch(t *testing.T) {
	var d SqliteDb

	d.Open()

	uc := NewUbuntuContents("focal", &d)
	testDebianPackageSearch(t, &uc)
}

func testDebianPackageSearch(t *testing.T, dc *DebianContents) {
	find := map[string]bool{"/bin/bash": false, "/usr/share/aqemu/os_templates/Linux": false}

	for path, _ := range find {
		pks := dc.Search(path)
		if len(pks) > 0 {
			find[path] = true
		}
	}

	for path, found := range find {
		if !found {
			t.Errorf("Could not find %s", path)
		}
	}
}
