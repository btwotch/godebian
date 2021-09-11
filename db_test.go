package godebian

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestETag(t *testing.T) {
	dbfh, err := ioutil.TempFile("/var/tmp", "aptfs-test-db-*")
	if err != nil {
		panic(err)
	}

	filename := dbfh.Name()
	defer dbfh.Close()
	defer os.Remove(filename)

	var d SqliteDb

	d.dbPath = filename

	d.Open()

	d.setETag("stable", "bar")
	d.setETag("stable", "foo")

	et := d.getETag("stable")

	if et != "foo" {
		t.Fatalf("Setting and retrieving etag failed, should be foo, but is: %s", et)
	}
}

func TestPackages(t *testing.T) {
	dbfh, err := ioutil.TempFile("/var/tmp", "aptfs-test-db-*")
	if err != nil {
		panic(err)
	}

	filename := dbfh.Name()
	defer dbfh.Close()
	defer os.Remove(filename)

	var d SqliteDb

	d.dbPath = filename

	d.Open()

	d.beginTransaction()
	for i := 0; i < 10; i++ {
		for j := 0; j <= i; j++ {
			packageName := fmt.Sprintf("package-%d-%d", i, j)
			packageFile := fmt.Sprintf("/usr/%d/file", i)

			d.insertPackageFile("stable", packageFile, packageName)
		}
	}
	d.endTransaction()

	for i := 0; i < 10; i++ {
		packageFile := fmt.Sprintf("/usr/%d/file", i)
		ps := d.getPackage("stable", packageFile)
		if len(ps) != i+1 {
			t.Fatalf("%s should have %d packages, but is %+v", packageFile, i+1, ps)
		}
	}
}

func TestPackageWalk(t *testing.T) {
	dbfh, err := ioutil.TempFile("/var/tmp", "aptfs-test-db-*")
	if err != nil {
		panic(err)
	}

	filename := dbfh.Name()
	defer dbfh.Close()
	defer os.Remove(filename)

	var d SqliteDb

	d.dbPath = filename

	d.Open()
	d.insertPackageFile("stable", "/usr/bin/foo", "foo")

	d.walk("stable", func(path, pkg string) bool {
		if path != "/usr/bin/foo" || pkg != "foo" {
			t.Errorf("path should be /usr/bin/foo but is %s; pkg should be foo, but is %s", path, pkg)
		}
		return true
	})
}
