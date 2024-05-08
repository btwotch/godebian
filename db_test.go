package godebian

import (
	"fmt"
	"os"
	"testing"
)

func TestETag(t *testing.T) {
	dbfh, err := os.CreateTemp("/var/tmp", "aptfs-test-db-*")
	if err != nil {
		panic(err)
	}

	filename := dbfh.Name()
	defer dbfh.Close()
	defer os.Remove(filename)

	var d SqliteDb

	d.dbPath = filename

	d.Open()

	d.setContentETag("stable", "amd64", "contrib", "bar")
	d.setContentETag("stable", "amd64", "contrib", "foo")

	et := d.getContentETag("stable", "amd64", "contrib")

	if et != "foo" {
		t.Fatalf("Setting and retrieving etag failed, should be foo, but is: %s", et)
	}
}

func TestPackages(t *testing.T) {
	dbfh, err := os.CreateTemp("/var/tmp", "aptfs-test-db-*")
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

			d.insertPackageFile("stable", "amd64", "main", packageFile, packageName)
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

	for i := 0; i < 10; i++ {
		packageFile := fmt.Sprintf("%d/file", i)
		ps := d.getPackage("stable", packageFile)
		if len(ps) != i+1 {
			t.Fatalf("%s should have %d packages, but is %+v", packageFile, i+1, ps)
		}
	}
}

func TestPackageWalk(t *testing.T) {
	dbfh, err := os.CreateTemp("/var/tmp", "aptfs-test-db-*")
	if err != nil {
		panic(err)
	}

	filename := dbfh.Name()
	defer dbfh.Close()
	defer os.Remove(filename)

	var d SqliteDb

	d.dbPath = filename

	d.Open()
	d.insertPackageFile("stable", "amd64", "main", "/usr/bin/foo", "foo")

	d.walk("stable", "amd64", "main", func(path, pkg string) bool {
		if path != "/usr/bin/foo" || pkg != "foo" {
			t.Errorf("path should be /usr/bin/foo but is %s; pkg should be foo, but is %s", path, pkg)
		}
		return true
	})
}

func TestCreatePackagesSqlFmtString(t *testing.T) {
	str := createPackagesSqlFmtString(5)

	t.Log(str)
}

func FuzzSplitStringArray(f *testing.F) {
	f.Fuzz(func(t *testing.T, splitLen int, arrayLen uint32) {
		if arrayLen == 0 || splitLen <= 0 {
			return
		}
		arr := make([]string, arrayLen)

		for i := 0; i < int(arrayLen)-1; i++ {
			arr[i] = fmt.Sprintf("%d", i)
		}

		arrs := split(arr, splitLen)

		flattenedArr := make([]string, 0)
		for _, arr := range arrs {
			for _, str := range arr {
				t.Logf("\t%s", str)
				flattenedArr = append(flattenedArr, str)
			}
			t.Logf("----")
		}

		if len(flattenedArr) != len(arr) {
			t.Fatal("different array length")
		}

		for i := range flattenedArr {
			if flattenedArr[i] != arr[i] {
				t.Fatalf("%d: %s <-> %s", i, flattenedArr[i], arr[i])
			}
		}
	})
}
func TestSplitStringArray(t *testing.T) {
	splitLen := 3

	arr := make([]string, 0)

	for i := 0; i < 5; i++ {
		arr = append(arr, fmt.Sprintf("%d", i))
	}

	arrs := split(arr, splitLen)
	flattenedArr := make([]string, 0)

	for _, arr := range arrs {
		for _, str := range arr {
			t.Logf("\t%s", str)
			flattenedArr = append(flattenedArr, str)
		}
		t.Logf("----")
	}

	if len(flattenedArr) != len(arr) {
		t.Fatal("different array length")
	}

	for i := range flattenedArr {
		if flattenedArr[i] != arr[i] {
			t.Fatalf("%d: %s <-> %s", i, flattenedArr[i], arr[i])
		}
	}
}
