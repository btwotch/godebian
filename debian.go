package godebian

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Db interface {
	beginTransaction()
	endTransaction()
	setETag(version, etag string)
	getETag(version string) string
	getPackage(version, path string) []string
	removeAllPackages(version string)
	insertPackageFile(version, path, filePackage string)
	walk(version string, walker func(path, pkg string) bool)
}

type DebianContents struct {
	db                Db
	distroWithVersion string
}

func (d *DebianContents) readContentsFileIntoDB(r io.Reader) {
	scanner := bufio.NewScanner(r)
	d.db.beginTransaction()
	defer d.db.endTransaction()
	for scanner.Scan() {
		ss := strings.Fields(scanner.Text())
		if len(ss) < 2 {
			continue
		}
		path := "/" + ss[0]
		debs := ss[len(ss)-1]
		for _, deb := range strings.Split(debs, ",") {
			d.db.insertPackageFile(d.distroWithVersion, path, deb)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

}

func NewDebianContents(version string, db Db) DebianContents {
	return newContents("debian", version, db, "http://ftp.debian.org/debian/dists/%s/main/Contents-amd64.gz")
}

func NewUbuntuContents(version string, db Db) DebianContents {
	return newContents("ubuntu", version, db, "http://de.archive.ubuntu.com/ubuntu/dists/%s/Contents-amd64.gz")
}

func newContents(distro, version string, db Db, urlfmt string) DebianContents {
	dc := DebianContents{distroWithVersion: fmt.Sprintf("%s/%s", distro, version), db: db}

	etag := db.getETag(dc.distroWithVersion)
	client := &http.Client{}
	url := fmt.Sprintf(urlfmt, version)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	req.Header.Set("If-None-Match", etag)
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	if resp.StatusCode == http.StatusNotModified {
		return dc
	}
	db.removeAllPackages(dc.distroWithVersion)

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		panic(err)
	}

	defer gzr.Close()
	defer resp.Body.Close()

	dc.readContentsFileIntoDB(gzr)

	dc.db.setETag(dc.distroWithVersion, resp.Header.Get("Etag"))

	return dc
}

func (d DebianContents) Search(path string) []string {
	var ret []string
	pkgs := d.db.getPackage(d.distroWithVersion, path)

	for _, pkg := range pkgs {
		ss := strings.Split(pkg, "/")
		ret = append(ret, ss[len(ss)-1])
	}

	return ret
}

func (d DebianContents) Walk(walker func(path, pkg string) bool) {
	d.db.walk(d.distroWithVersion, walker)
}
