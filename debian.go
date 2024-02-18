package godebian

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type Db interface {
	beginTransaction()
	endTransaction()
	setContentETag(version, etag string)
	getContentETag(version string) string
	setPopularityETag(version, etag string)
	getPopularityETag(version string) string
	getPackage(version, path string) []string
	removeAllPackages(version string)
	removeAllPopularities(version string)
	insertPackageFile(version, path, filePackage string)
	insertPackagePopularity(version, pkg string, popularity uint)
	walk(version string, walker func(path, pkg string) bool)
	getPackagePopularity(version, pkg string) uint
}

type DebianContents struct {
	db                Db
	version           string
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
			pkgPath := strings.Split(deb, "/")
			pkg := pkgPath[len(pkgPath)-1]
			d.db.insertPackageFile(d.distroWithVersion, path, pkg)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

}

func NewDebianContents(version string, db Db) DebianContents {
	dc := DebianContents{distroWithVersion: fmt.Sprintf("debian/%s", version), db: db, version: version}
	dc.updatePopularity("https://popcon.debian.org/by_inst.gz")
	dc.updateContents("http://ftp.debian.org/debian/dists/%s/main/Contents-amd64.gz")

	return dc
}

func NewUbuntuContents(version string, db Db) DebianContents {
	dc := DebianContents{distroWithVersion: fmt.Sprintf("ubuntu/%s", version), db: db, version: version}
	dc.updateContents("http://de.archive.ubuntu.com/ubuntu/dists/%s/Contents-amd64.gz")
	dc.updatePopularity("https://popcon.debian.org/by_inst.gz")

	return dc
}

func (d *DebianContents) readPopularityFileIntoDB(r io.Reader) {
	scanner := bufio.NewScanner(r)
	d.db.beginTransaction()
	defer d.db.endTransaction()
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "#") {
			continue
		}

		ss := strings.Fields(scanner.Text())
		if len(ss) < 2 {
			continue
		}

		pkg := ss[1]
		popularity, err := strconv.Atoi(ss[0])
		if err != nil {
			panic("Could not parse line " + scanner.Text())
		}

		d.db.insertPackagePopularity(d.distroWithVersion, pkg, uint(popularity))
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

}

func (d *DebianContents) updatePopularity(url string) {
	etag := d.db.getPopularityETag(d.distroWithVersion)
	client := &http.Client{}
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
		return
	}

	d.db.removeAllPopularities(d.distroWithVersion)

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		panic(fmt.Errorf("updating popularity from %s failed: %v", url, err))
	}

	defer gzr.Close()
	defer resp.Body.Close()

	d.readPopularityFileIntoDB(gzr)

	d.db.setPopularityETag(d.distroWithVersion, resp.Header.Get("Etag"))
}

func (d *DebianContents) updateContents(urlfmt string) {
	etag := d.db.getContentETag(d.distroWithVersion)
	client := &http.Client{}
	url := fmt.Sprintf(urlfmt, d.version)
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
		return
	}
	d.db.removeAllPackages(d.distroWithVersion)

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		panic(fmt.Errorf("Opening content file failed: %+v, req: %+v, resp: %+v", err, req, resp))
	}

	defer gzr.Close()
	defer resp.Body.Close()

	d.readContentsFileIntoDB(gzr)

	d.db.setContentETag(d.distroWithVersion, resp.Header.Get("Etag"))
}

func (d DebianContents) Search(path string) []string {
	var ret []string
	retMap := make(map[string]struct{})
	pkgs := d.db.getPackage(d.distroWithVersion, path)

	for _, pkg := range pkgs {
		ss := strings.Split(pkg, "/")
		retMap[ss[len(ss)-1]] = struct{}{}
	}

	for k, _ := range retMap {
		ret = append(ret, k)
	}

	return ret
}

func (d DebianContents) Popularity(pkg string) uint {
	return d.db.getPackagePopularity(d.distroWithVersion, pkg)
}

func (d DebianContents) Walk(walker func(path, pkg string) bool) {
	d.db.walk(d.distroWithVersion, walker)
}
