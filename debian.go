package godebian

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type PackageInfo struct {
	Name     string
	Version  string
	Depends  []string
	Filename string
}

type Db interface {
	beginTransaction()
	endTransaction()
	setContentETag(version, arch, repo, etag string)
	getContentETag(version, arch, repo string) string
	setPopularityETag(version, etag string)
	getPopularityETag(version string) string
	setPackageInfoETag(version, repo, arch, etag string)
	getPackageInfoETag(version, repo, arch string) string
	getPackage(version, path string) []string
	getPackageInfo(version, arch, pkg string) PackageInfo
	removeAllPackages(version, arch, repo string)
	removeAllPackageInfos(version, repo, arch string)
	removeAllPopularities(version string)
	insertPackageFile(version, arch, repo, path, filePackage string)
	insertPackageInfo(version, repo string, arch string, pi PackageInfo)
	insertPackagePopularity(version, pkg string, popularity uint)
	walk(version, arch, repo string, walker func(path, pkg string) bool)
	getPackagePopularity(version, pkg string) uint
}

type DebianContents struct {
	db                Db
	version           string
	distroWithVersion string
	arch              string
	downloadBaseURL   string
}

type urlWithArch struct {
	url  string
	arch string
}

func (d *DebianContents) readContentsFileIntoDB(r io.Reader, arch, repo string) {
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
			d.db.insertPackageFile(d.distroWithVersion, arch, repo, path, pkg)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

}

func NewDebianContents(version string, db Db) DebianContents {
	dc := DebianContents{distroWithVersion: fmt.Sprintf("debian/%s", version), db: db, version: version, arch: "amd64"}
	dc.downloadBaseURL = "http://ftp.debian.org/debian/"

	contentsURLFmt := "http://ftp.debian.org/debian/dists/%s/%s/Contents-%s.gz"
	packageInfoFmt := "http://ftp.debian.org/debian/dists/%s/%s/binary-%s/Packages.gz"

	dc.updatePopularity("https://popcon.debian.org/by_vote.gz")

	for _, repo := range []string{"main", "non-free"} {
		for _, arch := range []string{"amd64", "all"} {
			contentsURL := fmt.Sprintf(contentsURLFmt, dc.version, repo, arch)
			packageInfo := fmt.Sprintf(packageInfoFmt, dc.version, repo, arch)

			dc.updateContents(urlWithArch{
				url:  contentsURL,
				arch: arch,
			}, repo)
			dc.updatePackageInfo(packageInfo, repo, arch)
		}
	}

	return dc
}

func NewUbuntuContents(version string, db Db) DebianContents {
	dc := DebianContents{distroWithVersion: fmt.Sprintf("ubuntu/%s", version), db: db, version: version, arch: "amd64"}
	dc.downloadBaseURL = "http://de.archive.ubuntu.com/ubuntu"

	contentsURLFmt := "http://de.archive.ubuntu.com/ubuntu/dists/%s/Contents-%s.gz"
	packageInfoFmt := "http://de.archive.ubuntu.com/ubuntu/dists/%s/%s/binary-%s/Packages.gz"

	dc.updatePopularity("https://popcon.debian.org/by_vote.gz")

	contentsURL := fmt.Sprintf(contentsURLFmt, dc.version, dc.arch)
	dc.updateContents(urlWithArch{
		url:  contentsURL,
		arch: dc.arch,
	}, "")
	for _, repo := range []string{"main", "multiverse", "universe", "restricted"} {
		packageInfoURL := fmt.Sprintf(packageInfoFmt, dc.version, repo, dc.arch)

		dc.updatePackageInfo(packageInfoURL, repo, dc.arch)
	}

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

	resp := eTagRequest(url, etag)
	if resp == nil {
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

func setContentFileValue(line, prefix string, value *string) {
	if *value != "" {
		return
	}

	if strings.HasPrefix(line, prefix) {
		ss := strings.SplitN(line, ": ", 2)
		if len(ss) == 2 {
			*value = ss[1]
		}
	}

}

func (d *DebianContents) updatePackageInfo(url string, repo string, arch string) {
	etag := d.db.getPackageInfoETag(d.distroWithVersion, repo, arch)

	resp := eTagRequest(url, etag)
	if resp == nil {
		return
	}

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		panic(fmt.Errorf("updating package info from %s failed: %v", url, err))
	}

	d.db.removeAllPackageInfos(d.distroWithVersion, repo, arch)
	defer gzr.Close()
	defer resp.Body.Close()

	scanner := bufio.NewScanner(gzr)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	var pi PackageInfo
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			d.db.insertPackageInfo(d.distroWithVersion, repo, arch, pi)
			pi = PackageInfo{}
		}
		setContentFileValue(line, "Package: ", &pi.Name)
		setContentFileValue(line, "Filename: ", &pi.Filename)
		setContentFileValue(line, "Version: ", &pi.Version)
		var deps string
		setContentFileValue(line, "Depends: ", &deps)
		if deps != "" {
			pi.Depends = strings.Split(deps, ", ")
		}
	}
	if scanner.Err() != nil {
		panic(scanner.Err())
	}

	d.db.setPackageInfoETag(d.distroWithVersion, repo, arch, resp.Header.Get("Etag"))
}

func (d *DebianContents) updateContents(urls urlWithArch, repo string) {
	etag := d.db.getContentETag(d.distroWithVersion, urls.arch, repo)

	resp := eTagRequest(urls.url, etag)
	if resp == nil {
		return
	}
	d.db.removeAllPackages(d.distroWithVersion, urls.arch, repo)

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		panic(fmt.Errorf("Opening content file failed: %+v, req: url: %s, resp: %+v", err, urls.url, resp))
	}

	defer gzr.Close()
	defer resp.Body.Close()

	d.readContentsFileIntoDB(gzr, urls.arch, repo)

	d.db.setContentETag(d.distroWithVersion, urls.arch, repo, resp.Header.Get("Etag"))
}

func eTagRequest(url string, etag string) *http.Response {
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
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("http request failed: %+v", resp)
		panic(err)
	}
	return resp
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

func (d DebianContents) PackageInfo(pkg string) PackageInfo {
	return d.db.getPackageInfo(d.distroWithVersion, d.arch, pkg)
}

func (d DebianContents) Extract(pkg string, filter func(fp io.Reader, fi FileInfo)) {
	url := d.PackageURL(pkg)
	if url == "" {
		return
	}

	e := extractor{}
	e.extractFunc = filter
	e.extract(url)
}

func (d DebianContents) PackageURL(pkg string) string {
	pi := d.db.getPackageInfo(d.distroWithVersion, d.arch, pkg)

	if pi.Filename == "" {
		return ""
	}

	u, err := url.JoinPath(d.downloadBaseURL, pi.Filename)
	if err != nil {
		panic(err)
	}

	return u
}

func (d DebianContents) Popularity(pkg string) uint {
	return d.db.getPackagePopularity(d.distroWithVersion, pkg)
}

func (d DebianContents) Walk(arch, repo string, walker func(path, pkg string) bool) {
	d.db.walk(d.distroWithVersion, arch, repo, walker)
}
