package godebian

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/blakesmith/ar"
	archiver "github.com/mholt/archiver/v4"
)

func extractDataFile(r io.Reader, filename string) {
	format, input, err := archiver.Identify(filename, r)
	if err != nil {
		panic(err)
	}

	baseDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	baseDir = filepath.Join(baseDir, "extract")
	handler := func(ctx context.Context, f archiver.File) error {
		path := filepath.Join(baseDir, f.NameInArchive)
		h, ok := f.Header.(*tar.Header)
		if !ok {
			return nil
		}
		if f.IsDir() {
			os.MkdirAll(path, f.Mode())
			err := os.Chown(path, h.Uid, h.Gid)
			if err != nil && !errors.Is(err, syscall.EPERM) {
				panic(err)
			}

			return nil
		}

		dirPath := filepath.Dir(path)
		os.MkdirAll(dirPath, f.Mode())
		wp, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, f.Mode())
		if err != nil {
			panic(err)
		}
		defer wp.Close()
		fp, err := f.Open()
		if err != nil {
			panic(err)
		}
		defer fp.Close()

		io.Copy(wp, fp)
		err = os.Chown(path, h.Uid, h.Gid)
		if err != nil && !errors.Is(err, syscall.EPERM) {
			panic(err)
		}

		return nil
	}

	ctx := context.TODO()
	if ex, ok := format.(archiver.Extractor); ok {
		ex.Extract(ctx, input, nil, handler)
	}
}

func extract(url string) {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	debFile := resp.Body
	defer resp.Body.Close()
	deb := ar.NewReader(debFile)

	for {
		header, err := deb.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Printf("%T %+v\n", err, err)
			panic(err)
		}
		if strings.HasPrefix(header.Name, "data") {
			extractDataFile(deb, header.Name)

		}
	}
}

/*
func main() {
	xfig := "http://ftp.debian.org/debian/pool/main/x/xfig/xfig_3.2.8b-2+b2_amd64.deb"
	extract(xfig)
}
*/
