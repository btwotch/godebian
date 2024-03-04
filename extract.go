package godebian

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/blakesmith/ar"
	archiver "github.com/mholt/archiver/v4"
)

type FileInfo struct {
	Path     string
	Uid      int
	Gid      int
	Mode     fs.FileMode
	ModeTime time.Time
	IsDir    bool
}

type extractor struct {
	extractFunc func(fp io.Reader, fi FileInfo)
}

func (e extractor) extractDataFile(r io.Reader, filename string) {
	format, input, err := archiver.Identify(filename, r)
	if err != nil {
		panic(err)
	}

	handler := func(ctx context.Context, f archiver.File) error {
		h, ok := f.Header.(*tar.Header)
		if !ok {
			return nil
		}
		fi := FileInfo{
			Path:     f.NameInArchive,
			Uid:      h.Uid,
			Gid:      h.Gid,
			Mode:     f.Mode(),
			ModeTime: f.ModTime(),
			IsDir:    f.IsDir(),
		}

		var fp io.ReadCloser
		if f.Open != nil {
			fp, err = f.Open()
			if err != nil {
				panic(err)
			}

		}
		defer func() {
			if fp != nil {
				fp.Close()
			}
		}()

		e.extractFunc(fp, fi)

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

func (e extractor) extract(url string) {
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
			e.extractDataFile(deb, header.Name)

		}
	}
}
