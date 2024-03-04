package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	godebian "github.com/btwotch/godebian"
	"github.com/spf13/cobra"
)

func main() {
	var c godebian.DebianContents
	var d godebian.SqliteDb

	d.Open()
	rootCmd := &cobra.Command{
		Use:   "goapt",
		Short: "goapt - example cmd for godebian",
	}

	searchCmd := &cobra.Command{
		Use:   "search",
		Short: "<ubuntu|debian> version path",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			distro := args[0]
			version := args[1]
			path := args[2]
			if distro == "ubuntu" {
				c = godebian.NewUbuntuContents(version, &d)
			} else if distro == "debian" {
				c = godebian.NewDebianContents(version, &d)
			}
			packages := c.Search(path)
			for _, pkg := range packages {
				pkginfo := c.PackageInfo(pkg)
				pop := c.Popularity(pkg)
				fmt.Printf("%s | package info: %+v | popularity: %d\n", pkg, pkginfo, pop)
			}
		},
	}

	packageInfoCmd := &cobra.Command{
		Use:   "show",
		Short: "<ubuntu|debian> version package",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			distro := args[0]
			version := args[1]
			pkg := args[2]
			if distro == "ubuntu" {
				c = godebian.NewUbuntuContents(version, &d)
			} else if distro == "debian" {
				c = godebian.NewDebianContents(version, &d)
			}
			pi := c.PackageInfo(pkg)
			fmt.Printf("%+v\n", pi)
		},
	}

	packageDownloadCmd := &cobra.Command{
		Use:   "download",
		Short: "<ubuntu|debian> version package",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			distro := args[0]
			version := args[1]
			pkg := args[2]
			if distro == "ubuntu" {
				c = godebian.NewUbuntuContents(version, &d)
			} else if distro == "debian" {
				c = godebian.NewDebianContents(version, &d)
			}

			url := c.PackageURL(pkg)
			fmt.Printf("%s\n", url)
		},
	}

	packageExtractCmd := &cobra.Command{
		Use:   "extract",
		Short: "<ubuntu|debian> version package path",
		Args:  cobra.ExactArgs(4),
		Run: func(cmd *cobra.Command, args []string) {
			distro := args[0]
			version := args[1]
			pkg := args[2]
			baseDir := args[3]
			if distro == "ubuntu" {
				c = godebian.NewUbuntuContents(version, &d)
			} else if distro == "debian" {
				c = godebian.NewDebianContents(version, &d)
			}

			f := func(fp io.Reader, fi godebian.FileInfo) {
				path := filepath.Join(baseDir, fi.Path)
				if fi.IsDir {
					os.MkdirAll(path, fi.Mode)
					err := os.Chown(path, fi.Uid, fi.Gid)
					if err != nil && !errors.Is(err, syscall.EPERM) {
						panic(err)
					}

				} else {
					dirPath := filepath.Dir(path)
					os.MkdirAll(dirPath, fi.Mode)
					wp, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, fi.Mode)
					if err != nil {
						panic(err)
					}
					defer wp.Close()
					io.Copy(wp, fp)
					err = os.Chown(path, fi.Uid, fi.Gid)
					if err != nil && !errors.Is(err, syscall.EPERM) {
						panic(err)
					}
				}
			}
			c.Extract(pkg, f)
		},
	}

	rootCmd.AddCommand(searchCmd)
	rootCmd.AddCommand(packageInfoCmd)
	rootCmd.AddCommand(packageDownloadCmd)
	rootCmd.AddCommand(packageExtractCmd)

	rootCmd.Execute()

}
