package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
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

	searchDirContentsCmd := &cobra.Command{
		Use:   "search-dir-contents",
		Short: "<ubuntu|debian> version dir",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			distro := args[0]
			version := args[1]
			path := args[2]

			paths := make([]string, 0)
			filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
				paths = append(paths, path)
				return nil
			})
			if distro == "ubuntu" {
				c = godebian.NewUbuntuContents(version, &d)
			} else if distro == "debian" {
				c = godebian.NewDebianContents(version, &d)
			}
			fmt.Printf("len(paths) = %d\n", len(paths))
			packages := c.SearchPaths(paths)
			for path, pkgs := range packages {
				if len(pkgs) > 1 {
					fmt.Println()
				}
				for _, pkg := range pkgs {
					pkginfo := c.PackageInfo(pkg)
					pop := c.Popularity(pkg)
					if len(pkgs) > 1 {
						fmt.Printf("    ")
					}
					fmt.Printf("%s: %s | package info: %+v | popularity: %d\n", path, pkg, pkginfo, pop)
				}
				if len(pkgs) > 1 {
					fmt.Println()
				}
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

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "<ubuntu|debian> version package",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			distro := args[0]
			version := args[1]
			if distro == "ubuntu" {
				c = godebian.NewUbuntuContents(version, &d)
			} else if distro == "debian" {
				c = godebian.NewDebianContents(version, &d)
			}
			c.Walk("amd64", "main", func(path, pkg string) bool {
				fmt.Printf("%s:\t\t%s\n", path, pkg)
				return true
			})
		},
	}

	getPCsCmd := &cobra.Command{
		Use:   "pc",
		Short: "<ubuntu|debian> version package",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			distro := args[0]
			version := args[1]
			if distro == "ubuntu" {
				c = godebian.NewUbuntuContents(version, &d)
			} else if distro == "debian" {
				c = godebian.NewDebianContents(version, &d)
			}
			pkg2files := make(map[string]map[string]struct{})
			rex := regexp.MustCompile("^.*\\.pc$")
			c.Walk("amd64", "main", func(path, pkg string) bool {
				if rex.MatchString(path) {
					if pkg2files[pkg] == nil {
						pkg2files[pkg] = make(map[string]struct{})
					}
					pkg2files[pkg][path] = struct{}{}

				}
				return true
			})

			for pkg, paths := range pkg2files {
				fmt.Printf("%s -> %+v\n", pkg, paths)

				c.Extract(pkg, func(fp io.Reader, fi godebian.FileInfo) {
					fiPath := fi.Path[1:]
					_, found := paths[fiPath]
					if found {
						base := filepath.Base(fiPath)
						wp, err := os.OpenFile(base, os.O_CREATE|os.O_WRONLY, fi.Mode)
						if err != nil {
							panic(err)
						}
						defer wp.Close()
						io.Copy(wp, fp)

					}

				})
			}
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
	rootCmd.AddCommand(searchDirContentsCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(getPCsCmd)
	rootCmd.AddCommand(packageInfoCmd)
	rootCmd.AddCommand(packageDownloadCmd)
	rootCmd.AddCommand(packageExtractCmd)

	rootCmd.Execute()

}
