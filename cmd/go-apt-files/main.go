package main

import (
	"fmt"
	"os"

	godebian "github.com/btwotch/godebian"
)

func main() {
	var d godebian.SqliteDb

	if len(os.Args) != 4 {
		fmt.Printf("Usage: %s <ubuntu|debian> <version> <path>\n", os.Args[0])
		os.Exit(0)
	}
	d.Open()
	var c godebian.DebianContents
	if os.Args[1] == "ubuntu" {
		c = godebian.NewUbuntuContents(os.Args[2], &d)
	} else if os.Args[1] == "debian" {
		c = godebian.NewDebianContents(os.Args[2], &d)
	}

	packages := c.Search(os.Args[3])
	for _, pkg := range packages {
		pkginfo := c.PackageInfo(pkg)
		pop := c.Popularity(pkg)
		fmt.Printf("%s | package info: %+v | popularity: %d\n", pkg, pkginfo, pop)
	}
}
