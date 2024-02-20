package main

import (
	"fmt"

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

	rootCmd.AddCommand(searchCmd)

	rootCmd.Execute()

}
