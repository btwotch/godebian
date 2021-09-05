package main

import (
	"fmt"
	"os"

	godebian "github.com/btwotch/godebian"
)

func main() {
	var d godebian.SqliteDb

	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <path>\n", os.Args[0])
		os.Exit(0)
	}
	d.Open()
	c := godebian.NewDebianContents("stable", &d)

	p := c.Search(os.Args[1])

	fmt.Println(p)
}
