package main

import (
	"github.com/dragonflyoss/Dragonfly/dfdaemon/seed/performance_test/client"
	"os"
	"path/filepath"
)

func main()  {
	if len(os.Args) != 2 {
		panic("need remote server host")
	}

	host := os.Args[1]
	cacheDir := filepath.Join("./cache", "direct_request")
	if err := os.MkdirAll(cacheDir, 0744); err != nil {
		panic(err)
	}
	client.Run(host, cacheDir, true, false, false)
}
