package main

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/core/helper"
	"math/rand"
	"time"
)

func main()  {
	port := rand.Intn(1000) + 63000
	host := fmt.Sprintf("127.0.0.1:%d", port)

	server := helper.NewMockFileServer()
	err := server.StartServer(context.Background(), port)
	if err != nil {
		panic(err)
	}

	err = server.RegisterFile("fileG", 100*1024*1024, "1abcdefg")
	if  err != nil {
		panic(err)
	}

	fmt.Printf("host: %s\n", host)
	time.Sleep(time.Hour * 100)
}
