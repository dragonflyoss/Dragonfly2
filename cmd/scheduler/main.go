package main

import "github.com/dragonflyoss/Dragonfly2/scheduler/server"

func main() {
	svr := server.NewServer()
	svr.Start()
}
