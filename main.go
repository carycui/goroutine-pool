package main

import (
	"log"
	"runtime"
	"time"
	. "github.com/carycui/goroutine-pool/worker_pool"
)

func demoFunc() error {
	time.Sleep(100 * time.Millisecond)
	return nil
}

func innner_a(loopers int, wp *WorkerPool) {
	for i := 0; i < loopers; i++ {
			_ = wp.RunTask(demoFunc)
	}
	wp.WaitTasksDone()	
}

func main() {
	curMem := runtime.MemStats{}
	runtime.ReadMemStats(&curMem)
	log.Println("start to main")
	wp,_ := NewWorkerPool(1000)
	var runTimes = 2000
	for i:=0;i<runTimes;i++ {
		innner_a(1000,wp)
	}
	//time.Sleep(1 * time.Second)
	wp.Close()
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	usage := mem.TotalAlloc - curMem.TotalAlloc
	log.Printf("memory usage:%d B", usage)
}
