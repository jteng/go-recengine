package file

import (
	"fmt"
	"io/ioutil"
)

func handleFile(queue chan string, status chan string) {
	for fi := range queue {
		fmt.Printf("handle file %v \n", fi)
		status <- fi
	}
	/*
	fi:=<-queue
	fmt.Printf("handle file %v \n", (*fi).Name())
	status <- (*fi).Name()
	*/
}

func processFile(request chan string, status chan string) {
	for i := 0; i < 3; i++ {
		go handleFile(request, status)
	}
}

func Loadfiles() string {
	finfos, _ := ioutil.ReadDir("/Users/tengj6/Downloads/cdt-6711")
	workQueue := make(chan string, 5)
	done := make(chan string,5)

	processFile(workQueue, done)
	//go handleFile(workQueue,done)

	num := 0
	for _, v := range finfos {
		fmt.Printf("add file %s \n", v.Name())
		workQueue <- v.Name()
		num++
		//<-done
	}
	close(workQueue)

	fmt.Printf("wait for %d \n", num)
	for num > 0 {
		fmt.Printf("file %s is procssed. \n",<-done)
		num--
	}

	return "done"
}
