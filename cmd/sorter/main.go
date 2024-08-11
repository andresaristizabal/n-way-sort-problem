package main

import (
	"flag"
	"fmt"
	"n-way-sort/pkg"
	"os"
	"sync"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type writePart struct {
	fileName string
	index    int
	data     []byte
}

func writeWorker(writeJobs chan *writePart, group *sync.WaitGroup) {
	for job := range writeJobs {
		f, err := os.Create(job.fileName)
		check(err)
		f.Write(job.data)
		group.Done()
	}
}

func readWorker(readJobs chan *writePart, writeJobs chan *writePart, file *os.File, gbByFile int) {
	for writeJob := range readJobs {
		bytePerFile := pkg.GB * gbByFile
		b := make([]byte, bytePerFile)
		readAt, err := file.ReadAt(b, int64(writeJob.index*(bytePerFile)))
		check(err)
		if readAt != bytePerFile {
			panic("error on read")
		}
		writeJob.data = b
		writeJobs <- writeJob
	}
}

func main() {
	filePath := flag.String("file-input", "", "input file")
	rWorkers := flag.Int("r-workers", 2, "input file")
	wWorkers := flag.Int("w-workers", 1, "input file")
	nGb := flag.Int("n-gb", 1, "input file")
	flag.Parse()
	err := os.RemoveAll("tmp")
	if err != nil {
	}
	err = os.MkdirAll("tmp", 0777)
	check(err)
	file, err := os.Open(*filePath)
	check(err)
	stat, err := file.Stat()
	check(err)
	fileSize := stat.Size()
	nFiles := int(fileSize / int64(pkg.GB*(*nGb)))
	if fileSize%int64(*nGb*pkg.GB) != 0 {
		panic("file size must be multiple of nGb")
	}
	fmt.Println("number of files: ", nFiles)
	readJob := make(chan *writePart)
	writeJob := make(chan *writePart)
	var wg sync.WaitGroup
	for i := 0; i < *rWorkers; i++ {
		go readWorker(readJob, writeJob, file, *nGb)
	}
	for i := 0; i < *wWorkers; i++ {
		go writeWorker(writeJob, &wg)
	}
	for i := 0; i < nFiles; i++ {
		wg.Add(1)
		readJob <- &writePart{
			fileName: fmt.Sprintf("tmp/file-%d.txt", i),
			index:    i,
		}
	}
	wg.Wait()
}
