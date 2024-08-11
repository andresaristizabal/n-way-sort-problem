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
}

func writeWorker(jobs chan writePart, file *os.File, group *sync.WaitGroup, gbByFile int) {
	for job := range jobs {
		f, err := os.Create(job.fileName)
		check(err)
		bytePerFile := pkg.GB * gbByFile
		b := make([]byte, bytePerFile)
		readAt, err := file.ReadAt(b, int64(job.index*(bytePerFile)))
		check(err)
		if readAt != bytePerFile {
			panic("error on read")
		}
		f.Write(b)
		group.Done()
	}
}

func main() {
	filePath := flag.String("file-input", "", "input file")
	nWorkers := flag.Int("n-workers", 2, "input file")
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
	writeJob := make(chan writePart)
	var wg sync.WaitGroup
	for i := 0; i < *nWorkers; i++ {
		go writeWorker(writeJob, file, &wg, *nGb)
	}
	for i := 0; i < nFiles; i++ {
		wg.Add(1)
		writeJob <- writePart{
			fileName: fmt.Sprintf("tmp/file-%d.txt", i),
			index:    i,
		}
	}
	wg.Wait()
}
