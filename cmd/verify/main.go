package main

import (
	"bytes"
	"flag"
	"fmt"
	"n-way-sort/pkg/utils"
	"os"
	"path"
	"slices"
	"sync"
)

func checkOrderFile(file <-chan *os.File, gbPerFile int, group *sync.WaitGroup) {
	for f := range file {
		fmt.Println("Checking file: ", f.Name())
		currentPage := make([]byte, utils.Page)
		f.ReadAt(currentPage, 0)
		nextPage := make([]byte, utils.Page)
		pagePerPage := utils.GB * gbPerFile / utils.Page
		for i := 0; i < pagePerPage; i++ {
			f.ReadAt(nextPage, int64(i*utils.Page))
			if bytes.Compare(currentPage, nextPage) == 1 {
				panic(fmt.Sprintf("File is not ordered: %s, block: %i", f.Name(), i))
			}
			currentPage = slices.Clone(nextPage)
		}
		group.Done()
	}
}

func main() {
	folderPath := flag.String("folder-input", "", "input file")
	gbPerFile := flag.Int("gb-per-file", 2, "number of gb per file")
	nWorker := flag.Int("n-workers", 1, "number of workers")
	flag.Parse()

	// read files from folderPath
	dir, err := os.ReadDir(*folderPath)
	utils.CheckError(err)
	files := make([]*os.File, 0, len(dir))
	bytesPerFile := int64(*gbPerFile * utils.GB)
	isThereAnyError := false
	fileJob := make(chan *os.File, len(dir))
	waitGroup := sync.WaitGroup{}

	for _, entry := range dir {
		f, e := os.OpenFile(path.Join(*folderPath, entry.Name()), os.O_RDONLY, 0666)
		utils.CheckError(e)
		files = append(files, f)
	}
	fmt.Println("Checking file's size")

	for _, f := range files {
		s, _ := f.Stat()
		if bytesPerFile != s.Size() {
			fmt.Sprintf("File %s has %d bytes, expected %d bytes", f.Name(), s.Size(), bytesPerFile)
			isThereAnyError = true
		}
	}

	for i := 0; i < *nWorker; i++ {
		go checkOrderFile(fileJob, *nWorker, &waitGroup)
	}

	for _, file := range files {
		waitGroup.Add(1)
		fileJob <- file
	}

	waitGroup.Wait()
	if !isThereAnyError {
		fmt.Println("All files are ok")
	}

}
