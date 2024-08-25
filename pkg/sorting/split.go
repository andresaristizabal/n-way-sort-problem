package sorting

import (
	"bytes"
	"fmt"
	"n-way-sort/pkg/utils"
	"os"
	"slices"
	"sync"
)

type writePart struct {
	fileName string
	index    int
	data     []byte
}

func writeWorker(writeJobs chan *writePart, group *sync.WaitGroup, files []*os.File, config utils.Config) {
	for job := range writeJobs {
		f, err := os.Create(job.fileName)
		utils.CheckError(err)
		numberOfPages := (config.NGb * utils.GB) / utils.Page
		pages := make([][]byte, 0, numberOfPages)
		for i := 0; i < numberOfPages; i++ {
			pages = append(pages, job.data[(i*utils.Page):((i*utils.Page)+utils.Page)])
		}
		// TODO: move it to a worker
		slices.SortFunc(pages, bytes.Compare)
		for _, p := range pages {
			f.Write(p)
		}
		f.Sync()
		files[job.index] = f
		group.Done()
	}
}

func readWorker(readJobs chan *writePart, writeJobs chan *writePart, file *os.File, gbByFile int) {
	for writeJob := range readJobs {
		bytePerFile := utils.GB * gbByFile
		b := make([]byte, bytePerFile)
		readAt, err := file.ReadAt(b, int64(writeJob.index*(bytePerFile)))
		utils.CheckError(err)
		if readAt != bytePerFile {
			panic("error on read")
		}
		writeJob.data = b
		writeJobs <- writeJob
	}
}

func Split(config utils.Config) []*os.File {
	err := os.RemoveAll("tmp")
	if err != nil {
	}
	err = os.MkdirAll("tmp", 0777)
	utils.CheckError(err)
	file, err := os.Open(config.FilePath)
	utils.CheckError(err)
	stat, err := file.Stat()
	utils.CheckError(err)
	fileSize := stat.Size()
	nFiles := int(fileSize / int64(utils.GB*(config.NGb)))
	if fileSize%int64(config.NGb*utils.GB) != 0 {
		panic("file size must be multiple of nGb")
	}
	fmt.Println("number of files: ", nFiles)
	readJob := make(chan *writePart)
	writeJob := make(chan *writePart)
	resultFiles := make([]*os.File, nFiles)
	var wg sync.WaitGroup
	for i := 0; i < config.RWorkers; i++ {
		go readWorker(readJob, writeJob, file, config.NGb)
	}
	for i := 0; i < config.WWorkers; i++ {
		go writeWorker(writeJob, &wg, resultFiles, config)
	}
	for i := 0; i < nFiles; i++ {
		wg.Add(1)
		readJob <- &writePart{
			fileName: fmt.Sprintf("tmp/file-%d.txt", i),
			index:    i,
		}
	}
	wg.Wait()
	return resultFiles
}
