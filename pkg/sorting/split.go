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
	fileName      string
	index         int
	data          []byte
	expectedBytes int64
}

func writeWorker(writeJobs chan *writePart, group *sync.WaitGroup) {
	for job := range writeJobs {
		f, err := os.Create(job.fileName)
		utils.CheckError(err)
		f.Write(job.data)
		f.Sync()
		group.Done()
	}
}

func orderWorker(orderJob chan *writePart, writeChannel chan *writePart) {
	for job := range orderJob {
		numberOfPages := job.expectedBytes / utils.Page
		pages := make([][]byte, 0, numberOfPages)
		for i := int64(0); i < int64(numberOfPages); i++ {
			pages = append(pages, job.data[(i*utils.Page):((i*utils.Page)+utils.Page)])
		}
		slices.SortFunc(pages, bytes.Compare)
		job.data = slices.Concat(pages...)
		pages = nil
		writeChannel <- job
	}
}

func readWorker(readJobs chan *writePart, orderJobs chan *writePart, file *os.File, gbByFile int) {
	for writeJob := range readJobs {
		b := make([]byte, writeJob.expectedBytes)
		readAt, err := file.ReadAt(b, int64(writeJob.index)*writeJob.expectedBytes)
		utils.CheckError(err)
		// TODO: writeJob.expectedBytes could be int.Max?
		if readAt != int(writeJob.expectedBytes) {
			panic("error on read")
		}
		writeJob.data = b
		orderJobs <- writeJob
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
	remainingBytes := int64(0)
	requiredExtraFile := false
	if fileSize%int64(config.NGb*utils.GB) != 0 {
		// increase one file, in order to store the remaining bytes
		nFiles++
		remainingBytes = fileSize % int64(utils.GB*(config.NGb))
		requiredExtraFile = true
	}

	fmt.Println("number of files: ", nFiles)

	readJob := make(chan *writePart)
	writeJob := make(chan *writePart)
	orderJob := make(chan *writePart)

	resultFiles := make([]*os.File, nFiles)

	var wg sync.WaitGroup
	for i := 0; i < config.RWorkers; i++ {
		go readWorker(readJob, orderJob, file, config.NGb)
	}
	for i := 0; i < config.WWorkers; i++ {
		go orderWorker(orderJob, writeJob)
	}
	for i := 0; i < config.WWorkers; i++ {
		go writeWorker(writeJob, &wg)
	}
	for i := 0; i < nFiles; i++ {
		wg.Add(1)
		fileSize := int64(utils.GB * config.NGb)
		if nFiles == i+1 && requiredExtraFile {
			fileSize = remainingBytes
		}
		readJob <- &writePart{
			fileName:      fmt.Sprintf("tmp/file-%d.txt", i),
			index:         i,
			expectedBytes: fileSize,
		}
	}
	wg.Wait()
	return resultFiles
}
