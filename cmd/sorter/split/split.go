package split

import (
	"bytes"
	"fmt"
	"n-way-sort/cmd/sorter/common"
	"n-way-sort/pkg"
	"os"
	"slices"
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

func writeWorker(writeJobs chan *writePart, group *sync.WaitGroup, files []*os.File, config common.Config) {
	for job := range writeJobs {
		f, err := os.Create(job.fileName)
		check(err)
		pages := make([][]byte, (config.NGb*pkg.GB)/pkg.Page)
		for i, _ := range pages {
			pages[i] = job.data[(i * pkg.Page):((i * pkg.Page) + pkg.Page)]
		}
		// TODO: move it to a worker
		slices.SortFunc(pages, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})
		for _, p := range pages {
			_, err = f.Write(p)
			check(err)
		}
		f.Sync()
		files[job.index] = f
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

func Split(config common.Config) []*os.File {
	err := os.RemoveAll("tmp")
	if err != nil {
	}
	err = os.MkdirAll("tmp", 0777)
	check(err)
	file, err := os.Open(config.FilePath)
	check(err)
	stat, err := file.Stat()
	check(err)
	fileSize := stat.Size()
	nFiles := int(fileSize / int64(pkg.GB*(config.NGb)))
	if fileSize%int64(config.NGb*pkg.GB) != 0 {
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
