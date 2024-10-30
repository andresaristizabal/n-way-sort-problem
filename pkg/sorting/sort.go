package sorting

import (
	"bufio"
	"container/heap"
	"fmt"
	"n-way-sort/pkg/utils"
	"os"
	"slices"
)

func getNextFile(filePerReaders *[]FilePerReader, currentPageShort PageShort, page []byte) (FilePerReader, error) {
	newFilePerReaders := slices.DeleteFunc(*filePerReaders, func(fr FilePerReader) bool {
		return fr.file == nil || fr.file.Name() == currentPageShort.filePerReader.file.Name()
	})
	*filePerReaders = newFilePerReaders
	if len(newFilePerReaders) != 0 {
		newFPR := newFilePerReaders[0]
		_, err := newFPR.reader.Peek(len(page))
		if err != nil {
			nextFile, err := getNextFile(filePerReaders, PageShort{
				bytes:         []byte{},
				filePerReader: newFPR,
			}, page)
			if err != nil {
				return FilePerReader{}, fmt.Errorf("no more files")
			}
			return nextFile, nil
		} else {
			return newFPR, nil
		}
	}
	return FilePerReader{}, fmt.Errorf("no more files")
}

func drainHeap(f *FileHeap, writeCh chan []byte, outputBuffer []byte, file *os.File) {
	// drain the current outputBuffer
	writeCh <- outputBuffer
	drainOutputBuffer := make([]byte, 0, f.Len()*utils.Page)
	for f.Len() >= 0 {
		v := heap.Pop(f).(PageShort)
		drainOutputBuffer = append(drainOutputBuffer, v.bytes...)
		if f.Len() == 0 {
			_, err := file.Write(drainOutputBuffer)
			utils.CheckError(err)
			return
		}
	}
}

func wWorker(bytes <-chan []byte, file *os.File) {
	for b := range bytes {
		_, err := file.Write(b)
		utils.CheckError(err)
	}
}

func Sort(config utils.Config) {
	if !config.OnlySort {
		_ = Split(config)
	} else {
		fmt.Println("skip split and use tmp files")
	}
	if config.OnlySplit {
		fmt.Println("Only split")
		return
	}
	err := os.Remove("tmp/final.txt")
	if err != nil {
	}
	files := make([]*os.File, 0)
	filePerReaders := make([]FilePerReader, 0)
	utils.CheckError(err)
	stat, err := inputFile.Stat()
	dir, _ := os.ReadDir("tmp")
	for i := 0; i < len(dir); i++ {
		f, _ := os.Open(fmt.Sprintf("tmp/file-%d.txt", i))
		bufio.NewReader(f)
		files = append(files, f)
	}
	// TODO: check if this is compitable with Page size and improve it
	initialBytesPerFile := int(utils.GB / (4 * (stat.Size() / utils.GB) / 25))
	h := &FileHeap{}

	fmt.Println("Creating final reader")
	finalFile, err := os.Create("tmp/final.txt")
	utils.CheckError(err)
	writeChannel := make(chan []byte)
	go wWorker(writeChannel, finalFile)
	// Init heap values
	fmt.Printf("Initiating heap with %d files \n", len(files))

	for fileIndex := 0; fileIndex < len(files); fileIndex++ {
		file := files[fileIndex]
		reader := bufio.NewReaderSize(file, initialBytesPerFile)
		filePerReaders = append(filePerReaders, FilePerReader{
			file:   file,
			reader: reader,
		})
	}

	for _, filePerReader := range filePerReaders {
		if initialBytesPerFile%utils.Page != 0 {
			panic("page size is not compatible")
		}
		buf := make([]byte, initialBytesPerFile)
		_, e := filePerReader.reader.Read(buf)
		utils.CheckError(e)
		numberOfPages := initialBytesPerFile / utils.Page
		for i := 0; i < numberOfPages; i++ {
			heap.Push(h, PageShort{
				bytes:         buf[(i * utils.Page):((i * utils.Page) + utils.Page)],
				filePerReader: filePerReader,
			})
		}
	}
	h.Start(writeChannel, filePerReaders, finalFile)
}
