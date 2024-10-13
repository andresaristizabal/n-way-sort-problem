package sorting

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"n-way-sort/pkg/utils"
	"os"
	"slices"
)

type FilePerReader struct {
	file   *os.File
	reader *bufio.Reader
}

type PageShort struct {
	bytes         []byte
	filePerReader FilePerReader
}

type FileHeap []PageShort

func (f FileHeap) Len() int {
	return len(f)
}

func (f FileHeap) Less(i, j int) bool {
	a := f[i]
	b := f[j]
	return bytes.Compare(a.bytes, b.bytes) < 0
}

func (f FileHeap) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (f *FileHeap) Push(x any) {
	p, ok := x.(PageShort)
	if !ok {
		panic("not a page short pushed")
	}
	*f = append(*f, p)
}

func (f *FileHeap) Pop() any {
	old := *f
	n := len(old)
	x := old[n-1]
	*f = old[0 : n-1]
	return x
}

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

func (f *FileHeap) Start(writeChannel chan []byte, filePerReaders []FilePerReader, file *os.File) {
	capacityBuf := utils.GB * 1
	outputBuffer := make([]byte, 0, capacityBuf)
	for f.Len() > 0 {
		v := heap.Pop(f).(PageShort)
		//fmt.Println("Popped", v.bytes[0:10], " from file ", v.fileName)
		outputBuffer = append(outputBuffer, v.bytes...)
		page := make([]byte, utils.Page)
		fpr := v.filePerReader
		_, err := fpr.reader.Peek(len(page))

		if err != nil {
			nextFilePerReader, err := getNextFile(&filePerReaders, v, page)
			if err != nil {
				drainHeap(f, writeChannel, outputBuffer, file)
				return
			}
			fpr = nextFilePerReader
		}
		_, err = fpr.reader.Read(page)
		newPageShort := PageShort{
			bytes:         page,
			filePerReader: fpr,
		}
		heap.Push(f, newPageShort)
		if len(outputBuffer) == capacityBuf {
			fmt.Println("Writing to final file")
			writeChannel <- outputBuffer
			outputBuffer = make([]byte, 0, capacityBuf)
		}
	}
	close(writeChannel)
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
	_ = Split(config)
	files := make([]*os.File, 0, 25)
	filePerReaders := make([]FilePerReader, 0, 25)
	inputFile, err := os.Open(config.FilePath)
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

	err = os.Remove("tmp/final.txt")
	if err != nil {
	}
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
