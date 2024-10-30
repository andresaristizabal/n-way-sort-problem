package sorting

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"n-way-sort/pkg/utils"
	"os"
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

func (f *FileHeap) Start(writeChannel chan []byte, filePerReaders []FilePerReader, file *os.File) {
	capacityBuf := utils.GB / 2
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
