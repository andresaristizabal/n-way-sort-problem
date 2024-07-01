package main

import (
	"bufio"
	"log"
	"math/rand"
	"n-way-sort/pkg"
	"os"
	"sync"
)

func main() {

	writeFileCh := make(chan []byte)
	fileSize := int64(20 * 1_073_741_824)
	var mainBufferWG sync.WaitGroup

	nBlocks := fileSize / pkg.Page

	nBuffers := int64(20)

	blocksByGroup := nBlocks / nBuffers

	if blocksByGroup < 1 {
		panic("bad group blocks")
	}

	file, err := os.Create("input.txt")
	if err != nil {
		log.Panic("can not create file")
	}
	defer file.Close()
	b := bufio.NewWriter(file)
	defer b.Flush()

	for i := int64(0); i < nBuffers; i++ {
		mainBufferWG.Add(1)
		go generateBuff(writeFileCh, blocksByGroup, &mainBufferWG)
	}

	go func() {
		for {
			v := <-writeFileCh
			b.Write(v)
		}
	}()
	mainBufferWG.Wait()
}

func generateBuff(writeFileCh chan []byte, blocksByGroup int64, wg *sync.WaitGroup) {
	writeBf := make([]byte, blocksByGroup*pkg.Page)
	var generateWaitGroup sync.WaitGroup
	for i := int64(0); i < blocksByGroup; i++ {
		generateWaitGroup.Add(1)
		go RandStringBytes(writeBf, pkg.Page*i, &generateWaitGroup)
	}
	generateWaitGroup.Wait()
	writeFileCh <- writeBf
	wg.Done()
}

func RandStringBytes(groupBuffer []byte, startOffset int64, group *sync.WaitGroup) {
	for i := int64(0); i < pkg.Page; i++ {
		groupBuffer[i+startOffset] = byte(rand.Intn(255))
	}
	group.Done()
}
