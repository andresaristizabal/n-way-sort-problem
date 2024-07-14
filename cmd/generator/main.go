package main

import (
	"bufio"
	"flag"
	"log"
	"math/rand"
	"n-way-sort/pkg"
	"os"
	"sync"
)

func main() {

	writeFileCh := make(chan []byte)
	fileSize := flag.Int64("file-size", 1_073_741_824, "file size generated")
	nBuffers := flag.Int64("n-buffers", 10, "number of buffer groups")

	flag.Parse()

	var mainBufferWG sync.WaitGroup

	nBlocks := *fileSize / pkg.Page

	blocksByGroup := nBlocks / *nBuffers

	if blocksByGroup < 1 {
		panic("bad group blocks")
	}

	file, err := os.Create("input.txt")
	if err != nil {
		log.Panic("cannot create file")
	}
	defer file.Close()
	b := bufio.NewWriter(file)
	defer b.Flush()

	go func() {
		for {
			v, ok := <-writeFileCh
			if !ok {
				return
			}
			b.Write(v)
		}
	}()

	for i := int64(0); i < *nBuffers; i++ {
		mainBufferWG.Add(1)
		go generateBuff(writeFileCh, blocksByGroup, &mainBufferWG)
	}
	mainBufferWG.Wait()
	close(writeFileCh)
}

func generateBuff(writeFileCh chan []byte, blocksByGroup int64, wg *sync.WaitGroup) {
	writeBf := make([]byte, blocksByGroup*pkg.Page)
	var generateWaitGroup sync.WaitGroup
	for i := int64(0); i < blocksByGroup; i++ {
		generateWaitGroup.Add(1)
		buf := writeBf[pkg.Page*i : pkg.Page*i+pkg.Page]
		go RandStringBytesMaskImpr(buf, &generateWaitGroup)
	}
	generateWaitGroup.Wait()
	writeFileCh <- writeBf
	wg.Done()
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImpr(b []byte, wg *sync.WaitGroup) {
	n := 4096
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	wg.Done()
}
