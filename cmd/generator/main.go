package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"n-way-sort/pkg/utils"
	"os"
	"sync"
)

func worker(jobs chan int64, result chan []byte) {
	for job := range jobs {
		bytes := generateBuff(job)
		result <- bytes
	}
}

func main() {

	writeFileCh := make(chan []byte, 1)
	finish := make(chan int)

	nGb := flag.Int64("n-gigabyte", 1, "file size generated in GB")
	flag.Parse()

	fileSize := *nGb * utils.GB
	fmt.Println("file size:", fileSize)

	nBlocks := fileSize / utils.Page

	blocksByGroup := nBlocks / *nGb

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
	c := int64(0)

	go func() {
		for {
			v := <-writeFileCh
			b.Write(v)
			c++
			if *nGb == c {
				finish <- 1
				close(writeFileCh)
				return
			}
		}
	}()

	// Worker implementation
	nWorker := 4
	job := make(chan int64)

	// start workers
	for i := 0; i < nWorker; i++ {
		go worker(job, writeFileCh)
	}

	for i := int64(0); i < *nGb; i++ {
		job <- blocksByGroup
	}
	<-finish
}

func generateBuff(blocksByGroup int64) []byte {
	writeBf := make([]byte, blocksByGroup*utils.Page)
	var generateWaitGroup sync.WaitGroup
	for i := int64(0); i < blocksByGroup; i++ {
		generateWaitGroup.Add(1)
		buf := writeBf[utils.Page*i : utils.Page*i+utils.Page]
		go RandStringBytesMaskImpr(buf, &generateWaitGroup)
	}
	generateWaitGroup.Wait()
	return writeBf
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
