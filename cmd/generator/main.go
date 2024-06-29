package main

import (
	"bufio"
	"log"
	"math/rand"
	"n-way-sort/pkg"
	"os"
)

func main() {

	mc := make(chan []byte)
	fileSize := int64(1_073_741_824)
	file, err := os.Create("input.txt")
	if err != nil {
		log.Panic("can not create file")
	}
	defer file.Close()
	b := bufio.NewWriter(file)
	defer b.Flush()
	sizeCh := writeFile(b, mc)

	go func() {
		for {
			<-sizeCh
			stat, err := file.Stat()
			if err != nil {
				panic("can not get file stat")
			}
			fileSizeC := stat.Size()
			if fileSizeC >= fileSize {
				close(mc)
				close(sizeCh)
				os.Exit(1)
			}
		}
	}()

	for true {
		mc <- RandStringBytes(pkg.Page)
	}

}

func writeFile(f *bufio.Writer, ch chan []byte) chan int {
	size := make(chan int)
	go func() {
		for {
			v := <-ch
			n, _ := f.Write(v)
			size <- n
		}
	}()
	return size
}

func RandStringBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Intn(255))
	}
	return b
}
