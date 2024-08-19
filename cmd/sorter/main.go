package main

import (
	"flag"
	"n-way-sort/cmd/sorter/common"
	"n-way-sort/cmd/sorter/split"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	filePath := flag.String("file-input", "", "input file")
	rWorkers := flag.Int("r-workers", 2, "number of read workers")
	wWorkers := flag.Int("w-workers", 2, "number of write workers")
	nGb := flag.Int("n-gb", 2, "number of gb per file")
	flag.Parse()
	config := common.Config{
		FilePath: *filePath,
		RWorkers: *rWorkers,
		WWorkers: *wWorkers,
		NGb:      *nGb,
	}
	split.Split(config)
}
