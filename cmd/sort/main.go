package main

import (
	"flag"
	"n-way-sort/pkg/sorting"
	"n-way-sort/pkg/utils"
)

func main() {
	filePath := flag.String("file-input", "", "input file")
	rWorkers := flag.Int("r-workers", 2, "number of read workers")
	wWorkers := flag.Int("w-workers", 2, "number of write workers")
	oWorkers := flag.Int("o-workers", 2, "number of write workers")
	nGb := flag.Int("n-gb", 2, "number of gb per file")
	maxRamGb := flag.Int("max-gb", 25, "Max RAM in GB")
	flag.Parse()
	config := utils.Config{
		FilePath: *filePath,
		RWorkers: *rWorkers,
		WWorkers: *wWorkers,
		OWorkers: *oWorkers,
		NGb:      *nGb,
		MaxRamGb: *maxRamGb * utils.GB,
	}
	sorting.Sort(config)
}
