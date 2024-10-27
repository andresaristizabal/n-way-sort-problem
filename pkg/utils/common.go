package utils

import "flag"

const (
	Page = 4096
	GB   = 1_073_741_824
)

type Config struct {
	FilePath string
	RWorkers int
	WWorkers int
	OWorkers int
	NGb      int
	MaxRamGb int
}

func CheckError(e error) {
	if e != nil {
		panic(e)
	}
}

func LoadConfig() Config {
	filePath := flag.String("file-input", "", "input file")
	rWorkers := flag.Int("r-workers", 2, "number of read workers")
	wWorkers := flag.Int("w-workers", 2, "number of write workers")
	oWorkers := flag.Int("o-workers", 2, "number of write workers")
	nGb := flag.Int("n-gb", 2, "number of gb per file")
	maxRamGb := flag.Int("max-gb", 25, "Max RAM in GB")
	flag.Parse()
	config := Config{
		FilePath: *filePath,
		RWorkers: *rWorkers,
		WWorkers: *wWorkers,
		OWorkers: *oWorkers,
		NGb:      *nGb,
		MaxRamGb: *maxRamGb * GB,
	}
	return config
}
