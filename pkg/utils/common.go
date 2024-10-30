package utils

import "flag"

const (
	Page = 4096
	GB   = 1_073_741_824
)

type Config struct {
	FilePath  string
	RWorkers  int
	WWorkers  int
	OWorkers  int
	NGb       int
	MaxRamGb  int
	OnlySplit bool
	OnlySort  bool
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
	onlySplit := flag.Bool("only-split", false, "only split the input file")
	onlySort := flag.Bool("only-sort", false, "only sort using tmp temporal files folder")
	flag.Parse()
	config := Config{
		FilePath:  *filePath,
		RWorkers:  *rWorkers,
		WWorkers:  *wWorkers,
		OWorkers:  *oWorkers,
		NGb:       *nGb,
		MaxRamGb:  *maxRamGb * GB,
		OnlySplit: *onlySplit,
		OnlySort:  *onlySort,
	}
	return config
}
