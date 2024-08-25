package utils

const (
	Page = 4096
	GB   = 1_073_741_824
)

type Config struct {
	FilePath string
	RWorkers int
	WWorkers int
	NGb      int
}

func CheckError(e error) {
	if e != nil {
		panic(e)
	}
}
