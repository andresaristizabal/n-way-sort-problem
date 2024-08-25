package main

import (
	"flag"
	"fmt"
	"n-way-sort/pkg/utils"
	"os"
	"path"
)

func main() {
	folderPath := flag.String("folder-input", "", "input file")
	gbPerFile := flag.Int("gb-per-file", 2, "number of gb per file")
	flag.Parse()

	// read files from folderPath
	dir, err := os.ReadDir(*folderPath)
	utils.CheckError(err)
	files := make([]*os.File, 0, len(dir))
	bytesPerFile := int64(*gbPerFile * utils.GB)
	isThereAnyError := false
	for _, entry := range dir {
		f, e := os.OpenFile(path.Join(*folderPath, entry.Name()), os.O_RDONLY, 0666)
		utils.CheckError(e)
		files = append(files, f)
	}
	fmt.Println("Checking file's size")
	for _, f := range files {
		s, _ := f.Stat()
		if bytesPerFile != s.Size() {
			fmt.Sprintf("File %s has %d bytes, expected %d bytes", f.Name(), s.Size(), bytesPerFile)
			isThereAnyError = true
		}
	}

	if !isThereAnyError {
		fmt.Println("All files are ok")
	}

}
