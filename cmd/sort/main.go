package main

import (
	"n-way-sort/pkg/sorting"
	"n-way-sort/pkg/utils"
)

func main() {
	config := utils.LoadConfig()
	sorting.Sort(config)
}
