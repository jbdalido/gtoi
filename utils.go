package main

import (
	"fmt"
	"io/ioutil"
	"os"
)

// Open a single file
func OpenFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Failed Open %s\n", path)
	}

	return file, nil
}

func OpenAndReadFile(path string) ([]byte, error) {
	file, err := OpenFile(path)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return b, nil
}
