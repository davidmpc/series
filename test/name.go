package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

const file = "./words"
const outFile = "./names"

func main() {
	b, _ := ioutil.ReadFile(file)
	words := strings.Fields(string(b))
	names := []string{}
	for i := 0; i < len(words); i++ {
		for j := 0; j < len(words); j++ {
			names = append(names, words[i]+words[j])
		}
	}
	fmt.Println(names, len(names))
	f, _ := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0644)
	for i, name := range names {
		f.Write([]byte(name + "  "))
		if (i+1)%10 == 0 {
			f.Write([]byte("\n\n"))
		}
	}
}
