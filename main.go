package main

import (
	"flag"
	"fmt"
	"github.com/KitchenMishap/pudding-huffman/jobs"
)

func main() {
	//graphics.TestPgmHist()
	//bcd.TestBcdComplexity()

	// OPTION A: For deterministic results (debugging)
	// The "Modern Go" way to get a deterministic random generator
	//source := rand.NewSource(1)
	//deterministic := rand.New(source)

	// OPTION B: Random results (Production)
	//deterministic := nil

	var sDirFlag = flag.String("Dir", "", "Directory to serve data from")
	flag.Parse()

	var err error
	//err = jobs.GatherStatistics(*sDirFlag, deterministic)
	err = jobs.ComplexityToFile(*sDirFlag)

	if err != nil {
		fmt.Printf(err.Error())
	}
}
