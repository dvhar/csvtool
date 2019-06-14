package main
import (
	. "fmt"
	"os"
)

type DebugPrint struct {
	active bool
}
func (d DebugPrint) Print(args ...interface{}) {
	if d.active { Println(args...) }
}
var db DebugPrint

func runTests(doTest bool){
	if !doTest { return }
	db.active = true

	dir1 := "/home/dave/Documents/work/"
	file1 := "parkingTruncated.csv"
	f1 := " " + dir1 + file1 + " "

	var testQs = []string {
		// 1
		"select top 10 from"+f1+"where 'Issue Date' < 6/1/17",
		// 2
		"select top 10 from"+f1+"where 'Issue Date' between 6/1/16 and 7/1/16",
	}
	var testNums = []int{ 2, }

	for _,ii := range testNums {
		if err := runOneTestQuery(testQs[ii-1]); err != nil { return }
	}
	os.Exit(0)
}

func runOneTestQuery(query string) error {
	Println("=======================================================================================================")
	Println("testing query:",query)
	Println("=======================================================================================================")
	q := QuerySpecs{ queryString : query, }
	res, err := csvQuery(&q)
	if err != nil { Println("err:",err); os.Exit(1) }
	Println("number of colums:",res.Numcols)
	Println("number of rows:",res.Numrows)
	Println("types:",res.Types)
	Println("colnames:",res.Colnames)
	for ii := range res.Vals {
		Println("-----------------------------------------")
		Println(res.Vals[ii])
	}
	return nil
}
