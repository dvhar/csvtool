package main
import (
	. "fmt"
	"os"
)

type DebugPrint struct {
	verbose1 bool
	verbose2 bool
	verbose3 bool
}
func (d DebugPrint) Print1(args ...interface{}) { if d.verbose1 { Println(args...) } }
func (d DebugPrint) Print2(args ...interface{}) { if d.verbose2 { Println(args...) } }
func (d DebugPrint) Print3(args ...interface{}) { if d.verbose3 { Println(args...) } }
var db DebugPrint

func runTests(doTest bool){
	if !doTest { return }
	db.verbose1 = true

	dir1 := "/home/dave/Documents/work/"
	file1 := "parkingTruncated.csv"
	f1 := " " + dir1 + file1 + " "

	var testQs = []string {
		// 1
		"select top 10 from"+f1+"where 'Issue Date' between 6/1/16 and 1/1/17",
		// 2
		"select top 10 from"+f1+"where 'Issue Date' between 6/1/16 and '234324'",
	}
	var testNums = []int{ 1,2, }

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
