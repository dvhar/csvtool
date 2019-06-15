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

type Test struct {
	query string
	title string
	good bool  //query is supposed to succeed
}

func runTests(doTest bool){
	db.verbose1 = true
	db.verbose2 = true
	//db.verbose3 = true
	if !doTest { return }

	dir1 := "/home/dave/Documents/work/"
	file1 := "parkingTest.csv"
	f1 := " " + dir1 + file1 + " "

	var tests = []Test {
		Test{"select top 2 from"+f1, "select all", true},
		Test{"select top 2 * from"+f1, "select all star", true},
		Test{`select top 2 c4 'Issue Date' c8+c12+10 as int-sum 'c8-int'=c8 c12 as 'c12-int' 
			c1+c2+10.2 as flt-add c1*c2*10.2 as flt-mult c2 / c1 / 10.2 as flt-div c2 - c1 - 10.2 as flt-sub
			from`+f1, "simple expressions and aliases", true},
		Test{` select top 2
				floaty = case c8
				when 7 then 7.12 when 40 then 40.23 when 47 then 47.234 when 36 then 30.32
				else 12.3 end
				case c8
				when 5 then 72.12 when 69 then 140.23 when 47 then 427.234 when 36 then 310.32 when 321 then 210.98
				else 612.3 end as floaty2
				from`+f1, "2 cases", true},
		Test{` select top 2
				case c8
				when 7 then 7.12 when 40 then 40.23 when 47 then 47.234 when 36 then 30.32
				else 12.3 end as floaty +
				case c8
				when 5 then 72.12 when 69 then 140.23 when 47 then 427.234 when 36 then 310.32 when 321 then 210.98
				else 612.3 end as addy
				from`+f1,
				"add 2 cases - malformed because alias in the middle", false},
	}

	for _,t := range tests {
		Println("=======================================================================================================")
		Println("test title:",t.title)
		Println("testing query:",t.query)
		Println("-------------------------------------------------------------------------------------------------------")
		err := runOneTestQuery(t.query)
		if t.good && err != nil { return }
		if !t.good && err == nil { return }
		Println("Test successful\n\n")
	}
	os.Exit(0)
}

func runOneTestQuery(query string) error {
	q := QuerySpecs{ queryString : query, }
	res, err := csvQuery(&q)
	if err != nil { Println("err:",err); return err }
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
