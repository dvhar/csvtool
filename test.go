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
	set int //bits for which set of tests it belongs to
}

func runTests(doTest bool){
	//db.verbose1 = true
	//db.verbose2 = true
	//db.verbose3 = true
	if !doTest { return }

	//dir1 := "/home/dave/Documents/work/"
	dir1 := "/home/dave/testing/ram/"
	file1 := "parkingTest.csv"
	file2 := "parkingTestShort.csv"
	f1 := " " + dir1 + file1 + " "
	f2 := " " + dir1 + file2 + " "
	selectSet := 1
	whereSet := 1<<1
	thisTest := whereSet | selectSet //can be bit | combo of sets

	var tests = []Test {
		Test{"select top 20 from"+f1, "select all", true, selectSet},
		Test{"select top 20 * from"+f1, "select all star", true, selectSet},
		Test{`select top 20 c4 'Issue Date' c8+c12+10 as int-sum 'c8-int'=c8 c12 as 'c12-int' 
			c1+c2+10.2 as flt-add c1*c2*10.2 as flt-mult c2 / c1 / 10.2 as flt-div c2 - c1 - 10.2 as flt-sub
			from`+f1, "simple expressions and aliases", true, selectSet},
		Test{`select top 20
			floaty = case c8
			when 7 then 7.12 when 40 then 40.23 when 47 then 47.234 when 36 then 30.32
			else 12.3 end
			case c8
			when 5 then 72.12 when 69 then 140.23 when 47 then 427.234 when 36 then 310.32 when 321 then 210.98
			else 612.3 end as floaty2
			from`+f1, "2 cases", true, selectSet},
		Test{` select top 20
			case c8
			when 7 then 7.12 when 40 then 40.23 when 47 then 47.234 when 36 then 30.32
			else 12.3 end as floaty +
			case c8
			when 5 then 72.12 when 69 then 140.23 when 47 then 427.234 when 36 then 310.32 when 321 then 210.98
			else 612.3 end as addy
			from`+f1,
			"add 2 cases - malformed because alias in the middle", false, selectSet},
		Test{`select top 20 c1+c3 as f-i-sum c1*c3 as f-i-mult c1 - c3 as f-i-sub c1 / c3 as f-i-div c4+'1/12/1999' as c_str-l_date
			c3+c4 as i-t-add c16+c17 as s_i_add
			from`+f1, "good mixing types", true, selectSet},
		Test{`select top 20 c7+c8 from`+f1, "add date", false, selectSet},
		Test{`select top 20 c7*c8 from`+f1, "mult date", false, selectSet},
		Test{`select top 20 c4*8 from`+f1, "mult string", false, selectSet},
		Test{`select top 20 c16*c17 from`+f1, "mult string", false, selectSet},
		Test{`select top 20 mixpred=case
			when c5 like ny then likey when c1+c8 < 20 then int-flt when c7 < 2017 then datecomp
			when c8+c17 < 20 then int-int end from`+f1, "case with multiple predicate types", true, selectSet},
		Test{`select top 20 casexpr=case c1+c8*c12
			when 23 then inty
			when 24.45 then floaty
			when 23*24.54 then combo
			when c2 then fcol
			when c19 then icol
			when c2+c19 then ficol
			else 234 end from`+f1, "case with mixed int/float comparision expressions", true, selectSet},
		Test{`select top 20 caseexpr=case c5
			when NY then new+york
			when MA then massechuestsskjsdlkj
			when VA then virginia
			when NJ then Jersy
			else flyover end from`+f1, "expression case with actual results", true, selectSet},
		Test{`select top 20 - c1 - c2 as confusing - c8 (- c2)+c8 from`+f1, "negations", true, selectSet},
		Test{`select top 20 c1 c2 from`+f1+`where c2 < 10*c1`, "compare floats, mix with int", true, whereSet},
		Test{`select top 20 c13 c14 from`+f1+`where c13 != c14`, "compare two ints with != operators", true, whereSet},
		Test{`select top 20 c13 c14 from`+f1+`where c13 <> c14`, "compare two ints with <> operator", true, whereSet},
		Test{`select top 20 c13 c14 from`+f1+`where c13 = c14`, "compare two ints", true, whereSet},
		Test{`select top 20 c5 c6 from`+f1+`where c5 like ny and c6 not like %pas%`, "like and not like", true, whereSet},
		Test{`select top 20 c7 from`+f1+`where c7 between 8/1/2016 and 10/30/2016`, "between dates", true, whereSet},
		Test{`select top 20 c7 from`+f1+`where c7 not between 8/1/2016 and 10/30/2016`, "not between dates", true, whereSet},
		Test{`select top 20 c7 from`+f2+`where c7 = 6/14/2017 and c7 != 6/14/2017`, "date = contradiction", true, whereSet},
		Test{`select top 20 c7 from`+f2+`where c7 between 8/1/2016
			and 10/30/2016 and c7 not between 8/1/2016 and 10/30/2016`, "between dates contradiction", true, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067 or c4 = FZX9232) and ((((c4 = (GZH7067))) or c4=FZX9232))`,
			"predicate parens", true, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067 or c4)`, "predicate parens error", false, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067) and`, "dangling logop", false, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067) and c4+dog`, "dangling predicate expression", false, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067`, "bad predicate parentheses", false, whereSet},
		Test{`select top 20 c4 c3 c2 c7 from`+f1+`where c4 = GZH7067 or c3 > 4006265037 or c2 >  72.12 or c7 > 6/1/2017`,
			"different predicate types", true, whereSet},
		Test{`select top 20 c6 c10 case c6 when COM then 1 when OMT then 2 when PAS then 3 else 4 end
			case c10 when TOYOT then 1 when FORD then 2 when BMW then 3 else 4 end 
			from`+f1+`where case c6 when COM then 1 when OMT then 2 when PAS then 3 else 4 end =
			case c10 when TOYOT then 1 when FORD then 2 when BMW then 3 else 4 end `, "cases in predicates", true, whereSet},
	}

	for _,t := range tests {
		if (t.set & thisTest) == 0 { continue }
		Println("=======================================================================================================")
		Println("test title:",t.title)
		Println("testing query:",t.query)
		Println("-------------------------------------------------------------------------------------------------------")
		err := runOneTestQuery(t.query)
		if t.good && err != nil { os.Exit(1) }
		if !t.good && err == nil { os.Exit(1) }
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
