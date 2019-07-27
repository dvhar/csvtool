package main
import (
	. "fmt"
	"os"
	//"encoding/json"
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

func runTests(){
	db.verbose1 = true
	//db.verbose2 = true
	//db.verbose3 = true
	if !*flags.testing { return }

	dir1 := "/home/dave/Documents/work/"
	//dir1 := "/home/dave/testing/ram/"
	file1 := "parkingTest.csv"
	file2 := "parkingTestShort.csv"
	f1 := " '" + dir1 + file1 + "' "
	f2 := " '" + dir1 + file2 + "' "
	selectSet := 1
	whereSet := 1<<1
	fromSet := 1<<2
	thisTest := selectSet | fromSet | whereSet
	_,_,_ = f2, whereSet, fromSet

	var tests = []Test {
		Test{"select top 20 from"+f1, "select all", true, selectSet},
		Test{"select top 20 * from"+f1, "select all star", true, selectSet},
		Test{`select top 20 c5 from`+f1+`where c5 like '%ny%'`,  "case with multiple predicate types", true, selectSet},
		Test{`select top 20 c4 'Issue Date' c8+c12+10 as 'int-sum' 'c8-int'=c8 c12 as 'c12-int' 
			c1+c2+10.2 as 'flt-add' c1*c2*10.2 as 'flt-mult' c2 / c1 / 10.2 as 'flt-div' c2-c1-10.2 as 'flt-sub'
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
		Test{`select top 20 c1+c3 as 'f-i-sum' c1*c3 as 'f-i-mult' c1 - c3 as 'f-i-sub' c1 / c3 as 'f-i-div' c4+'1/12/1999' as 'c_str-l_date'
			c3+c4 as 'i-t-add' c16+c17 as 's_i_add'
			from`+f1, "good mixing types", true, selectSet},
		Test{`select top 20 c7+c8 from`+f1, "add date", false, selectSet},
		Test{`select top 20 c7*c8 from`+f1, "mult date", false, selectSet},
		Test{`select top 20 c4*8 from`+f1, "mult string", false, selectSet},
		Test{`select top 20 c16*c17 from`+f1, "mult string", false, selectSet},
		Test{`select top 20 mixpred=case
			when c5 like ny then likey when c1+c8 < 20 then 'int-flt' when c7 < 2017 then datecomp
			when c8+c17 < 20 then int-int end from`+f1, "case with multiple predicate types", true, selectSet},
		Test{`select top 20 casexpr=case c1+c8*c12
			when 23 then inty when 24.45 then floaty when 23*24.54 then combo when c2 then fcol when c19 then icol when c2+c19 then ficol
			else 234 end from`+f1, "case with mixed int/float comparision expressions", true, selectSet},
		Test{`select top 20 caseexpr=case c5
			when NY then new+york when MA then massechuestsskjsdlkj when VA then virginia when NJ then Jersy
			else flyover end from`+f1, "expression case with actual results", true, selectSet},
		Test{`select top 20-c1-c2 as confusing -c8 (-c2)+c8 from`+f1, "negations", true, selectSet},
		Test{`select top 20 c1 c2 from`+f1+`where c2<10*c1`, "compare floats, mix with int", true, whereSet},
		Test{`select top 20 c13 c14 from`+f1+`where c13!=c14`, "compare two ints with != operators", true, whereSet},
		Test{`select top 20 c13 c14 from`+f1+`where c13 <> c14`, "compare two ints with <> operator", true, whereSet},
		Test{`select top 20 c13 c14 from`+f1+`where c13 = c14`, "compare two ints", true, whereSet},
		Test{`select top 20 c5 c6 from`+f1+`where c5 like ny and c6 not like '%pas%'`, "like and not like", true, whereSet},
		Test{`select top 20 c7 from`+f1+`where c7 between '8/1/2016' and '10/30/2016'`, "between dates", true, whereSet},
		Test{`select top 20 c7 from`+f1+`where c7 not between '8/1/2016' and '10/30/2016'`, "not between dates", true, whereSet},
		Test{`select top 20 c7 from`+f2+`where c7 = '6/14/2017' and c7 != '6/14/2017'`, "date = contradiction", true, whereSet},
		Test{`select top 20 c7 from`+f2+`where c7 between '8/1/2016'
			and '10/30/2016' and c7 not between '8/1/2016' and '10/30/2016'`, "between dates contradiction", true, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067 or c4 = FZX9232) and ((((c4 = (GZH7067))) or c4=FZX9232))`,
			"predicate parens", true, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067 or c4)`, "predicate parens error", false, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067) and`, "dangling logop", false, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067) and c4+dog`, "dangling predicate expression", false, whereSet},
		Test{`select top 20 c4 from`+f1+`where (c4 = GZH7067`, "bad predicate parentheses", false, whereSet},
		Test{`select top 20 c4 c3 c2 c7 from`+f1+`where c4 = GZH7067 or c3 > 4006265037 or c2 >  72.12 or c7 > '6/1/2017'`,
			"different predicate types", true, whereSet},
		Test{`select top 20 c6 c10 case c6 when COM then 1 when OMT then 2 when PAS then 3 else 4 end
			case c10 when TOYOT then 1 when FORD then 2 when BMW then 3 else 4 end 
			from`+f1+`where case c6 when COM then 1 when OMT then 2 when PAS then 3 else 4 end =
			case c10 when TOYOT then 1 when FORD then 2 when BMW then 3 else 4 end `, "cases in predicates", true, whereSet},
		Test{`select top 10 c10 c8 c1 c7 case when c10=TOYOT then 1.2 when c8=36 or c1=30.32 then 48 when c7 > 'june 1 1017' then horse end
			from`+f1+`where case when c10=TOYOT then 1.2 when c8=36 or c1=30.32 then 48 when c7 > 'june 1 1017' then horse end = 48`,
			"predicate case with mixed types", true, whereSet},
		Test{`select from /home/bort/file.csv`, "no file", false, fromSet},
		Test{`select top 5 1 2 3 '1' '2' '3' from`+f1, "select numbers default", true, selectSet},
		Test{`c select top 5 1 2 3 '1' '2' '3' from`+f1, "select numbers c", true, selectSet},
		Test{`select top 5 1 2 3 '1' '2' '3' c1 c2 c3 from`+f1, "select numbers with c# default", true, selectSet},
		Test{`c select top 5 1 2 3 '1' '2' '3' c1 c2 c3 from`+f1, "select numbers with c# c", true, selectSet},
		Test{`select top 20 c1 c2 c37 c40 from`+f1+`where c1 = c2 or c37 = c40`, "where cols = each other", true, whereSet},
		Test{`select top 20 c38 from`+f1+`where c38 % 2 = 0`, "modulus", true, whereSet},
		Test{`select top 20 c38 from`+f1+`where c38 % 2.1 = 0`, "bad modulus", false, whereSet},
		Test{`select top 20 c38`+f1+`where c38 % 2.1 = 0`, "missing from", false, whereSet},
		Test{`select top 20 c3 c3 % 2 case when c3 % 2 = 1 then odd when not c3 % 2 = 1 then even end from`+f1, "case predicate negation", true, selectSet},
		Test{`select c1 c8 c13 'and' c6 'and' c9 c6 c12 from`+f1+`where c1 between c8 and c13 and c6 between COM and PAS and c9 between c6 and c12`, "between various types", true, selectSet},
		Test{`select top 10 c32 c33 from`+f1+`where c32 = null and c33 = null`,"and",true,whereSet},
		Test{`select top 10 c32 c33 from`+f1+`where c32 = null or c33 = null`,"or",true,whereSet},
		Test{`select top 10 c32 c33 from`+f1+`where c32 = null xor c33 = null`,"xor",true,whereSet},
		Test{`select top 10 c32 c33 from`+f1+`where not (c32 = null and c33 = null)`,"not and",true,whereSet},
		Test{`select top 10 c32 c33 from`+f1+`where not (c32 = null or c33 = null)`,"not or",true,whereSet},
		Test{`select top 10 c32 c33 from`+f1+`where not (c32 = null xor c33 = null)`,"not xor",true,whereSet},
		Test{`select top 20 ((c7+'19 years'*1.2) - '1/1/1997') + c7, c7, c7+'1.2 days', c7+'2 days' from`+f1,"date arithmetic",true,selectSet},
		Test{`select top 20 c7 + '19 years' + '88 days' + '2 weeks' from`+f1,"more date arithmetic",true,selectSet},
		Test{`select top 20 count(*), day(c7) c7 from`+f1+`group by day(c8)`,"function needs date not int",false,selectSet},
		Test{`select top 20 count(*), day(c6) c7 from`+f1+`group by day(c7)`,"function needs date not string",false,selectSet},
		Test{`select top 20 distinct day(c7) dayofweek(c7) dayofmonth(c7) hour(c7) dayname(c7) dayofyear(c7) week(c7+'8 weeks') month(c7) monthname(c7) year(c7) c7 abs(c8 - 40) as abs from`+f1,"non-aggregate funcions",true,selectSet},
		Test{"select top 20 from"+f1+"order by c5", "select all", true, selectSet},
		Test{`select top 20 monthname(c7) dayname(c7) week(c7) sum(c3) c5 from`+f1+`group by month(c7) week(c7) order by c5 asc `,"group sort",true,selectSet},
		Test{`c select top 2000 42 26 from`+f1+`where 42=null and 26<>null and 42='' and 26!=''`, "select where null and not null", true, whereSet},
		Test{`c select top 2000 42 26 from`+f1+`where 42=null*2`, "cant multiply null", false, whereSet},
		Test{`select top 20 c37 from`+f1+`where c37 = null`, "where int = null", true, whereSet},
		Test{`select top 20 c1 c2 from`+f1+`where c1 = null and c2 <> null`, "where float = null and not null", true, whereSet},
		Test{`select max(c3) as max min(c3) as min sum(c3) as sum avg(c3) as avg count(c3) as cnt c3 from`+f1,"aggregate with one group",true,selectSet},
		Test{`select count(c1) c5 from`+f1+`group by c5`,"aggregate with multiple groups",true,selectSet},
		Test{`select top 5 count(c1) c5 * from`+f1+`group by c5`,"limited aggregate with multiple groups",true,selectSet},
		Test{`select c5 c9 max(c3) as max min(c3) as min avg=avg(c3) caout = count(*) from`+f1+`group by c5 c6`,"nested groups",true,selectSet},
		Test{`select max(c3) as max min(c3) as min sum(c3) as sum avg(c3) as avg count(c3) as cnt c3 from`+f1+`group by c5`,"aggregate with groupings",true,selectSet},
		Test{`select top 20 count(c1) + count(c2) count(c1) count(c2) sum(c1+c2) + avg(c1+c2) avg(c1+c2) c7 from`+f1+`group by month(c7) order by c7`,"expression of aggregates",true,selectSet},
		Test{`select top 20 max(min(c1)) from`+f1+`group by month(c7) order by c7`,"nested aggregate error",false,selectSet},
		Test{`select top 20 max(c1)+c2*4 from`+f1,"aggregate add error",false,selectSet},
        Test{`select top 20 max(c1)*(c2+5) from`+f1,"aggregate mult error",false,selectSet},
        Test{`select top 20 from`+f1+`where max(c1) between min(c1) and c2`,"aggregate between error",false,selectSet},
        Test{`select top 20 from`+f1+`where max(c1) between c1 and max(c2)`,"aggregate between error",false,selectSet},
        Test{`select top 20 from`+f1+`where c1 between max(c1) and max(c2)`,"aggregate between error",false,selectSet},
        Test{`select top 20 from`+f1+`where c1 between c1 and max(c2)`,"aggregate between error",false,selectSet},
        Test{`select top 20 case c1 when c2 then a when max(c2) then b end from`+f1,"aggregate case error",false,selectSet},
        Test{`select top 20 case max(c1) when c2 then a when max(c2) then b end from`+f1,"aggregate case error",false,selectSet},
        Test{`select top 20 case max(c1) when c2 then a when c2 then b end from`+f1,"aggregate case error",false,selectSet},
        Test{`select top 20 case c1 when c2 then a else max(c2) end from`+f1,"aggregate case error",false,selectSet},
		Test{`select count(*) month('Issue Date') from`+f1+`group by month('Issue Date') having count(*) between 80 and 100`,"having clause",true,whereSet},
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
	if err != nil { return err }
	Println("number of colums:",res.Numcols)
	Println("number of rows:",res.Numrows)
	Println("types:",res.Types)
	Println("colnames:",res.Colnames)
	for ii := range res.Vals {
		Println("-----------------------------------------")
		Println(res.Vals[ii])
	}
	//js,_ := json.Marshal(res)
	//Println("json:",string(js))
	return nil
}
