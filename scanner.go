package main
import (
	. "fmt"
	"errors"
	. "strconv"
	"strings"
)

const (
	//misc
	NUM_STATES =  5
	EOS =         255
	ERROR =       1<<23
	AGG_BIT =     1<<26
	//final bits
	FINAL =       1<<22
	KEYBIT =      1<<20
	LOGOP =       1<<24
	RELOP =       1<<25
	//final tokens
	WORD =        FINAL|1<<27
	NUMBER =      FINAL|iota
	KEYWORD =     FINAL|KEYBIT
	//keywords
	KW_AND =      LOGOP|KEYWORD|iota
	KW_OR  =      LOGOP|KEYWORD|iota
	KW_XOR =      LOGOP|KEYWORD|iota
	KW_SELECT =   KEYWORD|iota
	KW_FROM  =    KEYWORD|iota
	KW_HAVING  =  KEYWORD|iota
	KW_AS  =      KEYWORD|iota
	KW_WHERE =    KEYWORD|iota
	KW_GROUP =    KEYWORD|iota
	KW_ORDER =    KEYWORD|iota
	KW_BY =       KEYWORD|iota
	KW_TOP =      KEYWORD|iota
	KW_DISTINCT = KEYWORD|iota
	KW_ORDHOW =   KEYWORD|iota
	KW_CASE =     KEYWORD|iota
	KW_WHEN =     KEYWORD|iota
	KW_THEN =     KEYWORD|iota
	KW_ELSE =     KEYWORD|iota
	KW_END =      KEYWORD|iota
	KW_JOIN=      KEYWORD|iota
	KW_INNER=     KEYWORD|iota
	KW_OUTER=     KEYWORD|iota
	KW_LEFT=      KEYWORD|iota
	KW_RIGHT=     KEYWORD|iota
	KW_BETWEEN =  RELOP|KEYWORD|iota
	KW_LIKE =     RELOP|KEYWORD|iota
	KW_IN =       RELOP|KEYWORD|iota
	//not scanned as keywords but still using that for unique vals
	FN_SUM =      KEYWORD|AGG_BIT|iota
	FN_AVG =      KEYWORD|AGG_BIT|iota
	FN_MIN =      KEYWORD|AGG_BIT|iota
	FN_MAX =      KEYWORD|AGG_BIT|iota
	FN_COUNT =    KEYWORD|AGG_BIT|iota
	FN_ABS =      KEYWORD|iota
	FN_FORMAT =   KEYWORD|iota
	FN_COALESCE = KEYWORD|iota
	FN_YEAR =     KEYWORD|iota
	FN_MONTH =    KEYWORD|iota
	FN_MONTHNAME= KEYWORD|iota
	FN_WEEK =     KEYWORD|iota
	FN_WDAY =      KEYWORD|iota
	FN_WDAYNAME =  KEYWORD|iota
	FN_YDAY =     KEYWORD|iota
	FN_MDAY =     KEYWORD|iota
	FN_HOUR =     KEYWORD|iota
	//special bits
	SPECIALBIT =  1<<21
	SPECIAL =      FINAL|SPECIALBIT
	//special tokens
	SP_EQ =        RELOP|SPECIAL|iota
	SP_NOEQ =      RELOP|SPECIAL|iota
	SP_LESS =      RELOP|SPECIAL|iota
	SP_LESSEQ =    RELOP|SPECIAL|iota
	SP_GREAT =     RELOP|SPECIAL|iota
	SP_GREATEQ =   RELOP|SPECIAL|iota
	SP_NEGATE =    SPECIAL|iota
	SP_SQUOTE =    SPECIAL|iota
	SP_DQUOTE =    SPECIAL|iota
	SP_COMMA =     SPECIAL|iota
	SP_LPAREN =    SPECIAL|iota
	SP_RPAREN =    SPECIAL|iota
	SP_STAR =      SPECIAL|iota
	SP_DIV =       SPECIAL|iota
	SP_MOD =       SPECIAL|iota
	SP_CARROT =    SPECIAL|iota
	SP_MINUS =     SPECIAL|iota
	SP_PLUS =      SPECIAL|iota
	//non-final states
	STATE_INITAL =    0
	STATE_SSPECIAL =  1
	STATE_DSPECIAL =  2
	STATE_MBSPECIAL = 3
	STATE_WORD =      4
)
var enumMap = map[int]string {
	EOS :           "EOS",
	ERROR :         "ERROR",
	FINAL :         "FINAL",
	KEYBIT :        "KEYBIT",
	LOGOP :         "LOGOP",
	RELOP :         "RELOP",
	WORD :          "WORD",
	NUMBER :        "NUMBER",
	KEYWORD :       "KEYWORD",
	KW_AND :        "KW_AND",
	KW_OR  :        "KW_OR",
	KW_XOR  :       "KW_XOR",
	KW_SELECT :     "KW_SELECT",
	KW_FROM  :      "KW_FROM",
	KW_HAVING  :    "KW_HAVING",
	KW_AS  :        "KW_AS",
	KW_WHERE :      "KW_WHERE",
	KW_ORDER :      "KW_ORDER",
	KW_BY :         "KW_BY",
	KW_DISTINCT :   "KW_DISTINCT",
	KW_TOP :        "KW_TOP",
	KW_ORDHOW :     "KW_ORDHOW",
	KW_CASE :       "KW_CASE",
	KW_WHEN :       "KW_WHEN",
	KW_THEN :       "KW_THEN",
	KW_ELSE :       "KW_ELSE",
	KW_END :        "KW_END",
	SPECIALBIT :    "SPECIALBIT",
	SPECIAL :       "SPECIAL",
	SP_EQ :         "SP_EQ",
	SP_NEGATE :     "SP_NEGATE",
	SP_NOEQ :       "SP_NOEQ",
	SP_LESS :       "SP_LESS",
	SP_LESSEQ :     "SP_LESSEQ",
	SP_GREAT :      "SP_GREAT",
	SP_GREATEQ :    "SP_GREATEQ",
	SP_SQUOTE :     "SP_SQUOTE",
	SP_DQUOTE :     "SP_DQUOTE",
	SP_COMMA :      "SP_COMMA",
	SP_LPAREN :     "SP_LPAREN",
	SP_RPAREN :     "SP_RPAREN",
	SP_STAR :       "SP_STAR",
	SP_DIV :        "SP_DIV",
	SP_MOD :        "SP_MOD",
	SP_MINUS :      "SP_MINUS",
	SP_PLUS :       "SP_PLUS",
	STATE_INITAL :  "STATE_INITAL",
	STATE_SSPECIAL :"STATE_SSPECIAL",
	STATE_DSPECIAL :"STATE_DSPECIAL",
	STATE_MBSPECIAL:"STATE_MBSPECIAL",
	STATE_WORD :    "STATE_WORD",
}
//characters of special tokens
var specials = []int{ '*','=','!','<','>','\'','"','(',')',',','+','-','%','/','^' }
//non-alphanumeric characters of words
var others = []int{ '\\',':','_','.','[',']' }
var keywordMap = map[string]int {
	"and" :       KW_AND,
	"or" :        KW_OR,
	"xor" :       KW_XOR,
	"select" :    KW_SELECT,
	"from" :      KW_FROM,
	"having" :    KW_HAVING,
	"as" :        KW_AS,
	"where" :     KW_WHERE,
	"order" :     KW_ORDER,
	"by" :        KW_BY,
	"distinct" :  KW_DISTINCT,
	"top" :       KW_TOP,
	"asc" :       KW_ORDHOW,
	"between" :   KW_BETWEEN,
	"like" :      KW_LIKE,
	"case" :      KW_CASE,
	"when" :      KW_WHEN,
	"then" :      KW_THEN,
	"else" :      KW_ELSE,
	"end" :       KW_END,
	"in" :        KW_IN,
	"group" :     KW_GROUP,
	"not" :       SP_NEGATE,
}
//functions are normal words to avoid taking up too many words
//use map when parsing not scanning
var functionMap = map[string]int {
	"sum" :      FN_SUM,
	"avg" :      FN_AVG,
	"min" :      FN_MIN,
	"max" :      FN_MAX,
	"count" :    FN_COUNT,
	"abs" :      FN_ABS,
	"format" :   FN_FORMAT,
	"coalesce" : FN_COALESCE,
	"year" :     FN_YEAR,
	"month" :    FN_MONTH,
	"monthname": FN_MONTHNAME,
	"week" :     FN_WEEK,
	"day" :      FN_WDAY,
	"dayname" :  FN_WDAYNAME,
	"dayofyear": FN_YDAY,
	"dayofmonth":FN_MDAY,
	"dayofweek": FN_WDAY,
	"hour" :     FN_HOUR,
}
var joinMap = map[string]int {
	"inner" :  KW_INNER,
	"outer" :  KW_OUTER,
	"left" :   KW_LEFT,
	"join" :   KW_JOIN,
	"bjoin" :   KW_JOIN,
	"sjoin" :   KW_JOIN,
}
var specialMap = map[string]int {
	"=" :  SP_EQ,
	"!" :  SP_NEGATE,
	"<>" : SP_NOEQ,
	"<" :  SP_LESS,
	"<=" : SP_LESSEQ,
	">" :  SP_GREAT,
	">=" : SP_GREATEQ,
	"'" :  SP_SQUOTE,
	"\"" : SP_DQUOTE,
	"," :  SP_COMMA,
	"(" :  SP_LPAREN,
	")" :  SP_RPAREN,
	"*" :  SP_STAR,
	"+" :  SP_PLUS,
	"-" :  SP_MINUS,
	"%" :  SP_MOD,
	"/" :  SP_DIV,
	"^" :  SP_CARROT,
}
var table [NUM_STATES][256]int
var tabinit bool = false
func initable(){
	if tabinit { return }
	//initialize table to errr
	for ii:=0; ii<NUM_STATES; ii++ {
		for ij:=0; ij<255; ij++ { table[ii][ij] = ERROR }
	}
	//next state from initial
	for ii:=0; ii<len(others); ii++ { table[0][others[ii]] = STATE_WORD }
	for ii:=0; ii<len(specials); ii++ { table[0][specials[ii]] = STATE_SSPECIAL }
	table[0][255] =  EOS
	table[0][' '] =  STATE_INITAL
	table[0]['\n'] = STATE_INITAL
	table[0]['\t'] = STATE_INITAL
	table[0][';'] =  STATE_INITAL
	table[0][0] =	STATE_INITAL
	table[0]['<'] =  STATE_MBSPECIAL
	table[0]['>'] =  STATE_MBSPECIAL
	for ii:='a'; ii<='z'; ii++ { table[0][ii] = STATE_WORD }
	for ii:='A'; ii<='Z'; ii++ { table[0][ii] = STATE_WORD }
	for ii:='0'; ii<='9'; ii++ { table[0][ii] = STATE_WORD }
	//next state from single-char special
	for ii:='a'; ii<='z'; ii++ { table[STATE_SSPECIAL][ii] = SPECIAL }
	for ii:='A'; ii<='Z'; ii++ { table[STATE_SSPECIAL][ii] = SPECIAL }
	for ii:='0'; ii<='9'; ii++ { table[STATE_SSPECIAL][ii] = SPECIAL }
	for ii:=0; ii<len(others); ii++ { table[STATE_SSPECIAL][others[ii]] = SPECIAL }
	for ii:=0; ii<len(specials); ii++ { table[STATE_SSPECIAL][specials[ii]] = SPECIAL }
	table[STATE_SSPECIAL][';'] =  SPECIAL
	table[STATE_SSPECIAL][' '] =  SPECIAL
	table[STATE_SSPECIAL]['\n'] = SPECIAL
	table[STATE_SSPECIAL]['\t'] = SPECIAL
	table[STATE_SSPECIAL][EOS] =  SPECIAL
	//next state from must-be double-char special
	//table[2]['='] = STATE_SSPECIAL
	//next state from maybe double-char special
	table[STATE_MBSPECIAL]['='] =  STATE_MBSPECIAL
	table[STATE_MBSPECIAL]['>'] =  STATE_MBSPECIAL
	table[STATE_MBSPECIAL][';'] =  STATE_SSPECIAL
	table[STATE_MBSPECIAL][' '] =  STATE_SSPECIAL
	table[STATE_MBSPECIAL]['\n'] = STATE_SSPECIAL
	table[STATE_MBSPECIAL]['\t'] = STATE_SSPECIAL
	for ii:='a'; ii<='z'; ii++ { table[STATE_MBSPECIAL][ii] = SPECIAL }
	for ii:='A'; ii<='Z'; ii++ { table[STATE_MBSPECIAL][ii] = SPECIAL }
	for ii:='0'; ii<='9'; ii++ { table[STATE_MBSPECIAL][ii] = SPECIAL }
	for ii:=0; ii<len(others); ii++ { table[STATE_MBSPECIAL][others[ii]] = SPECIAL }
	//next state from word
	for ii:=0; ii<len(specials); ii++ { table[STATE_WORD][specials[ii]] = WORD }
	for ii:=0; ii<len(others); ii++ { table[STATE_WORD][others[ii]] = STATE_WORD }
	table[STATE_WORD][' '] =  WORD
	table[STATE_WORD]['\n'] = WORD
	table[STATE_WORD]['\t'] = WORD
	table[STATE_WORD][';'] =  WORD
	table[STATE_WORD][EOS] =  WORD
	for ii:='a'; ii<='z'; ii++ { table[STATE_WORD][ii] = STATE_WORD }
	for ii:='A'; ii<='Z'; ii++ { table[STATE_WORD][ii] = STATE_WORD }
	for ii:='0'; ii<='9'; ii++ { table[STATE_WORD][ii] = STATE_WORD }

	/*
	for ii:=0; ii< NUM_STATES; ii++{
		for ij:=0; ij< 255; ij++{
			Printf("[ %d ][ %c ]=%-34s",ii,ij,enumMap[table[ii][ij]])
		}
		Printf("\n")
	}
	*/

	tabinit = true
}


type Token struct {
	id int
	val string
	line int
	quoted bool
}
func (t Token) Lower() string {
	return strings.ToLower(t.val)
}

var lineNo = 1
var colNo = 1
var waitForQuote int
func scanner(s* StringLookahead) Token {
	initable()
	state := STATE_INITAL
	var nextState, nextchar int
	var S string

	for ; (state & FINAL) == 0 && state < NUM_STATES ; {
		nextState = table[state][s.peek()]
		if (nextState & ERROR) != 0 {
		//end of string
			if state == 255 { return Token { id: 255, val: "END", line: lineNo } }
			Printf("error peek: %d state: %d nextstates: %d nextchar: %c S: %s\n",s.peek(),state, nextState, nextchar,S)
			return Token{ id: ERROR, val:"line:"+Itoa(lineNo)+"  col: "+Itoa(colNo), line: lineNo }
		}

		if (nextState & FINAL) != 0 {
			//see if keyword or regular word
			if nextState == WORD {
				if kw,ok := keywordMap[strings.ToLower(S)];ok && waitForQuote == 0 {
					//return keyword token
					return Token { id: kw, val: S, line: lineNo }
				} else {
					//return word token
					return Token { id: nextState, val: S, line: lineNo }
				}
			//see if special type or something else
			} else if nextState == SPECIAL {
				if sp,ok := specialMap[S];ok {
					//find out if these tokens are in a quote to preserve whitespace
					if (sp == SP_SQUOTE || sp == SP_DQUOTE) && waitForQuote == 0 {
						waitForQuote = sp
					} else if (sp == SP_SQUOTE || sp == SP_DQUOTE) && waitForQuote == sp {
						waitForQuote = 0
					}
					//return special token
					return Token { id: sp, val: S, line: lineNo }
				} else {
					println("error: unknown special. peek: "+Itoa(s.peek())+" state: "+Itoa(state)+" ns: "+Itoa(nextState));
					println(enumMap[nextState])
					return Token{ id: ERROR, val:"line:"+Itoa(lineNo)+"  col: "+Itoa(colNo), line: lineNo }
				}
			} else {
				return Token { id: nextState, val: S, line: lineNo }
			}

		} else {
			state = nextState
			nextchar = s.getc()
			colNo++
			//include whitespace in the token when waiting for a closing quote
			if waitForQuote != 0 {
				S += string(nextchar)
			} else if (nextchar != ' ' && nextchar != '\t' && nextchar != '\n' && nextchar != ';'){
				S += string(nextchar)
			}
			if nextchar == '\n' { lineNo++; colNo=0 }
			if nextchar == EOS {
				return Token { id: EOS, val: "END", line: lineNo }
			}
		}
	}
	return Token { id: EOS, val: "END", line: lineNo }


}

//type with lookahead methods for scanner
type StringLookahead struct {
	Str string
	idx int
}
func (s* StringLookahead) getc() int {
	if s.idx >= len(s.Str) { return EOS }
	s.idx++
	return int(s.Str[s.idx-1])
}
func (s* StringLookahead) peek() int {
	if s.idx >= len(s.Str) { return EOS }
	return int(s.Str[s.idx])
}

//turn query text into tokens and check if ints are columns or numbers
func scanTokens(q *QuerySpecs) error {
	input := &StringLookahead{Str:q.QueryString}
	for {
		t := scanner(input)
		//turn tokens inside quotes into single token
		if t.id == SP_SQUOTE || t.id == SP_DQUOTE {
			quote := t.id
			S := ""
			for t = scanner(input); t.id != quote && t.id != EOS ; t = scanner(input) { S += t.val }
			if t.id != quote { return errors.New("Quote was not terminated") }
			t = Token{WORD,S,t.line,true}
		}
		q.tokArray = append(q.tokArray, t)
		if t.id == ERROR { return errors.New("scanner error: "+t.val) }
		if t.id == EOS { break }
	}
	return nil
}


