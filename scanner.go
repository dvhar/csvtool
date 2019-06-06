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
	KW_SELECT =   KEYWORD|iota
	KW_FROM  =    KEYWORD|iota
	KW_AS  =      KEYWORD|iota
	KW_WHERE =    KEYWORD|iota
	KW_ORDER =    KEYWORD|iota
	KW_BY =       KEYWORD|iota
	KW_TOP =      KEYWORD|iota
	KW_DISTINCT = KEYWORD|iota
	KW_ORDHOW =   KEYWORD|iota
	KW_BETWEEN =  KEYWORD|iota
	KW_CASE =     KEYWORD|iota
	KW_LIKE =     RELOP|KEYWORD|iota
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
	SP_STAR =       SPECIAL|iota
	SP_DIV =       SPECIAL|iota
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
	KW_SELECT :     "KW_SELECT",
	KW_FROM  :      "KW_FROM",
	KW_AS  :        "KW_AS",
	KW_WHERE :      "KW_WHERE",
	KW_ORDER :      "KW_ORDER",
	KW_BY :         "KW_BY",
	KW_DISTINCT :   "KW_DISTINCT",
	KW_TOP :        "KW_TOP",
	KW_ORDHOW :     "KW_ORDHOW",
	KW_CASE :       "KW_CASE",
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
	SP_STAR :        "SP_STAR",
	SP_DIV :        "SP_DIV",
	SP_MINUS :      "SP_MINUS",
	SP_PLUS :       "SP_PLUS",
	STATE_INITAL :  "STATE_INITAL",
	STATE_SSPECIAL :"STATE_SSPECIAL",
	STATE_DSPECIAL :"STATE_DSPECIAL",
	STATE_MBSPECIAL:"STATE_MBSPECIAL",
	STATE_WORD :    "STATE_WORD",
}
//characters of special tokens
var specials = []int{ '*','=','!','<','>','\'','"','(',')',',' }
//non-alphanumeric characters of words
var others = []int{ '/','\\',':','-','_','.','%','[',']','^' }
var keywordMap = map[string]int {
	"and" :       KW_AND,
	"or" :        KW_OR,
	"select" :    KW_SELECT,
	"from" :      KW_FROM,
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
	"not" :       SP_NEGATE,
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
	"/" :  SP_DIV,
	"-" :  SP_MINUS,
	"+" :  SP_PLUS,
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
	Id int
	Val string
	Line int
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
			if state == 255 { return Token { Id: 255, Val: "END", Line: lineNo } }
			Printf("error peek: %d state: %d nextstates: %d nextchar: %c\n",s.peek(),state, nextState, nextchar)
			return Token{ Id: ERROR, Val:"line:"+Itoa(lineNo)+"  col: "+Itoa(colNo), Line: lineNo }
		}

		if (nextState & FINAL) != 0 {
			//see if keyword or regular word
			if nextState == WORD {
				if kw,ok := keywordMap[strings.ToLower(S)];ok && waitForQuote == 0 {
					//return keyword token
					return Token { Id: kw, Val: S, Line: lineNo }
				} else {
					//return word token
					return Token { Id: nextState, Val: S, Line: lineNo }
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
					return Token { Id: sp, Val: S, Line: lineNo }
				} else {
					println("error: unknown special. peek: "+Itoa(s.peek())+" state: "+Itoa(state)+" ns: "+Itoa(nextState));
					println(enumMap[nextState])
					return Token{ Id: ERROR, Val:"line:"+Itoa(lineNo)+"  col: "+Itoa(colNo), Line: lineNo }
				}
			} else {
				return Token { Id: nextState, Val: S, Line: lineNo }
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
				return Token { Id: EOS, Val: "END", Line: lineNo }
			}
		}
	}
	return Token { Id: EOS, Val: "END", Line: lineNo }


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

//turn query text into tokens
func scanTokens(q *QuerySpecs) error {
	input := &StringLookahead{Str:q.queryString}
	for {
		t := scanner(input)
		q.tokArray = append(q.tokArray, t)
		if t.Id == ERROR { return errors.New("scanner error: "+t.Val) }
		if t.Id == EOS { break }
	}
	return nil
}


