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
    //parse helpers
    BT_SEL =      1<<28
    BT_WHR =      1<<29
    BT_AFTWR =    1<<30
    BT_AGG =      1<<19
    //final tokens
    WORD =        FINAL|1<<27
    NUMBER =      FINAL|iota
    KEYWORD =     FINAL|KEYBIT
    //keywords
    KW_AND =      BT_WHR|LOGOP|KEYWORD|iota
    KW_OR  =      BT_WHR|LOGOP|KEYWORD|iota
    KW_SELECT =   KEYWORD|iota
    KW_FROM  =    KEYWORD|iota
    KW_AS  =      KEYWORD|iota
    KW_WHERE =    BT_WHR|KEYWORD|iota
    KW_ORDER =    BT_AFTWR|KEYWORD|iota
    KW_BY =       BT_AFTWR|KEYWORD|iota
    KW_TOP =      BT_SEL|KEYWORD|iota
    KW_DISTINCT = BT_AGG|KEYWORD|iota
    KW_ORDHOW =   KEYWORD|iota
    KW_BETWEEN =  KEYWORD|iota
    KW_LIKE =     RELOP|KEYWORD|iota
    //special bits
    SPECIALBIT =  1<<21
    SPECIAL =      FINAL|SPECIALBIT
    //special tokens
    SP_EQ =        BT_WHR|RELOP|SPECIAL|iota
    SP_NOEQ =      BT_WHR|RELOP|SPECIAL|iota
    SP_LESS =      BT_WHR|RELOP|SPECIAL|iota
    SP_LESSEQ =    BT_WHR|RELOP|SPECIAL|iota
    SP_GREAT =     BT_WHR|RELOP|SPECIAL|iota
    SP_GREATEQ =   BT_WHR|RELOP|SPECIAL|iota
    SP_NEGATE =    BT_WHR|SPECIAL|iota
    SP_SQUOTE =    SPECIAL|iota
    SP_DQUOTE =    SPECIAL|iota
    SP_COMMA =     SPECIAL|iota
    SP_LPAREN =    BT_WHR|SPECIAL|iota
    SP_RPAREN =    BT_WHR|SPECIAL|iota
    SP_ALL =       BT_SEL|SPECIAL|iota
    //non-final states
    STATE_INITAL =    0
    STATE_SSPECIAL =  1
    STATE_DSPECIAL =  2
    STATE_MBSPECIAL = 3
    STATE_WORD =      4
    //BTokens - generated from first tokens in the parser
    BT_SCOL =      BT_SEL|iota
    BT_WCOL =      BT_WHR|iota
    BT_WCOMP =     BT_WHR|iota
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
    SP_ALL :        "SP_ALL",
    STATE_INITAL :  "STATE_INITAL",
    STATE_SSPECIAL :"STATE_SSPECIAL",
    STATE_DSPECIAL :"STATE_DSPECIAL",
    STATE_MBSPECIAL:"STATE_MBSPECIAL",
    STATE_WORD :    "STATE_WORD",
    BT_SEL :        "BT_SEL",
    BT_WHR :        "BT_WHR",
    BT_AFTWR :      "BT_AFTWR",
    BT_AGG :        "BT_AGG",
    BT_SCOL :       "BT_SCOL",
    BT_WCOL :       "BT_WCOL",
    BT_WCOMP :      "BT_WCOMP",
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
    "*" :  SP_ALL,
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
    table[0][0] =    STATE_INITAL
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


type AToken struct {
    Id int
    Val string
    Line int
}

var lineNo = 1
var colNo = 1
var waitForQuote int
func scanner(s* StringLookahead) AToken {
    initable()
    state := STATE_INITAL
    var nextState, nextchar int
    var S string

    for ; (state & FINAL) == 0 && state < NUM_STATES ; {
        nextState = table[state][s.peek()]
        if (nextState & ERROR) != 0 {
        //end of string
            if state == 255 { return AToken { Id: 255, Val: "END", Line: lineNo } }
            Printf("error peek: %d state: %d nextstates: %d nextchar: %c\n",s.peek(),state, nextState, nextchar)
            return AToken{ Id: ERROR, Val:"line:"+Itoa(lineNo)+"  col: "+Itoa(colNo), Line: lineNo }
        }

        if (nextState & FINAL) != 0 {
            //see if keyword or regular word
            if nextState == WORD {
                if kw,ok := keywordMap[strings.ToLower(S)];ok && waitForQuote == 0 {
                    //return keyword token
                    return AToken { Id: kw, Val: S, Line: lineNo }
                } else {
                    //return word token
                    return AToken { Id: nextState, Val: S, Line: lineNo }
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
                    return AToken { Id: sp, Val: S, Line: lineNo }
                } else {
                    println("error: unknown special. peek: "+Itoa(s.peek())+" state: "+Itoa(state)+" ns: "+Itoa(nextState));
                    println(enumMap[nextState])
                    return AToken{ Id: ERROR, Val:"line:"+Itoa(lineNo)+"  col: "+Itoa(colNo), Line: lineNo }
                }
            } else {
                return AToken { Id: nextState, Val: S, Line: lineNo }
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
                return AToken { Id: EOS, Val: "END", Line: lineNo }
            }
        }
    }
    return AToken { Id: EOS, Val: "END", Line: lineNo }


}

//type with lookahead methods for scanner
type StringLookahead struct {
    Str string
    idx int
    end bool
}
func (s* StringLookahead) getc() int {
    if s.idx < len(s.Str) {
        s.idx++
        if s.idx == len(s.Str) { s.end = true }
    } else {
        return EOS
    }
    return int(s.Str[s.idx-1])
}
func (s* StringLookahead) peek() int {
    if s.end || s.idx >= len(s.Str) { return EOS }
    return int(s.Str[s.idx])
}

//replecement for old version
func tokenizeQspec(q *QuerySpecs) error {
    //put input into datatype with peek and getc methods
    input := &StringLookahead{Str:q.Qstring}
    for {
        t := scanner(input)
        q.ATokArray = append(q.ATokArray, t)
        if t.Id == ERROR { return errors.New("scanner error: "+t.Val) }
        if t.Id == EOS { break }
    }
    //Println(q.ATokArray)
    return nil
}


