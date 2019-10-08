package main
import (
	"encoding/json"
	"time"
	. "fmt"
	"regexp"
	"math"
	bt "github.com/google/btree"
)
//interface to simplify operations with various datatypes
type Value interface {
	Greater(other Value) bool
	GreatEq(other Value) bool
	Less(other bt.Item) bool
	LessEq(other Value) bool
	Equal(other Value) bool
	Add(other Value) Value
	Sub(other Value) Value
	Mult(other Value) Value
	Div(other Value) Value
	Mod(other Value) Value
	Pow(other Value) Value
	String() string
	MarshalJSON() ([]byte,error)
}

type StdDevVal struct {
	nums []float
	mean float
	samp int8
}
func (a StdDevVal) Add(other Value) Value       {
	if _,ok:=other.(float);!ok{return null("")};
	return StdDevVal{ append(a.nums, other.(float)), a.mean.Add(other.(float)).(float), a.samp }
}
func (a StdDevVal) String() string              { return a.Eval().String() }
func (a StdDevVal) MarshalJSON() ([]byte,error) { return json.Marshal(a.String()) }
func (a StdDevVal) Greater(other Value) bool    { return false }
func (a StdDevVal) GreatEq(other Value) bool    { return false }
func (a StdDevVal) Less(other bt.Item) bool     { return false }
func (a StdDevVal) LessEq(other Value) bool     { return false }
func (a StdDevVal) Equal(other Value) bool      { return false }
func (a StdDevVal) Sub(other Value) Value       { return a }
func (a StdDevVal) Mult(other Value) Value      { return a }
func (a StdDevVal) Div(other Value) Value       { return a }
func (a StdDevVal) Mod(other Value) Value       { return a }
func (a StdDevVal) Pow(other Value) Value       { return a }
func (a StdDevVal) Eval() Value                 {
	count := float(len(a.nums))
	if count == 0 { return null("") }
	mean := a.mean.Div(count)
	sum := float(0)
	for _,v := range a.nums { sum = sum.Add(v.Sub(mean).Pow(float(2))).(float) }
	return sum.Div(count-float(a.samp)).Pow(float(0.5))
}

type AverageVal struct {
	val Value
	count integer
}
func (a AverageVal) Add(other Value) Value       { return AverageVal{ a.val.Add(other), a.count + 1, } }
func (a AverageVal) String() string              { return a.Eval().String() }
func (a AverageVal) MarshalJSON() ([]byte,error) { return json.Marshal(a.String()) }
func (a AverageVal) Greater(other Value) bool    { return false }
func (a AverageVal) GreatEq(other Value) bool    { return false }
func (a AverageVal) Less(other bt.Item) bool     { return false }
func (a AverageVal) LessEq(other Value) bool     { return false }
func (a AverageVal) Equal(other Value) bool      { return false }
func (a AverageVal) Sub(other Value) Value       { return a }
func (a AverageVal) Mult(other Value) Value      { return a }
func (a AverageVal) Div(other Value) Value       { return a }
func (a AverageVal) Mod(other Value) Value       { return a }
func (a AverageVal) Pow(other Value) Value       { return a }
func (a AverageVal) Eval() Value                 { return a.val.Div(a.count) }

type float float64
type integer int
type date struct {val time.Time}
type duration struct {val time.Duration}
type text string
type null string
type liker struct {val *regexp.Regexp}

func (a float) Less(other bt.Item) bool      {
	switch o := other.(type) {
		case float:   return a < o
		case integer: return a < float(o)
	}
	return false
}
func (a integer) Less(other bt.Item) bool    { if _,ok:=other.(integer);!ok  {return false};return a < other.(integer) }
func (a date) Less(other bt.Item) bool       { if _,ok:=other.(date);!ok     {return false};return a.val.Before(other.(date).val) }
func (a duration) Less(other bt.Item) bool   {
	switch o := other.(type) {
		case duration: return a.val < o.val
		case integer:  return a.val < time.Duration(o) //for abs()
	}
	return false
}
func (a text) Less(other bt.Item) bool       { if _,ok:=other.(text);!ok     {return false};return a < other.(text) }
func (a null) Less(other bt.Item) bool       { if _,ok:=other.(null);ok      {return false};return true }
func (a liker) Less(other bt.Item) bool      { return false }

func (a float) LessEq(other Value) bool    { if _,ok:=other.(float);!ok    {return false};return a <= other.(float) }
func (a integer) LessEq(other Value) bool  { if _,ok:=other.(integer);!ok  {return false};return a <= other.(integer) }
func (a date) LessEq(other Value) bool     { if _,ok:=other.(duration);!ok {return false};return !a.val.After(other.(date).val) }
func (a duration) LessEq(other Value) bool { if _,ok:=other.(date);!ok     {return false};return a.val <= other.(duration).val }
func (a text) LessEq(other Value) bool     { if _,ok:=other.(text);!ok     {return false};return a <= other.(text) }
func (a null) LessEq(other Value) bool     { return false }
func (a liker) LessEq(other Value) bool    { return false }

func (a float) Greater(other Value) bool   { if _,ok := other.(float); !ok    { return true } else {return a > other.(float) } }
func (a integer) Greater(other Value) bool { if _,ok := other.(integer); !ok  { return true } else {return a > other.(integer) } }
func (a date) Greater(other Value) bool    { if _,ok := other.(date); !ok     { return true } else {return a.val.After(other.(date).val) } }
func (a duration) Greater(other Value) bool{ if _,ok := other.(duration); !ok { return true } else {return a.val > other.(duration).val } }
func (a text) Greater(other Value) bool    { if _,ok := other.(text); !ok     { return true } else {return a > other.(text) } }
func (a null) Greater(other Value) bool    { if o,ok := other.(null); ok      { return a > o } else {return false} }
func (a liker) Greater(other Value) bool   { return false }

func (a float) GreatEq(other Value) bool   { if _,ok:=other.(float);!ok    {return true};return a >= other.(float) }
func (a integer) GreatEq(other Value) bool { if _,ok:=other.(integer);!ok  {return true};return a >= other.(integer) }
func (a date) GreatEq(other Value) bool    { if _,ok:=other.(date);!ok     {return true};return !a.val.Before(other.(date).val) }
func (a duration) GreatEq(other Value) bool{ if _,ok:=other.(duration);!ok {return true};return a.val > other.(duration).val }
func (a text) GreatEq(other Value) bool    { if _,ok:=other.(text);!ok     {return true};return a >= other.(text) }
func (a null) GreatEq(other Value) bool    { return false }
func (a liker) GreatEq(other Value) bool   { return false }

func (a float) Equal(other Value) bool     { if _,ok:=other.(float);!ok     {return false};return a == other.(float) }
func (a integer) Equal(other Value) bool   { if _,ok:=other.(integer);!ok   {return false};return a == other.(integer) }
func (a date) Equal(other Value) bool      { if _,ok:=other.(date);!ok      {return false};return a.val.Equal(other.(date).val) }
func (a duration) Equal(other Value) bool  { if _,ok:=other.(duration);!ok  {return false};return a.val == other.(duration).val }
func (a text) Equal(other Value) bool      { if _,ok:=other.(text);!ok      {return false};return a == other.(text) }
func (a null) Equal(other Value) bool      { if _,ok := other.(null);ok     {return true };return false }
func (a liker) Equal(other Value) bool     { return a.val.MatchString(other.String()) }

func (a duration) Add(other Value) Value {
	switch o := other.(type) {
		case date:     return date{o.val.Add(a.val)}
		case duration: return duration{a.val + o.val}
		case null:     return o
	}
	return a
}
func (a duration) Sub(other Value) Value {
	switch o := other.(type) {
		case date:     return date{o.val.Add(- a.val)}
		case duration: return duration{a.val - o.val}
		case null:     return o
	}
	return a
}
func (a float) Add(other Value) Value   { if _,ok:=other.(float);!ok      {return other};return float(a + other.(float)) }
func (a integer) Add(other Value) Value { if _,ok:=other.(integer);!ok    {return other};return integer(a + other.(integer)) }
func (a date) Add(other Value) Value    { if _,ok:=other.(duration);!ok   {return other};return date{a.val.Add(other.(duration).val)} }
func (a text) Add(other Value) Value    { if _,ok:=other.(text);!ok       {return other};return text(a + other.(text)) }
func (a null) Add(other Value) Value    { return a }
func (a liker) Add(other Value) Value   { return a }

func (a float) Sub(other Value) Value   { if _,ok:=other.(float);!ok   {return other};return float(a - other.(float)) }
func (a integer) Sub(other Value) Value { if _,ok:=other.(integer);!ok {return other};return integer(a - other.(integer)) }
func (a date) Sub(other Value) Value    {
	switch o := other.(type) {
		case date:     return duration{a.val.Sub(o.val)}
		case duration: return date{a.val.Add(-o.val)}
		case null:     return o
	}
	return a
}
func (a text) Sub(other Value) Value    { return a }
func (a null) Sub(other Value) Value    { return a }
func (a liker) Sub(other Value) Value   { return a }

func (a float) Mult(other Value) Value  {
	switch o := other.(type) {
		case float:    return float(a * o)
		case integer:  return float(a * float(o))
		case duration: return duration{time.Duration(a) * o.val}
		case null:     return o
	}
	return a
}
func (a integer) Mult(other Value) Value{
	switch o := other.(type) {
		case integer:  return integer(a * o)
		case duration: return duration{time.Duration(a) * o.val}
		case null:     return o
	}
	return a
}
func (a date) Mult(other Value) Value     { return a }
func (a duration) Mult(other Value) Value {
	switch o := other.(type) {
		case integer: return duration{a.val * time.Duration(o)}
		case float:   return duration{a.val * time.Duration(o)}
		case null:    return o
	}
	return a
}
func (a text) Mult(other Value) Value     { return a }
func (a null) Mult(other Value) Value     { return a }
func (a liker) Mult(other Value) Value    { return a }

func (a float) Div(other Value) Value    {
	switch o := other.(type) {
		case float:   if o != 0 { return float(a / o)        } else { return null("") }
		case integer: if o != 0 { return float(a / float(o)) } else { return null("") }
		case null:     return o
	}
	return a
}
func (a integer) Div(other Value) Value  {
	switch o := other.(type) {
		case integer: if o != 0 { return integer(a / o)          } else { return null("") }
		case float:   if o != 0 { return integer(a / integer(o)) } else { return null("") }
		case null:     return o
	}
	return a
}
func (a date) Div(other Value) Value     { return a }
func (a duration) Div(other Value) Value {
	switch o := other.(type) {
		case integer: if o != 0 { return duration{a.val / time.Duration(o)} } else { return null("") }
		case float:   if o != 0 { return duration{a.val / time.Duration(o)} } else { return null("") }
		case null:     return o
	}
	return a
}
func (a text) Div(other Value) Value     { return a }
func (a null) Div(other Value) Value     { return a }
func (a liker) Div(other Value) Value    { return a }

func (a float) Mod(other Value) Value    { return a }
func (a integer) Mod(other Value) Value  { return integer(a % other.(integer)) }
func (a date) Mod(other Value) Value     { return a }
func (a duration) Mod(other Value) Value { return a }
func (a text) Mod(other Value) Value     { return a }
func (a null) Mod(other Value) Value     { return a }
func (a liker) Mod(other Value) Value    { return a }

func (a float) Pow(other Value) Value    { if _,ok:=other.(float);!ok   {return other}; return float(math.Pow(float64(a), float64(other.(float)))) }
func (a integer) Pow(other Value) Value  { if _,ok:=other.(integer);!ok {return other}; return integer(math.Pow(float64(a), float64(other.(integer)))) }
func (a date) Pow(other Value) Value     { return a }
func (a duration) Pow(other Value) Value { return a }
func (a text) Pow(other Value) Value     { return a }
func (a null) Pow(other Value) Value     { return a }
func (a liker) Pow(other Value) Value    { return a }

func (a float) String() string    { return Sprintf("%.10g",a) }
func (a integer) String() string  { return Sprintf("%d",a) }
func (a date) String() string     { return a.val.Format("2006-01-02 15:04:05") }
func (a duration) String() string { return a.val.String() }
func (a text) String() string     { return string(a) }
func (a null) String() string     { return string(a) }
func (a liker) String() string    { return Sprint(a.val) }

func (a float) MarshalJSON() ([]byte,error)    { return json.Marshal(a.String()) }
func (a integer) MarshalJSON() ([]byte,error)  { return json.Marshal(a.String())}
func (a date) MarshalJSON() ([]byte,error)     { return json.Marshal(a.String()) }
func (a duration) MarshalJSON() ([]byte,error) { return json.Marshal(a.String()) }
func (a text) MarshalJSON() ([]byte,error)     { return json.Marshal(a.String()) }
func (a null) MarshalJSON() ([]byte,error)     { return json.Marshal(a.String()) }
func (a liker) MarshalJSON() ([]byte,error)    { return json.Marshal(a.String())}
