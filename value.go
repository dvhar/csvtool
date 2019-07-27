package main
import (
	"encoding/json"
	"time"
	. "fmt"
	"regexp"
)
//interface to simplify operations with various datatypes
type Value interface {
	Greater(other Value) bool
	GreatEq(other Value) bool
	Less(other Value) bool
	LessEq(other Value) bool
	Equal(other Value) bool
	Add(other Value) Value
	Sub(other Value) Value
	Mult(other Value) Value
	Div(other Value) Value
	Mod(other Value) Value
	String() string
	MarshalJSON() ([]byte,error)
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
func (a AverageVal) Less(other Value) bool       { return false }
func (a AverageVal) LessEq(other Value) bool     { return false }
func (a AverageVal) Equal(other Value) bool      { return false }
func (a AverageVal) Sub(other Value) Value       { return a }
func (a AverageVal) Mult(other Value) Value      { return a }
func (a AverageVal) Div(other Value) Value       { return a }
func (a AverageVal) Mod(other Value) Value       { return a }
func (a AverageVal) Eval() Value                 { return a.val.Div(a.count) }

type float float64
type integer int
type date struct {val time.Time}
type duration struct {val time.Duration}
type text string
type null string
type liker struct {val *regexp.Regexp}

func (f float) Less(other Value) bool      {
	switch o := other.(type) {
		case float:   return f < o
		case integer: return f < float(o)
	}
	return false
}
func (i integer) Less(other Value) bool    { if _,ok:=other.(integer);!ok  {return false};return i < other.(integer) }
func (d date) Less(other Value) bool       { if _,ok:=other.(date);!ok     {return false};return d.val.Before(other.(date).val) }
func (d duration) Less(other Value) bool   {
	switch o := other.(type) {
		case duration: return d.val < o.val
		case integer:  return d.val < time.Duration(o) //for abs()
	}
	return false
}
func (t text) Less(other Value) bool       { if _,ok:=other.(text);!ok     {return false};return t < other.(text) }
func (n null) Less(other Value) bool       { if _,ok:=other.(null);ok      {return false};return true }
func (l liker) Less(other Value) bool      { return false }

func (f float) LessEq(other Value) bool    { if _,ok:=other.(float);!ok    {return false};return f <= other.(float) }
func (i integer) LessEq(other Value) bool  { if _,ok:=other.(integer);!ok  {return false};return i <= other.(integer) }
func (d date) LessEq(other Value) bool     { if _,ok:=other.(duration);!ok {return false};return !d.val.After(other.(date).val) }
func (d duration) LessEq(other Value) bool { if _,ok:=other.(date);!ok     {return false};return d.val <= other.(duration).val }
func (t text) LessEq(other Value) bool     { if _,ok:=other.(text);!ok     {return false};return t <= other.(text) }
func (n null) LessEq(other Value) bool     { return false }
func (l liker) LessEq(other Value) bool    { return false }

func (f float) Greater(other Value) bool   { if _,ok := other.(float); !ok    { return true } else {return f > other.(float) } }
func (i integer) Greater(other Value) bool { if _,ok := other.(integer); !ok  { return true } else {return i > other.(integer) } }
func (d date) Greater(other Value) bool    { if _,ok := other.(date); !ok     { return true } else {return d.val.After(other.(date).val) } }
func (d duration) Greater(other Value) bool{ if _,ok := other.(duration); !ok { return true } else {return d.val > other.(duration).val } }
func (t text) Greater(other Value) bool    { if _,ok := other.(text); !ok     { return true } else {return t > other.(text) } }
func (n null) Greater(other Value) bool    { if o,ok := other.(null); ok      { return n > o } else {return false} }
func (l liker) Greater(other Value) bool   { return false }

func (f float) GreatEq(other Value) bool   { if _,ok:=other.(float);!ok    {return true};return f >= other.(float) }
func (i integer) GreatEq(other Value) bool { if _,ok:=other.(integer);!ok  {return true};return i >= other.(integer) }
func (d date) GreatEq(other Value) bool    { if _,ok:=other.(date);!ok     {return true};return !d.val.Before(other.(date).val) }
func (d duration) GreatEq(other Value) bool{ if _,ok:=other.(duration);!ok {return true};return d.val > other.(duration).val }
func (t text) GreatEq(other Value) bool    { if _,ok:=other.(text);!ok     {return true};return t >= other.(text) }
func (n null) GreatEq(other Value) bool    { return false }
func (l liker) GreatEq(other Value) bool   { return false }

func (f float) Equal(other Value) bool     { if _,ok:=other.(float);!ok     {return false};return f == other.(float) }
func (i integer) Equal(other Value) bool   { if _,ok:=other.(integer);!ok   {return false};return i == other.(integer) }
func (d date) Equal(other Value) bool      { if _,ok:=other.(date);!ok      {return false};return d.val.Equal(other.(date).val) }
func (d duration) Equal(other Value) bool  { if _,ok:=other.(duration);!ok  {return false};return d.val == other.(duration).val }
func (t text) Equal(other Value) bool      { if _,ok:=other.(text);!ok      {return false};return t == other.(text) }
func (n null) Equal(other Value) bool      { if _,ok := other.(null);ok     {return true };return false }
func (l liker) Equal(other Value) bool     { return l.val.MatchString(Sprint(other)) }

func (d duration) Add(other Value) Value {
	switch o := other.(type) {
		case date:     return date{o.val.Add(d.val)}
		case duration: return duration{d.val + o.val}
		case null:     return o
	}
	return d
}
func (d duration) Sub(other Value) Value {
	switch o := other.(type) {
		case date:     return date{o.val.Add(- d.val)}
		case duration: return duration{d.val - o.val}
		case null:     return o
	}
	return d
}

func (f float) Add(other Value) Value   { if _,ok:=other.(float);!ok      {return other};return float(f + other.(float)) }
func (i integer) Add(other Value) Value { if _,ok:=other.(integer);!ok    {return other};return integer(i + other.(integer)) }
func (d date) Add(other Value) Value    { if _,ok:=other.(duration);!ok   {return other};return date{d.val.Add(other.(duration).val)} }
func (t text) Add(other Value) Value    { if _,ok:=other.(text);!ok       {return other};return text(t + other.(text)) }
func (n null) Add(other Value) Value    { return n }
func (l liker) Add(other Value) Value   { return l }

func (f float) Sub(other Value) Value   { if _,ok:=other.(float);!ok   {return other};return float(f - other.(float)) }
func (i integer) Sub(other Value) Value { if _,ok:=other.(integer);!ok {return other};return integer(i - other.(integer)) }
func (d date) Sub(other Value) Value    {
	switch o := other.(type) {
		case date:     return duration{d.val.Sub(o.val)}
		case duration: return date{d.val.Add(-o.val)}
		case null:     return o
	}
	return d
}
func (t text) Sub(other Value) Value    { return t }
func (n null) Sub(other Value) Value    { return n }
func (l liker) Sub(other Value) Value   { return l }

func (f float) Mult(other Value) Value  {
	switch o := other.(type) {
		case float:    return float(f * o)
		case integer:  return float(f * float(o))
		case duration: return duration{time.Duration(f) * o.val}
		case null:     return o
	}
	return f
}
func (i integer) Mult(other Value) Value{
	switch o := other.(type) {
		case integer:  return integer(i * o)
		case duration: return duration{time.Duration(i) * o.val}
		case null:     return o
	}
	return i
}
func (d date) Mult(other Value) Value     { return d }
func (d duration) Mult(other Value) Value {
	switch o := other.(type) {
		case integer: return duration{d.val * time.Duration(o)}
		case float:   return duration{d.val * time.Duration(o)}
		case null:    return o
	}
	return d
}
func (t text) Mult(other Value) Value     { return t }
func (n null) Mult(other Value) Value     { return n }
func (l liker) Mult(other Value) Value    { return l }

func (f float) Div(other Value) Value    {
	switch o := other.(type) {
		case float:   if o != 0 { return float(f / o)        } else { return null("") }
		case integer: if o != 0 { return float(f / float(o)) } else { return null("") }
		case null:     return o
	}
	return f
}
func (i integer) Div(other Value) Value  {
	switch o := other.(type) {
		case integer: if o != 0 { return integer(i / o)          } else { return null("") }
		case float:   if o != 0 { return integer(i / integer(o)) } else { return null("") }
		case null:     return o
	}
	return i
}
func (d date) Div(other Value) Value     { return d }
func (d duration) Div(other Value) Value {
	switch o := other.(type) {
		case integer: if o != 0 { return duration{d.val / time.Duration(o)} } else { return null("") }
		case float:   if o != 0 { return duration{d.val / time.Duration(o)} } else { return null("") }
		case null:     return o
	}
	return d
}
func (t text) Div(other Value) Value     { return t }
func (n null) Div(other Value) Value     { return n }
func (l liker) Div(other Value) Value    { return l }

func (f float) Mod(other Value) Value    { return f }
func (i integer) Mod(other Value) Value  { return integer(i % other.(integer)) }
func (d date) Mod(other Value) Value     { return d }
func (d duration) Mod(other Value) Value { return d }
func (t text) Mod(other Value) Value     { return t }
func (n null) Mod(other Value) Value     { return n }
func (l liker) Mod(other Value) Value    { return l }

func (f float) String() string    { return Sprintf("%.10g",f) }
func (i integer) String() string  { return Sprintf("%d",i) }
func (d date) String() string     { return d.val.Format("2006-01-02 15:04:05") }
func (d duration) String() string { return d.val.String() }
func (t text) String() string     { return string(t) }
func (n null) String() string     { return string(n) }
func (l liker) String() string    { return Sprint(l.val) }

func (f float) MarshalJSON() ([]byte,error)    { return json.Marshal(f.String()) }
func (i integer) MarshalJSON() ([]byte,error)  { return json.Marshal(i.String())}
func (d date) MarshalJSON() ([]byte,error)     { return json.Marshal(d.String()) }
func (d duration) MarshalJSON() ([]byte,error) { return json.Marshal(d.String()) }
func (t text) MarshalJSON() ([]byte,error)     { return json.Marshal(t.String()) }
func (n null) MarshalJSON() ([]byte,error)     { return json.Marshal(n.String()) }
func (l liker) MarshalJSON() ([]byte,error)    { return json.Marshal(l.String())}
