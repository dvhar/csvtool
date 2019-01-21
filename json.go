package main
import (
   . "fmt"
   "io/ioutil"
   "github.com/Jeffail/gabs"

)

func main() {

      read_json, _ := ioutil.ReadFile("schema.json")
      jparsed,_ := gabs.ParseJSON(read_json)

      o_list,_ := jparsed.Children()
      i_list,_ := o_list[0].Children()

      Println(i_list[100].StringIndent(""," "))

    var m =make(map[string]string)
    m["dog"] = "golden retriever"
    println(m["dog"])

    var jmap *gabs.Container
    jmap,_ = gabs.Consume(m)
    Printf("%T\n",jmap)
    Println(jmap.String())

    jsonObj,_ := gabs.ParseJSON([]byte("[]"))
    jsonObj.ArrayAppend(m)
    Println(jsonObj.String())
}
