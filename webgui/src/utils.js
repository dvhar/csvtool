
var offline = true;
var testdata = require('./schema.json');

export function getData(){
    if (!offline)
        fetch('/query')
            .then(function(response) {
                return response.json();
            })
            .then(function(myJson) {
                return myJson;
            });
    else {
        return testdata
    }
}
export function colIndex(queryResults,column){
    for (var i in queryResults.Colnames)
        if (queryResults.Colnames[i] === column)
            return i;
    return -1;
}
export function getUnique(queryResults,column){
    var uniqueList = [];
    var idx = colIndex(queryResults,column);
    if (idx > -1)
        for (var i in queryResults.Vals)
            if (uniqueList.indexOf(queryResults.Vals[i][idx]) < 0)
                uniqueList.push(queryResults.Vals[i][idx]);
    return uniqueList;
}
export function getWhere(queryResults,column,value){
    var subset = JSON.parse(JSON.stringify(queryResults));
    var idx = colIndex(queryResults,column);
    if (idx > -1){
        var ri = queryResults.Vals.length - 1;
        for (var i in queryResults.Vals){
            if (queryResults.Vals[ri-i][idx] !== value){
                subset.Vals.splice(ri-i,1);
                subset.Numrows--;
            }
        }
        return subset;
    } else return null;
}
