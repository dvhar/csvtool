

/*
// unused because chaching is done serverside
var sha1 = require("sha1");
export class dataFetcher {
    constructor(){
        this.cache = {};
    }
    makeQuery(request){
        //var data = postRequest({body:{Query:"select thing from place"}})
        console.log(request.body.Query);
        var hash = sha1(request.body.Query);
        if (this.cache[hash] === undefined){
            return postRequest(request).then(res=>{
                this.cache[hash] = JSON.parse(JSON.stringify({ query: request.body.Query, data: res }));
                return res;
            })
        }
        else {
            //this part should return a promise like the other part
            var data = this.cache[hash].data;
            console.log(data);
            return data;
        }
    }
    
}
*/

export function validJson(str) {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}


export function postRequest(request){
    var req = new Request(request.path, {
        method: 'POST',
        mode: 'cors',
        cache: "no-cache",
        //credentials: "same-origin", // include, *same-origin, omit
        redirect: 'follow',
        referrer: "no-referrer",
        headers: new Headers({ "Content-Type": "application/json", }),
        body: JSON.stringify(request.body),
    });
    return fetch(req)
    .then(res=>{if (res.status >= 400) return {Status: res.status}; else return res.json()})
    //.then(res =>res);
    //.then(res => {console.log(res); return res;});
    //.then(res=>{if (validJson(res)) return res.json(); return {err:"not valid json",res:res}})
}

export function colIndex(queryResults,column){
    for (var i in queryResults.Colnames)
        if (queryResults.Colnames[i].toUpperCase() === column.toUpperCase())
            return i;
    return -1;
}

export function sortQuery(queryResults,column){
    queryResults.Vals.sort(function(a,b){ return String(a[column]) > String(b[column]) ? 1 : -1; });
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
    if (value === "*") return queryResults;
    var subset = JSON.parse(JSON.stringify(queryResults));
    var idx = colIndex(queryResults,column);
    if (idx > -1){
        var ri = queryResults.Vals.length - 1;
        for (var i in queryResults.Vals){
            if (queryResults.Vals[ri-i][idx] == null ||
                String(queryResults.Vals[ri-i][idx]).toUpperCase() !== String(value).toUpperCase()){
                subset.Vals.splice(ri-i,1);
                subset.Numrows--;
            }
        }
        return subset;
    } else return null;
}

export function generateKeyDiagram(){
    console.log("ok");
}

export const bit = {
    DAT_BLANK      : 0,
    DAT_ERROR      : 1,
    DAT_GOOD       : 1 << 1,
    DAT_BADPATH    : 1 << 2,
    DAT_IOERR      : 1 << 3,

    CON_ERROR      : 1,
    CON_CHANGED    : 1 << 1,
    CON_UNCHANGED  : 1 << 2,
    CON_CHECKED    : 1 << 3,
    CON_BLANK      : 0,

    FP_CWD          : 0,
    FP_SERROR       : 1 << 1,
    FP_SCHANGED     : 1 << 2,
    FP_OERROR       : 1 << 3,
    FP_OCHANGED     : 1 << 4,

    F_CSV           : 1 << 5,
    F_JSON          : 1 << 6,
    F_OPEN          : 1 << 7,
    F_SAVE          : 1 << 8,
}
