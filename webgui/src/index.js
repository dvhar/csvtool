import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import * as serviceWorker from './serviceWorker';

var testdata = require('./schema.json');
var offline = true;

function colIndex(queryResults,column){
    for (var i in queryResults.Colnames)
        if (queryResults.Colnames[i] === column)
            return i;
    return -1;
}
function getUnique(queryResults,column){
    var uniqueList = [];
    var idx = colIndex(queryResults,column);
    if (idx > -1)
        for (var i in queryResults.Vals)
            if (uniqueList.indexOf(queryResults.Vals[i][idx]) < 0)
                uniqueList.push(queryResults.Vals[i][idx]);
    return uniqueList;
}
function getWhere(queryResults,column,value){
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

class Hmenu extends React.Component {
    //schema = getUnique(props.s.schema,"TABLE_NAME");
    render(){
        return (
            <div>menu goes here</div>
        )
    }
}

class Main extends React.Component {
    getData(){
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


    render(){
        var data = this.getData();
        //console.log(data);
        console.log(getUnique(data[0],"TABLE_NAME"));
        console.log(getWhere(data[0],"TABLE_NAME","Customer"));
        return (
            <>
            <h1>hello world</h1>
            <a onClick={()=>{this.getData()}}>try query</a>
            </>
        )
    }
}

ReactDOM.render(<Main />, document.getElementById('root'));

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
