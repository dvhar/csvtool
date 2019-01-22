import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import * as serviceWorker from './serviceWorker';

/*
function loadjson(where) {
  var meshRequest = new XMLHttpRequest();
  var jsondata;
  meshRequest.open("GET", `/${where}/`, true);
  meshRequest.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
  meshRequest.onreadystatechange = function() {
    if (meshRequest.readyState == 4 && meshRequest.status == 200){
      jsondata = meshRequest.responseText;
      document.write(jsondata);
      console.log(jsondata);
    }
  }
  meshRequest.send(null);
}
*/


class Main extends React.Component {
  render(){
    return (
    <h1>hello world</h1>
    )
  }
}

ReactDOM.render(<Main />, document.getElementById('root'));

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
