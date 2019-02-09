import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {postRequest,getUnique,getWhere,sortQuery} from './utils.js';
import * as premades from './premades.js';
import * as serviceWorker from './serviceWorker';

//var squel = require("squel");

function DropdownQueryTextbox(props){
    return(
        <div className={`dropmenu ${props.classes[0]}`}>
            <div className={`dropButton ${props.classes[1]}`}>
                {props.title}
            </div>
            <div className="dropmenu-content">
            <textarea rows="10" cols="70" id="textBoxId" placeholder={`If running multiple queries, separate them with a semicolon;`}>
            </textarea>
            <br/>
            <button onClick={()=>{
                var query = document.getElementById("textBoxId").value;
                props.submit(query);
            }}>Submit Query</button>
            </div>
        </div>
    )
}

function DropdownQueryMenu(props){
    return(
        <div className={`dropmenu ${props.classes[0]}`}>
            <div className={`dropButton ${props.classes[1]}`}>
                {props.title}
            </div>
            <div className="dropmenu-content">
            <select size={String(props.size)} className="dropSelect" id="premadeMultiSelect" multiple>
                {props.contents.map((v,i)=><option key={i} data-key={v.key} data-idx={i}>{v.label}</option>)}
            </select>
            <button onClick={()=>{
                    var queries = "";
                    var selected = document.getElementById("premadeMultiSelect").selectedOptions;
                    for (var i in selected)
                        if (i == Number(i))
                            queries += premades.metaDataQueries[selected[i].getAttribute("data-idx")].query;
                    props.submit(queries);
                }}
            >Submit</button>
            </div>
        </div>
    )
}

function DropdownGenericMenu(props){
    return(
        <div className={`dropmenu ${props.classes[0]}`}>
            <div className={`dropButton ${props.classes[1]}`}>
                {props.title}
            </div>
            <div className="dropmenu-content">
            <select size={String(props.size)} className="dropSelect">
                {props.contents}
            </select>
            </div>
        </div>
    )
}

//drop down list for what columns to hide
class TableSelectColumns extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            title: this.props.title,
        }
    }
    dropItem(choice,idx,order){
        if (choice !== null)
        return (
            <option className={`tableButton1${this.props.hideColumns[idx]?" hiddenColumn":""}`} key={idx} onClick={()=>this.props.toggleColumn(idx)}>
                {choice}
            </option>
        )
    }
    render(){
        return (
            <DropdownGenericMenu
                title = {this.props.title}
                size = {Math.min(20,this.props.table.Colnames.length)}
                contents = {this.props.table.Colnames.map((name,i)=>this.dropItem(name,i))}
                classes = {["tableModDiv","tableModButton"]}
            />
        )
    }
}

//drop down list for choosing section of table
//required props: title, table, firstDropItems, secondDropItems, dropAction
class TableSelectRows extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            title: this.props.title,
            secondDrop : false,
            firstChoice : "",
            secondDropItems : [],
        }
    }
    dropItem(choice,idx,order){
        if (choice !== null)
        return (
            <option className="tableButton1" key={idx} onClick={()=>{ 
                    switch (order){
                        case 'first':
                            this.setState({secondDrop:true,
                                           firstChoice: choice,
                                           secondDropItems: getUnique(this.props.table,choice) }); 
                            break;
                        case 'second':
                            this.props.dropAction(this.state.firstChoice,choice);
                            break;
                    }
                }}>
                {choice}
            </option>
        )
    }
    render(){
        var dropdowns = [
                <select className="dropSelect" size={Math.min(20,this.props.firstDropItems.length)}>
                    {this.props.firstDropItems.map((name,i)=>this.dropItem(name,i,'first'))}
                </select>
        ];
        if (this.state.secondDrop)
            dropdowns.push(
                <select className="dropSelect" size={Math.min(20,this.state.secondDropItems.length+1)}>
                    {["*"].concat(this.state.secondDropItems).map((name,i)=>this.dropItem(name,i,'second'))}
                </select>
            );
        return (
            <div className="dropmenu tableModDiv">
                <div className="dropButton tableModButton">
                {this.props.title}
                </div>
                <div className="dropmenu-content">
                {dropdowns}
                </div>
            </div>
        )
    }
}

//display html table of sql query
//required props: hideColumns, table
class TableGrid extends React.Component {
    constructor(props){
        super(props);
        this.state = {
            childId : Math.random(),
            parentId : Math.random()
        }
    }
    row(row,type,idx){
        return( 
            <tr key={idx} className="tableRow"> 
                {row.map((name,idx)=>{ 
                    if (this.props.hideColumns[idx]===0)
                        if (type === 'head')
                            return( <th key={idx} className="tableCell" onClick={()=>{sortQuery(this.props.table,idx);this.forceUpdate();}}> {name} </th>)
                        else
                            return( <td key={idx} className="tableCell"> {name} </td>) })}
            </tr>
        )
    }
    render(){
        if (this.props.table.Vals === null)
            this.props.table.Vals = [];
        return(
            <div className="tableDiv" id={this.state.parentId}> 
            <table className="table" id={this.state.childId}>
                <tbody>
                {this.row(this.props.table.Colnames,'head')}
                {this.props.table.Vals.map((row,i)=>{return this.row(row,'entry',i)})}
                </tbody>
            </table>
            </div>
        )
    }
    resize(){
        var inner = document.getElementById(this.state.childId);
        var outter = document.getElementById(this.state.parentId);
        var windoww = window.innerWidth;
        outter.style.maxWidth = `${Math.min(inner.offsetWidth+20,windoww*0.95)}px`;
    }
    componentDidUpdate(){ this.resize(); }
    componentDidMount(){ this.resize(); }
}

//query results section
class QueryRender extends React.Component {
    toggleColumn(column){
        this.props.hideColumns[column] ^= 1;
        this.forceUpdate();
    }
    render(){
        return ( 
        <div className="viewContainer">
            <div className="tableModifiers">
                <div> {this.props.table.Query} </div>
                <TableSelectRows 
                    title = {"Show with column value\u25bc"}
                    dropAction = {(column,value)=>{this.props.rows.col=column;this.props.rows.val=value;this.forceUpdate();}}
                    table = {this.props.table}
                    firstDropItems = {this.props.table.Colnames}
                />
                <TableSelectColumns
                    title = {"Show/Hide columns\u25bc"}
                    table = {this.props.table}
                    hideColumns = {this.props.hideColumns}
                    toggleColumn = {(i)=>this.toggleColumn(i)}
                />    
            </div>
            <TableGrid
                table = {getWhere(this.props.table,this.props.rows.col,this.props.rows.val)}
                hideColumns = {this.props.hideColumns}
                toggleColumn = {(i)=>this.toggleColumn(i)}
            />
        </div>
        )
    }
}

class QuerySelect extends React.Component {
    render(){
        var metaDataMenu = ( <div className="queryMenuContainer"> 
                         <DropdownQueryMenu
                            title = {<h2>View database schema query{"\u25bc"}</h2>}
                            size = {premades.metaDataQueries.length}
                            //make this run multi-select queries
                            contents = {premades.metaDataQueries}
                            submit = {(query, fileIO)=>this.props.submitQuery(query, fileIO)}
                            classes = {["queryMenuDiv","queryMenuButton"]}
                         />
                     </div>);

        var customQueryEntry = ( <div className="queryMenuContainer"> 
                         <DropdownQueryTextbox
                            title = {<h2>Enter Custom SQL Query{"\u25bc"}</h2>}
                            classes = {["queryMenuDiv","queryMenuButton"]}
                            submit = {(query, fileIO)=>this.props.submitQuery(query, fileIO)}
                         />
                     </div>);

        return (
            <div className="querySelect"> 
            {metaDataMenu} 
            {customQueryEntry} 
            {this.props.showQuery} 
            </div>
        );
    }
}

class TopMenuBar extends React.Component {
    render(){

        return (
            <div className="topBar">
            <LoginForm
                updateTopMessage = {this.props.updateTopMessage}
            />
            <Saver
                savepath = {this.props.s.savepath}
                changeSavePath = {this.props.changeSavePath}
                currentQuery = {this.props.s.queryHistory[this.props.s.historyPosition]}
                updateTopMessage = {this.props.updateTopMessage}
            />
            <Opener
                submitQuery = {this.props.submitQuery}
            />
            <div id="topMessage" className="topMessage">{this.props.s.topMessage}</div>
            <History
                position = {this.props.s.historyPosition} 
                last = {this.props.s.queryHistory.length - 1}
                viewHistory = {this.props.viewHistory}
            />
            </div>
        )
    }
}

class History extends React.Component {
    render(){
        return(
            <div className="historyArrows">
            {['◀ ',`${this.props.position}/${this.props.last}`,' ▶'].map((v,i)=> <span className={`${i===1?"":"arrow"}`} onClick={()=>{
                if (i === 0 && this.props.position > 1)
                    this.props.viewHistory(this.props.position - 1);
                if (i === 2 && this.props.position < this.props.last)
                    this.props.viewHistory(this.props.position + 1);
            }}>{v}</span>)}
            </div>
        )
    }
}

class Opener extends React.Component {
    toggleForm(){ document.getElementById("openShow").classList.toggle("show");
                  document.getElementById("LoginShow").classList.remove("show"); }
    render(){
        return(
            <>
            <button className="topButton dropContent" id="openButton" onClick={()=>this.toggleForm()}>Open</button>
            <div id="openShow" className="saveShow dropContent">
                <label className="dropContent">Save location:</label> 
                <input id="openPath" className="dropContent"/>
                <button onClick={()=>{
                    var path = document.getElementById("openPath").value;
                    this.props.submitQuery("",2,false,path);
                }}>open file</button>
            </div>
            </>
        )
    }
}

class Saver extends React.Component {
    toggleForm(){ document.getElementById("saveShow").classList.toggle("show");
                  document.getElementById("LoginShow").classList.remove("show"); }
    render(){
        return(
            <>
            <button className="topButton dropContent" id="saveButton" onClick={()=>this.toggleForm()}>Save</button>
            <div id="saveShow" className="saveShow dropContent">
                <label className="dropContent">Save location:</label> 
                <input id="savePath" className="dropContent"/>
                <button onClick={()=>{
                    var path = document.getElementById("savePath").value;
                    postRequest({path:"/query/",body:{Query:this.props.currentQuery, FileIO:1, FilePath:path}})
                    .then(dat=>{ this.props.updateTopMessage(dat.Message); });
                    this.props.changeSavePath(path);
                }}>save</button>
            </div>
            </>
        )
    }
    defValue(){ document.getElementById("savePath").value = this.props.savepath; }
    componentDidMount(){ this.defValue(); }
    componentDidUpdate(){ this.defValue(); }
}

class LoginForm extends React.Component {
    toggleForm(){ document.getElementById("LoginShow").classList.toggle("show");
                  document.getElementById("saveShow").classList.remove("show"); }
    render(){
        return (
            <>
            <button className="loginButton dropContent topButton" onClick={()=>this.toggleForm()}>
            Login
            </button>
            <div id="LoginShow" className="LoginShow  dropContent">
                <label className="dropContent">Database url:</label> 
                <input className="dropContent" id="dbUrl"/> <br/>
                <label className="dropContent">Database name:</label> 
                <input className="dropContent" id="dbName"/> <br/>
                <label className="dropContent">login name:</label> 
                <input className="dropContent" id="dbLogin"/> <br/>
                <label className="dropContent">login password:</label> 
                <input className="dropContent" id="dbPass" type="password"/> <br/>
                <div className="loginButtonDiv dropContent">
                    <button className="dropContent" onClick={()=>{
                        var dbUrl = document.getElementById("dbUrl").value;
                        var dbName = document.getElementById("dbName").value;
                        var dbLogin = document.getElementById("dbLogin").value;
                        var dbPass = document.getElementById("dbPass").value;
                        postRequest({path:"/login/",body:{Login: dbLogin, Pass:dbPass, Database: dbName, Server: dbUrl, Action: "login"}})
                        .then(dat=>{
                            var message;
                            switch (dat.Status){
                                case 4:  
                                    this.props.updateTopMessage(dat.Message);
                                    document.getElementById("LoginShow").classList.remove('show');
                                    break;
                                default: 
                                    this.props.updateTopMessage(dat.Message);
                                    document.getElementById("LoginMessage").innerHTML = "Nothing happened. Maybe bad credentials or connection.";
                                    break;
                            }
                        })
                    }}
                    >Submit</button><br/>
                </div>
                <span id="LoginMessage"></span>
            </div>
            </>
        )
    }
}

class Main extends React.Component {
    constructor(props) {
        super(props);

        this.state = {
            topMessage : "",
            savepath : "",
            queryHistory: ['',],
            historyPosition : 0,
            showQuery : <></>,
        }

        //get initial login state
        postRequest({path:"/login/",body:{Action: "check"}})
        .then(dat=>{
            console.log(dat);
            this.setState({topMessage: dat.Status===16?  dat.Message : "No connection"})
        });
        //get initial file path
        postRequest({path:"/info/",body:{Info : "savepath"}})
        .then(dat=>{
            console.log(dat); 
            this.setState({ savepath : dat.Status&1===1 ? dat.Path + "/savedQueries.json" : ""});
        });

    }
    showLoadedQuery(results){
        if (results.Status & 1){
            if (results.Message === undefined || results.Message === "")
                alert("Could not make query. Bad connection?");
            else
                alert(results.Message);
        }
        else if (results.Status & 2){
            this.setState({
                showQuery : results.Entries.map(
                    tab => <QueryRender 
                               table = {tab} 
                               hideColumns = {new Int8Array(tab.Numcols)}
                               rows = {new Object({col:"",val:"*"})}
                           />) });
        }
    }
    submitQuery(query, fileIO=0, backtrack=false, openPath=""){
        postRequest({path:"/query/",body:{Query:query, FileIO:fileIO, FilePath:openPath}}).then(dat=>{
            if ((dat.Status & 2) && (!backtrack)){
                this.setState({ topMessage : dat.Message,
                                historyPosition : this.state.queryHistory.length,
                                queryHistory : this.state.queryHistory.concat(dat.OriginalQuery),   });
            }
            this.showLoadedQuery(dat);
        });
    }
    viewHistory(position){
        this.setState({ historyPosition : position });
        this.submitQuery(this.state.queryHistory[position], 0, true);
    }

    render(){
        return (
        <>
        <TopMenuBar
            s = {this.state}
            updateTopMessage = {(message)=>this.setState({topMessage:message})}
            submitQuery = {(query, fileIO, backtrack, openpath)=>this.submitQuery(query, fileIO, backtrack, openpath)}
            viewHistory = {(position)=>this.viewHistory(position)}
            changeSavePath = {(path)=>this.setState({ savepath : path })}
        />
        <QuerySelect
            showLoadedQuery = {(results)=>this.showLoadedQuery(results)}
            submitQuery = {(query, fileIO)=>this.submitQuery(query, fileIO, false)}
            showQuery = {this.state.showQuery}
            metaTables = {this.props.metaTables}
        />
        </>
        )
    }
}

function startRender(){
    ReactDOM.render(
        <Main 
            metaTables = {["column info abridged","table key info","informationschema.colums","column info with keys"]}
        />, 
        document.getElementById('root'));
}

//dropdown closing
window.onclick = function(event) {
  if (!event.target.matches('.dropContent')) {
    document.getElementById("LoginMessage").innerHTML = ""; 
    var dropdowns = document.getElementsByClassName("dropContent");
    for (var i = 0; i < dropdowns.length; i++) {
      var openDropdown = dropdowns[i];
      if (openDropdown.classList.contains('show')) {
        openDropdown.classList.remove('show');
      }
    }
  }
}

startRender();



// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
