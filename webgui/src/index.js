import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {postRequest,getUnique,getWhere,sortQuery,bit} from './utils.js';
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

        var selectors = [];

        if (this.props.s.mode === "SQL")
            selectors.push(metaDataMenu,customQueryEntry);
        else
            selectors.push(<button>awwwww yeeee</button>);

        return (
            <div className="querySelect"> 
            {selectors} 
            {this.props.showQuery} 
            </div>
        );
    }
}

class TopMenuBar extends React.Component {
    render(){

        return (
            <div className="topBar">
            <TopDropdown
                updateTopMessage = {this.props.updateTopMessage}
                changeMode = {this.props.changeMode}
                changeSavePath = {this.props.changeSavePath}
                changeTopDrop = {this.props.changeTopDrop}
                currentQuery = {this.props.s.queryHistory[this.props.s.historyPosition]}
                submitQuery = {this.props.submitQuery}
                mode = {this.props.s.mode}
                section = {this.props.s.topDropdown}
                openpath = {this.props.s.openpath}
                savepath = {this.props.s.savepath}
            />
            <LoginForm
                changeTopDrop = {this.props.changeTopDrop}
                mode = {this.props.s.mode}
            />
            <Saver
                changeTopDrop = {this.props.changeTopDrop}
            />
            <Opener
                changeTopDrop = {this.props.changeTopDrop}
            />
            <ModeSelect
                changeTopDrop = {this.props.changeTopDrop}
                mode = {this.props.s.mode}
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

class TopDropdown extends React.Component {

    render(){
        var dropdownContents = {
            nothing : <></>,
            modeShow : (
                <div id="modeShow" className="modeShow dropContent">
                <button className="modeButton dropContent" onClick={()=>this.props.changeMode("SQL")}>SQL</button>
                <button className="modeButton dropContent" onClick={()=>this.props.changeMode("CSV")}>CSV</button>
                </div>
            ),

            openShow : (
                <div id="openShow" className="fileSelectShow dropContent">
                    <label className="dropContent">Open file:</label> 
                    <input id="openPath" className="dropContent"/>
                    <button className="dropContent" onClick={()=>{
                        var path = document.getElementById("openPath").value;
                        this.props.submitQuery("", 2, false, path);
                    }}>open</button>
                </div>
            ),

            saveShow : (
                <div id="saveShow" className="fileSelectShow dropContent">
                    <label className="dropContent">Save file:</label> 
                    <input id="savePath" className="dropContent"/>
                    <button className="dropContent" onClick={()=>{
                        console.log('trying to save');
                        var path = document.getElementById("savePath").value;
                        this.props.submitQuery(this.props.currentQuery, 1, false, path);
                    }}>save</button>
                </div>
            ),

            loginShow : (
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
            ),
        }
        return dropdownContents[this.props.section];
    }

    defValue(){ 
        switch(this.props.section){
            case "openShow":
                document.getElementById("openPath").value = this.props.openpath; 
                break;
            case "saveShow":
                document.getElementById("savePath").value = this.props.savepath;
                break
            case "loginShow":
                document.getElementById("dbUrl").value = "";
                document.getElementById("dbName").value = "";
            default: break;
        }
    }
    componentDidMount(){ this.defValue(); }
    componentDidUpdate(){ this.defValue(); }
}
class ModeSelect extends React.Component {
    toggleForm(){ this.props.changeTopDrop("modeShow");}
    render(){
        return(
            <button className="topButton dropContent" id="modeButton" onClick={()=>this.toggleForm()}>Mode: SQL</button>
        )
    }
}

class Opener extends React.Component {
    toggleForm(){ this.props.changeTopDrop("openShow");}
    render(){
        return(
            <button className="topButton dropContent" id="openButton" onClick={()=>this.toggleForm()}>Open</button>
        )
    }
}

class Saver extends React.Component {
    toggleForm(){ this.props.changeTopDrop("saveShow");}
    render(){
        return( <button className="topButton dropContent" id="saveButton" onClick={()=>this.toggleForm()}>Save</button>)
    }
}

class LoginForm extends React.Component {
    toggleForm(){ this.props.changeTopDrop("loginShow");}
    render(){
        var disp = {};
        if (this.props.mode !== "SQL")
            disp = {display:"none"};
        return (
            <button className="loginButton dropContent topButton" style={disp} onClick={()=>this.toggleForm()}>
            Login
            </button>
        )
    }
}

class Main extends React.Component {
    constructor(props) {
        super(props);

        this.state = {
            mode : "SQL",
            topMessage : "",
            topDropdown : "nothing",
            savepath : "",
            openpath : "",
            queryHistory: ['',],
            historyPosition : 0,
            showQuery : <></>,
        }
        this.topDropReset = this.topDropReset.bind(this);

        //get initial login state
        postRequest({path:"/login/",body:{Action: "check"}})
        .then(dat=>{
            console.log(dat);
            this.setState({topMessage: dat.Status===bit.CON_CHECKED?  dat.Message : "No connection"})
        });
        //get initial file path
        postRequest({path:"/info/",body:{}})
        .then(dat=>{
            console.log(dat); 
            this.setState({ savepath : dat.Status & bit.FP_SERROR===1 ? "" : dat.SavePath,
                            openpath : dat.Status & bit.FP_OERROR===1 ? "" : dat.OpenPath });
        });

    }
    showLoadedQuery(results){
        if (results.Status & bit.DAT_ERROR){
            if (results.Message === undefined || results.Message === "")
                alert("Could not make query. Bad connection?");
            else
                alert(results.Message);
        }
        else if (results.Status & bit.DAT_GOOD){
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
            //console.log(dat);
            if ((dat.Status & bit.DAT_GOOD) && (!backtrack)){
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
    changeMode(mode){ 
        this.setState({ mode : mode, topMessage : `${mode} mode` }); 
    }
    topDropReset(e){ 
        if (!e.target.matches('.dropContent'))
            this.setState({ topDropdown : "nothing" }); 
    }

    render(){
    
        document.addEventListener('click',this.topDropReset);

        return (
        <>
        <TopMenuBar
            s = {this.state}
            updateTopMessage = {(message)=>this.setState({ topMessage : message })}
            submitQuery = {(query, fileIO, backtrack, openpath)=>this.submitQuery(query, fileIO, backtrack, openpath)}
            viewHistory = {(position)=>this.viewHistory(position)}
            changeSavePath = {(path)=>this.setState({ savepath : path })}
            changeMode = {(mode)=>this.changeMode(mode)}
            changeTopDrop = {(section)=>this.setState({ topDropdown : section })}
        />
        <QuerySelect
            s = {this.state}
            showLoadedQuery = {(results)=>this.showLoadedQuery(results)}
            submitQuery = {(query, fileIO)=>this.submitQuery(query, fileIO)}
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


startRender();



// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
