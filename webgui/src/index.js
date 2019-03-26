import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {postRequest,getUnique,getWhere,sortQuery,bit,t} from './utils.js';
import * as premades from './premades.js';
import * as serviceWorker from './serviceWorker';
//import Websocket from 'react-websocket';
//import io from 'socket.io-client';
//const socket = io('/socket/');
//socket.on('connect', function(){console.log("connected to socket");});
//socket.on('event', function(data){console.log(data);});
//socket.on('disconnect', function(){console.log("disconnected from socket");});

class DropdownQueryTextbox extends React.Component {
    constructor(props){
        super(props);
        this.state = {clicked:this.props.open?1:0}
    }
    render(){
        var arrow = <span className={this.state.clicked==1?"dim":""}>{"\u25bc"}</span>
        return(
            <div className="dropmenu queryMenuDiv">
                <div className="dropButton queryMenuButton" onClick={()=>{this.setState({clicked:this.state.clicked^1})}}>
                    <h2>{this.props.title}{arrow}</h2>
                </div>
                <div className={`dropmenu-content ${this.state.clicked==1?"show":""}`}>
                <textarea rows="10" className="queryTextEntry" id="textBoxId" placeholder={`If running multiple queries, separate them with a semicolon;`}>
                </textarea>
                <br/>
                <button onClick={()=>{
                    var query = document.getElementById("textBoxId").value;
                    this.props.submit({query : query});
                }}>Submit Query</button>
                <button onClick={()=>{ this.props.send({Type : bit.SK_STOP}); }}>Cancel Query</button>
                </div>
            </div>
        );
    }
    componentDidMount(){ console.log("mounted dq box"); }
}

function DropdownQueryMenu(props){
    return(
        <div className="dropmenu queryMenuDiv">
            <div className="dropButton queryMenuButton">
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
                    props.submit({query : queries});
                }}
            >Submit</button>
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

    SelectColumnDropdown(title, size, contents){
        return(
            <div className="dropmenu tableModDiv">
                <div className="dropButton tableModButton">
                    {title}
                </div>
                <div className="dropmenu-content absolute-pos tableModDrop">
                <select size={String(size)} className="dropSelect">
                    {contents}
                </select>
                </div>
            </div>
        )
    }

    render(){
        return (
            <div className="dropmenu tableModDiv">
                <div className="dropButton tableModButton">
                    {this.props.title}
                </div>
                <div className="dropmenu-content absolute-pos tableModDrop">
                <select size={String(Math.min(20,this.props.table.Colnames.length))} className="dropSelect">
                    {this.props.table.Colnames.map((name,i)=>this.dropItem(name,i))}
                </select>
                </div>
            </div>
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
                    {["*"].concat(this.state.secondDropItems.sort()).map((name,i)=>this.dropItem(name,i,'second'))}
                </select>
            );
        return (
            <div className="dropmenu tableModDiv">
                <div className="dropButton tableModButton">
                {this.props.title}
                </div>
                <div className="dropmenu-content absolute-pos tableModDrop">
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
            parentId : Math.random(),
            headId : Math.random()
        }
    }
    header(){
        var names = this.props.table.Colnames.map((name,ii)=>{
            if (this.props.hideColumns[ii]===0) return (
            <th key={ii} className="tableCell" onClick={()=>{sortQuery(this.props.table,ii);this.forceUpdate();}}> 
                {this.props.table.Colnames[ii]}
            </th>
        )});
        var info = this.props.table.Types.map((name,ii)=>{
            if (this.props.hideColumns[ii]===0) return (
            <th key={ii} className="tableCell" onClick={()=>{sortQuery(this.props.table,ii);this.forceUpdate();}}> 
                {`${this.props.table.Pos[ii]}/${t[this.props.table.Types[ii]]}`}
            </th>
        )});
        return[<tr className="tableRow">{names}</tr>,<tr className="tableRow">{info}</tr>]
    }
    row(row,idx){
        return( 
            <tr key={idx} className="tableRow"> 
                {row.map((name,idx)=>{ 
                    if (this.props.hideColumns[idx]===0) return( <td key={idx} className="tableCell"> {name} </td>) })}
            </tr>
        )
    }
    render(){
        if (this.props.table.Vals === null)
            this.props.table.Vals = [];
        return(
            <div className="tableDiv" id={this.state.parentId}> 
            <table className="table" id={this.state.childId}>
                <thead className="tableHead" id={this.state.headId}>
                {this.header()}
                </thead>
                <tbody className="tableBody">
                {this.props.table.Vals.map((row,i)=>{return this.row(row,i)})}
                </tbody>
            </table>
            </div>
        )
    }
    resize(){
        var inner = document.getElementById(this.state.childId);
        var outter = document.getElementById(this.state.parentId);
        var head = document.getElementById(this.state.headId);
        var windoww = window.innerWidth;
        outter.style.maxWidth = `${Math.min(inner.offsetWidth+20,windoww*1.00)}px`;
        //outter.addEventListener('scroll',function(){head.style.left=`${inner.getBoundingClientRect().left}px`});
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
                <div className="dropmenu tableModDiv">
                    <div className="dropButton tableModButton">
                        <span>Rows: {this.props.table.Numrows}</span>
                    </div>
                </div>
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
        var sqlServerMetaDataMenu = ( <div className="queryMenuContainer"> 
                         <DropdownQueryMenu
                            title = {<h2>View database schema query{"\u25bc"}</h2>}
                            size = {premades.metaDataQueries.length}
                            //make this run multi-select queries
                            contents = {premades.metaDataQueries}
                            submit = {(query)=>this.props.submitQuery(query)}
                         />
                     </div>);

        var sqlServerCustomQueryEntry = ( <div className="queryMenuContainer"> 
                         <DropdownQueryTextbox
                            title = {<>Enter SQL Query</>}
                            submit = {(query)=>this.props.submitQuery(query)}
                            s = {this.props.s}
                            open = {false}
                         />
                     </div>);

        var csvCustomQueryEntry = ( <div className="queryMenuContainer"> 
                         <DropdownQueryTextbox
                            title = {<>Enter CSV Query</>}
                            submit = {this.props.submitQuery}
                            send = {this.props.sendSocket}
                            s = {this.props.s}
                            open = {true}
                         />
                     </div>);

        var selectors = [];

        if (this.props.s.mode === "MSSQL")
            selectors.push(sqlServerMetaDataMenu, sqlServerCustomQueryEntry);
        else
            selectors.push(csvCustomQueryEntry);

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
                <button className="modeButton " onClick={()=>this.props.changeMode("MSSQL")}>MSSQL</button>
                <button className="modeButton " onClick={()=>this.props.changeMode("CSV")}>CSV</button>
                </div>
            ),

            openShow : (
                <div id="openShow" className="fileSelectShow dropContent">
                    <p className="dropContent">This is for opening JSON files saved by this program. If you want to open a CSV, run a query on it.</p> 
                    <input id="openPath" className="pathInput" type="text" className="dropContent"/>
                    <button className="" onClick={()=>{
                        var path = document.getElementById("openPath").value;
                        this.props.submitQuery({fileIO : bit.F_OPEN, filePath : path});
                    }}>open</button>
                </div>
            ),

            //FileIO bits: CJOS: 32 64 128 256
            saveShow : (
                <div id="saveShow" className="fileSelectShow dropContent">
                    <label className="dropContent">Save file:</label> 
                    <input id="savePath" className="pathInput" type="text" className="dropContent"/>
                    <button className="" onClick={()=>{
                        var jradio = document.getElementById("jradio").checked;
                        var path = document.getElementById("savePath").value;
                        this.props.changeSavePath(path);
                        var filetype = jradio? bit.F_JSON : bit.F_CSV;
                        this.props.submitQuery({query : this.props.currentQuery.query, fileIO : bit.F_SAVE|filetype, filePath : path});
                    }}>save</button><br/>
                    <input className="dropContent saveRadio" name="ftype" type="radio" id="cradio" value="CSV"/>CSV - Save queries on page to their own csv file. A number will be added to file name if more than 1.<br/>
                    <input className="dropContent saveRadio" name="ftype" type="radio" id="jradio" value="JSON"/>JSON - Save queries on page to single json file.<br/>
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
                        <button className="" onClick={()=>{
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
            <button className="topButton dropContent" id="modeButton" onClick={()=>this.toggleForm()}>Mode: {this.props.mode}</button>
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
        if (this.props.mode !== "MSSQL")
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
            mode : "CSV",
            topMessage : "",
            topDropdown : "nothing",
            savepath : "",
            openpath : "",
            queryHistory: ['',],
            historyPosition : 0,
            showQuery : <></>,
        }
        this.topDropReset = this.topDropReset.bind(this);

        //get initial file path
        postRequest({path:"/info/",body:{}})
        .then(dat=>{
            this.setState({ savepath : dat.Status & bit.FP_SERROR===1 ? "" : dat.SavePath,
                            openpath : dat.Status & bit.FP_OERROR===1 ? "" : dat.OpenPath });
        });

    }
    showLoadedQuery(results){
        if (results.Status & bit.DAT_ERROR){
            if (results.Message === undefined || results.Message === ""){
                alert("Could not make query. Bad connection or syntax?");
                console.log(results);
            }else
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

    submitQuery(querySpecs){
        var fullQuery = {
            Query: querySpecs.query || "", 
            FileIO: querySpecs.fileIO || 0, 
            FilePath: querySpecs.filePath || "", 
            Mode: querySpecs.mode || this.state.mode
            };
        postRequest({path:"/query/",body:fullQuery}).then(dat=>{
            if ((dat.Status & bit.DAT_GOOD) && (!querySpecs.backtrack)){
                this.setState({ historyPosition : this.state.queryHistory.length,
                                queryHistory : this.state.queryHistory.concat({query : dat.OriginalQuery, mode: dat.Mode}) });
            }
            this.showLoadedQuery(dat);
        });
    }
    sendSocket(request){
        this.ws.send(JSON.stringify(request));
    }

    viewHistory(position){
        var q = this.state.queryHistory[position];
        this.setState({ historyPosition : position });
        this.submitQuery({ query : q.query, backtrack : true, mode: q.mode});
    }
    changeMode(mode){ this.setState({ mode : mode }); }
    topDropReset(e){ 
        setTimeout(()=>{
        if (!e.target.matches('.dropContent'))
            this.setState({ topDropdown : "nothing" }); 
        },50);
    }

    render(){
    
        document.addEventListener('click',this.topDropReset);

        return (
        <>
        <TopMenuBar
            s = {this.state}
            updateTopMessage = {(message)=>this.setState({ topMessage : message })}
            submitQuery = {(query)=>this.submitQuery(query)}
            viewHistory = {(position)=>this.viewHistory(position)}
            changeSavePath = {(path)=>this.setState({ savepath : path })}
            changeMode = {(mode)=>this.changeMode(mode)}
            changeTopDrop = {(section)=>this.setState({ topDropdown : section })}
        />
        <QuerySelect
            s = {this.state}
            showLoadedQuery = {(results)=>this.showLoadedQuery(results)}
            submitQuery = {(query)=>this.submitQuery(query)}
            sendSocket = {(request)=>this.sendSocket(request)}
            showQuery = {this.state.showQuery}
            metaTables = {this.props.metaTables}
        />
        </>
        )
    }

    componentDidMount(){
        //websocket
        var that = this;
        this.ws = new WebSocket("ws://" + document.location.host + "/socket/");
        console.log(this.ws);
        this.ws.onopen = function(e) { console.log("OPEN"); }
        this.ws.onclose = function(e) { console.log("CLOSE"); that.ws = null; } 
        this.ws.onmessage = function(e) { 
            var dat = JSON.parse(e.data);
            switch (dat.Type) {
                case bit.SK_MSG:
                    that.setState({ topMessage : dat.Text }); 
                    break;
                case bit.SK_PING:
                    that.ws.send(JSON.stringify({Type:2, Text:"pong"}));
                default:
                    break;
            }
        }
        this.ws.onerror = function(e) { console.log("ERROR: " + e.data); } 
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
