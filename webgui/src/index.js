import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {postRequest,bit} from './utils.js';
import * as command from './command.js';
import * as display from './display.js';
import * as help from './help.js';
import * as serviceWorker from './serviceWorker';


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
            <Helper
                showHelp = {this.props.showHelp}
                toggleHelp = {this.props.toggleHelp}
            />
            {/*
            <Opener
                changeTopDrop = {this.props.changeTopDrop}
            />
            <ModeSelect
                changeTopDrop = {this.props.changeTopDrop}
                mode = {this.props.s.mode}
            />
            */}
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
                    <p className="dropContent">{"This is for opening JSON files saved by this program. If you want to open a CSV, run a query on it."}</p> 
                    <input id="openPath" className="pathInput dropContent" type="text"/>
                    <button className="" onClick={()=>{
                        var path = document.getElementById("openPath").value;
                        this.props.submitQuery({fileIO : bit.F_OPEN, filePath : path});
                    }}>open</button>
                </div>
            ),

            saveShow : (
                <div id="saveShow" className="fileSelectShow dropContent">
                    <p className="dropContent">Save queries on page to their own csv file. A number will be added to file name if more than 1.</p> 
                    <label className="dropContent">Save file:</label> 
                    <input id="savePath" className="pathInput dropContent" type="text"/>
                    <button className="" onClick={()=>{
                        var path = document.getElementById("savePath").value;
                        this.props.changeSavePath(path);
                        this.props.submitQuery({query : this.props.currentQuery.query, fileIO : bit.F_SAVE|bit.F_CSV, filePath : path});
                    }}>save</button><br/>
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
                break;
        }
    }
    componentDidMount(){ this.defValue(); }
    componentDidUpdate(){ this.defValue(); }
}
/*
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
*/
class Saver extends React.Component {
    toggleForm(){ this.props.changeTopDrop("saveShow");}
    render(){
        return( <button className="topButton dropContent" id="saveButton" onClick={()=>this.toggleForm()}>Save</button>)
    }
}

class Helper extends React.Component {
    render(){
        var label = "Help";
        if (this.props.showHelp) label = "Hide Help";
        return( <button className="topButton dropContent" id="saveButton" onClick={()=>this.props.toggleHelp()}>{label}</button>)
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
            showHelp : 0,
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
                    tab => <display.QueryRender 
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
            toggleHelp = {()=>{this.setState({showHelp:this.state.showHelp^1})}}
            showHelp = {this.state.showHelp}
        />
        <help.Help
            show = {this.state.showHelp}
            toggleHelp = {()=>{this.setState({showHelp:this.state.showHelp^1})}}
        />
        <command.QuerySelect
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
            }
        }
        this.ws.onerror = function(e) { console.log("ERROR: " + e.data); } 
    }
    componentWillMount() { document.title = 'CSV Giant' }
}

ReactDOM.render(
    <Main 
        metaTables = {["column info abridged","table key info","informationschema.colums","column info with keys"]}
    />, 
    document.getElementById('root'));



// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();
