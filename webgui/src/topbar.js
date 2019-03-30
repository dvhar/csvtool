import React from 'react';
import {postRequest,bit} from './utils.js';

export class TopMenuBar extends React.Component {
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
                sendSocket = {this.props.sendSocket}
            />
            <LoginForm
                changeTopDrop = {this.props.changeTopDrop}
                mode = {this.props.s.mode}
            />
            <Saver
                changeTopDrop = {this.props.changeTopDrop}
            />
            <BrowseButton
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

            browseShow : ( <Browser
                                path = {this.props.openpath}
                                dirlist = {this.props.dirlist}
                                send = {this.props.sendSocket}
                            /> ),

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
//file browser
class Browser extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            defaultPath : this.props.path || "/home/dave/",
        }
        this.props.send({Type : bit.SK_FILECLICK, Text : this.state.defaultPath});
    }
    render(){
        return (
        <div id="browseShow" className="fileSelectShow fileBrowser dropContent">
            <p className="dropContent">Browse files</p> 
            <div className="browseDirs dropContent">
            <span className="dropContent" onClick={()=>{
                this.props.send({Type : bit.SK_FILECLICK, Text : this.state.defaultPath});
            }}>{this.state.defaultPath}</span>
            </div>
        </div>
        )
    }
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

class BrowseButton extends React.Component {
    toggleForm(){ this.props.changeTopDrop("browseShow");}
    render(){
        return( <button className="topButton dropContent" id="saveButton" onClick={()=>this.toggleForm()}>Browse Files</button>)
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
