import React from 'react';
import {postRequest,bit} from './utils.js';

export class TopMenuBar extends React.Component {
    render(){

        return (
            <div className="topBar">
            <TopDropdown
                updateTopMessage = {this.props.updateTopMessage}
                changeSavePath = {this.props.changeSavePath}
                changeTopDrop = {this.props.changeTopDrop}
                currentQuery = {this.props.s.queryHistory[this.props.s.historyPosition]}
                submitQuery = {this.props.submitQuery}
                section = {this.props.s.topDropdown}
                savepath = {this.props.s.savepath}
                sendSocket = {this.props.sendSocket}
                openDirlist = {this.props.openDirlist}
                saveDirlist = {this.props.saveDirlist}
                changeSavePath = {(path)=>this.props.changeFilePath({type:"save",path:path})}
                changeOpenPath = {(path)=>this.props.changeFilePath({type:"open",path:path})}
            />
            <SaveButton
                changeTopDrop = {this.props.changeTopDrop}
            />
            <BrowseButton
                changeTopDrop = {this.props.changeTopDrop}
            />
            <HelpButton
                showHelp = {this.props.showHelp}
                toggleHelp = {this.props.toggleHelp}
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
            saveShow : ( <Browser
                             dirlist = {this.props.saveDirlist}
                             send = {this.props.sendSocket}
                             changePath = {this.props.changeSavePath}
                             submitQuery = {this.props.submitQuery}
                             currentQuery = {this.props.currentQuery}
                             type = {"save"}
                          /> ),
            browseShow : ( <Browser
                               dirlist = {this.props.openDirlist}
                               send = {this.props.sendSocket}
                               changePath = {this.props.changeOpenPath}
                               type = {"open"}
                            /> ),

        }
        return dropdownContents[this.props.section];
    }
}

//attempt at copying file path to clipboard - not working yet
class FileSelector extends React.Component {
    constructor(props){
        super(props);
        this.state = { rowId : Math.random(), }
    }
    render() {
        return (
        <>
            <input type="hidden" value={this.props.path} id={this.state.rowId}/>
            <span className="dropContent browseFile browseEntry" onClick={()=>{
                var item = document.getElementById(this.state.rowId);    
                item.select();
                document.execCommand("copy");
            }} 
            >{this.props.path}</span>
        </>
        )
    }
}
//file browser
class Browser extends React.Component {
    constructor(props) {
        super(props);
        this.props.send({Type : bit.SK_FILECLICK, Text : this.props.dirlist.Path, Mode: this.props.type});
        this.handleChange = this.handleChange.bind(this);
        this.state = {
            innerBoxId : Math.random(),
            outterBoxId : Math.random(),
            currentDirId : Math.random(),
        };
    }
    clickPath(path){
        this.props.send({Type : bit.SK_FILECLICK, Text : path, Mode : this.props.type});
    }
    dirlist(){
        if (this.props.dirlist.Dirs) return (
        this.props.dirlist.Dirs.map(path => <span className="dropContent browseDir browseEntry" onClick={()=>this.clickPath(path)}>{path}</span>)
        );
    }
    filelist(){
        if (this.props.dirlist.Files) return (
        this.props.dirlist.Files.map(path => <FileSelector path={path} />)
        );
    }
    handleChange(e){
        this.props.changePath(e.target.value);
    }

    render(){
        var header = [];
        if (this.props.type === "open")
            header.push( <><span>To open a file copy the file path, paste into query box, and run a query.</span><br/></> );
        if (this.props.type === "save")
            header.push( <><span>Save queries on page to their own csv file. A number will be added to file name if more than 1.</span>
                <button className="saveButton" onClick={()=>{
                    var path = document.getElementById(this.state.currentDirId).value;
                    this.props.submitQuery({query : this.props.currentQuery.query, fileIO : bit.F_SAVE|bit.F_CSV, filePath : path});
                }}>save</button><br/></> );

        return (
        <div id={this.state.outterBoxId} className="fileSelectShow fileBrowser dropContent">
            {header}
            <input className="dropContent browseDir browseCurrent" id={this.state.currentDirId} value={this.props.dirlist.Path} onChange={this.handleChange}/>
            <div className="browseDirs dropContent" id={this.state.innerBoxId}>
            <span className="dropContent browseDir browseEntry" onClick={()=>{ this.clickPath(this.props.dirlist.Parent);
            }}><b>←</b></span>
            {this.dirlist()}
            {this.filelist()}
            </div>
        </div>
        )
    }
    componentDidUpdate(){
        const innerBox = document.getElementById(this.state.innerBoxId);
        const outterBox = document.getElementById(this.state.outterBoxId);
        var size = outterBox.offsetHeight;
        if (innerBox.offsetHeight > 480){
            innerBox.style.height = `${size-80}px`;
        }
    }
    componentDidMount(){
        var that = this;
        const dirText = document.getElementById(this.state.currentDirId);
        dirText.addEventListener("keyup", function(event) {
            if (event.key === "Enter") {
                that.clickPath(that.props.dirlist.Path)
            }
        });
    }
}
class SaveButton extends React.Component {
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

class HelpButton extends React.Component {
    render(){
        var label = "Help";
        if (this.props.showHelp) label = "Hide Help";
        return( <button className="topButton dropContent" id="saveButton" onClick={()=>this.props.toggleHelp()}>{label}</button>)
    }
}
