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
                changeOpenPath = {this.props.changeOpenPath}
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
                                openDirlist = {this.props.openDirlist}
                                send = {this.props.sendSocket}
                                changeOpenPath = {this.props.changeOpenPath}
                            /> ),

        }
        return dropdownContents[this.props.section];
    }

    defValue(){ 
        switch(this.props.section){
            case "saveShow":
                document.getElementById("savePath").value = this.props.savepath;
                break
        }
    }
    componentDidMount(){ this.defValue(); }
    componentDidUpdate(){ this.defValue(); }
}

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
        this.props.send({Type : bit.SK_FILECLICK, Text : this.props.openDirlist.Path});
        this.handleChange = this.handleChange.bind(this);
        this.state = {
            innerBoxId : Math.random(),
            outterBoxId : Math.random(),
            currentDirId : Math.random(),
        };
    }
    clickPath(path){
        this.props.send({Type : bit.SK_FILECLICK, Text : path});
    }
    dirlist(){
        if (this.props.openDirlist.Dirs) return (
        this.props.openDirlist.Dirs.map(path => <span className="dropContent browseDir browseEntry" onClick={()=>this.clickPath(path)}>{path}</span>)
        );
    }
    filelist(){
        if (this.props.openDirlist.Files) return (
        this.props.openDirlist.Files.map(path => <FileSelector path={path} />)
        );
    }
    handleChange(e){
        this.props.changeOpenPath(e.target.value);
    }

    render(){
        return (
        <div id={this.state.outterBoxId} className="fileSelectShow fileBrowser dropContent">
            <span>To open a file copy the file path, paste into query box, and run a query.</span><br/>
            <input className="dropContent browseDir browseCurrent" id={this.state.currentDirId} value={this.props.openDirlist.Path} onChange={this.handleChange}/>
            <div className="browseDirs dropContent" id={this.state.innerBoxId}>
            <span className="dropContent browseDir browseEntry" onClick={()=>{ this.clickPath(this.props.openDirlist.Parent);
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
                that.clickPath(that.props.openDirlist.Path)
            }
        });
    }
}
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
