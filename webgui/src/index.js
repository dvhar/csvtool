import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import './style.css';
import {postRequest,bit} from './utils.js';
import * as command from './command.js';
import * as display from './display.js';
import * as help from './help.js';
import * as topbar from './topbar.js';
import * as serviceWorker from './serviceWorker';


class Main extends React.Component {
    constructor(props) {
        super(props);

        this.state = {
            mode : "CSV",
            topMessage : "",
            topDropdown : "nothing",
            savepath : "",
            openpath : "",
            dirlist : [],
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
        <topbar.TopMenuBar
            s = {this.state}
            updateTopMessage = {(message)=>this.setState({ topMessage : message })}
            submitQuery = {(query)=>this.submitQuery(query)}
            viewHistory = {(position)=>this.viewHistory(position)}
            changeSavePath = {(path)=>this.setState({ savepath : path })}
            changeMode = {(mode)=>this.changeMode(mode)}
            changeTopDrop = {(section)=>this.setState({ topDropdown : section })}
            toggleHelp = {()=>{this.setState({showHelp:this.state.showHelp^1})}}
            showHelp = {this.state.showHelp}
            dirlist = {this.state.dirlist}
            sendSocket = {(request)=>this.sendSocket(request)}
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
                case bit.SK_DIRLIST:
                    console.log(dat);
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
