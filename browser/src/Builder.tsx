import React from 'react';
import ReactTable, {CellInfo, Column} from 'react-table';
import {fetchJsonFromServer} from './service';
import {FaHourglassEnd, FaCheck, FaTimes, FaTrashAlt, FaFolderMinus, FaFolder} from 'react-icons/fa';
import {formatSize, formatTime} from './utils';
import {ErrorContainer} from './Error';
import {JobDetail} from "./JobDetail";
import {Button, ButtonGroup} from "reactstrap";

interface Props {
    match: any,
    err: ErrorContainer
}

interface JobSummary {
    key: string,
    size: number,
    comp_time: number,
    config: any,
    state: string,
}

interface State {
    data: JobSummary[],
    columns: Column[],
    loading: boolean,
    state_filter: string | null,
}

function state_icon(name: string) {
    if (name === "f") {
        return <span style={{color: "green"}}><FaCheck/></span>
    } else if (name === "a") {
        return <span style={{color: "orange"}}><FaHourglassEnd/></span>
    } else if (name === "r") {
        return <span style={{color: "green"}}><FaHourglassEnd/></span>
    } else if (name === "e") {
        return <span style={{color: "red"}}><FaTimes/></span>
    } else if (name === "F") {
        return <span style={{color: "green"}}><FaFolder/></span>
    } else if (name === "d") {
        return <span style={{color: "gray"}}><FaTrashAlt/></span>
    } else if (name === "D") {
        return <span style={{color: "gray"}}><FaFolderMinus/></span>
    } else {
        return <span>?</span>
    }
}

class Builder extends React.Component<Props, State> {

    constructor(props : Props) {
        super(props);
        this.state = {data: [], columns: [], loading: true, state_filter: null}
    }

    _cellConfigItem = (cellInfo : CellInfo) => {
        let v;
        if (typeof cellInfo.value === "string") {
            v = cellInfo.value;
        } else {
            v = JSON.stringify(cellInfo.value);
        }
        return (<span>{v}</span>);
    };

    _cellStateRepr = (cellInfo: CellInfo) => {
        const v = cellInfo.value;
        return state_icon(v);
    };

    _formatSize = (job : JobSummary) => formatSize(job.size);
    _formatTime = (job: JobSummary) => formatTime(job.comp_time);

    componentDidMount() {
        if (this.props.err.isOk) {
            fetchJsonFromServer("jobs/" + this.name, null, "GET").then((data) => {
                let cfgColumns = new Set();
                let nonObjectConfig = false;
                for (let e of data) {
                    let config = e.config;
                    if (typeof config != "object") {
                        nonObjectConfig = true;
                    } else {
                        for (let key in config) {
                            if (config.hasOwnProperty(key)) {
                                cfgColumns.add(key);
                            }
                        }
                    }
                }

                let cfgColumnArray = Array.from(cfgColumns);

                const column_defs = (cfgColumnArray.map((e, i) =>
                ({
                    id: "config_" + i,
                    style: {"background": "#fffff0"},
                    headerStyle: {"background": "#ffff90"},
                    Header: e as string,
                    accessor: "config." + e,
                    Cell: this._cellConfigItem
                })));

                if (nonObjectConfig) {
                    column_defs.unshift({
                        id: "config",
                        style: {"background": "#fffff0"},
                        headerStyle: {"background": "#ffff90"},
                        Header: "Config",
                        accessor: "config",
                        Cell: this._cellConfigItem
                    });
                }

                const config_column : Column = {
                    id: "config",
                    Header: "Config",
                    columns: column_defs
                };

                const columns = [config_column,
                    {
                        "Header": "Metadata",
                        "columns": [ {
                            /*"style": {"background": "#f0fff0"},
                            headerStyle: {"background": "#90ff90"},*/
                            Header: "S",
                            accessor: "state",
                            Cell: this._cellStateRepr,
                            maxWidth: 45,
                        },
                        {
                            id: "size",
                            Header: "Size",
                            accessor: this._formatSize,
                            maxWidth: 100,
                        },
                        {
                            id: "comptime",
                            Header: "CompTime",
                            accessor: this._formatTime,
                            maxWidth: 100,
                        },
                        {
                            id: "timestamp",
                            Header: "Finished time",
                            accessor: "finished",
                            maxWidth: 200,
                        }
                    ]
                    },
                ];
                this.setState({
                    data: data,
                    columns: columns,
                    loading: false
                });
            }).catch((error) => {
                console.log(error);
                this.props.err.setFetchError();
            });
        }
    }

    get name() : string {
        return this.props.match.params.name;
    }

    renderSubcomponent = (row: {original: {config: string, id: number}}) => {
        return (
            <JobDetail job_id={row.original.id} config={row.original.config} err={this.props.err}/>
        );
    };

    setFilter = (name: string | null) => {
        this.setState({...this.state, state_filter: name});
    }

    render() {
        let data = this.state.data;
        let state_filter = this.state.state_filter;
        if (state_filter !== null) {
            data = data.filter(r => r.state === state_filter);
        }
        return (
            <div>
                <h1>Builder '{this.name}'</h1>
                <ButtonGroup>
                    <Button outline onClick={() => this.setFilter(null)} active={this.state.state_filter === null}>All states</Button>
                    <Button outline onClick={() => this.setFilter("f")} active={this.state.state_filter === "f"}>{state_icon('f')} Finished</Button>
                    <Button outline onClick={() => this.setFilter("e")} active={this.state.state_filter === "e"}>{state_icon('e')} Failed</Button>
                    <Button outline onClick={() => this.setFilter("r")} active={this.state.state_filter === "r"}>{state_icon('r')} Running</Button>
                    <Button outline onClick={() => this.setFilter("a")} active={this.state.state_filter === "a"}>{state_icon('a')} Announced</Button>
                    <Button outline onClick={() => this.setFilter("F")} active={this.state.state_filter === "F"}>{state_icon('F')} Archived</Button>
                    <Button outline onClick={() => this.setFilter("d")} active={this.state.state_filter === "d"}>{state_icon('d')} Freed</Button>
                    <Button outline onClick={() => this.setFilter("D")} active={this.state.state_filter === "D"}>{state_icon('D')} Freed archived</Button>
                </ButtonGroup>
                <ReactTable
                    data={data}
                    loading={this.state.loading}
                    columns={this.state.columns}
                    SubComponent={this.renderSubcomponent} />
            </div>
        );
    }
}

export default Builder;
