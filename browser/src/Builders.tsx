import React from 'react';
import ReactTable, {CellInfo} from 'react-table';
import {fetchJsonFromServer} from './service';
import {formatSize} from './utils';
import {Link} from 'react-router-dom';
import {ErrorContainer} from './Error';

interface Props {
    err: ErrorContainer
}

interface BuilderSummary {
    name: string,
    count: number,
    size: number,
}

interface State {
    data: BuilderSummary[],
    loading: boolean,
}

class Builders extends React.Component<Props, State> {

    constructor(props : Props) {
        super(props);
        this.state = {data: [], loading: true}
    }

    componentDidMount() {
        if (this.props.err.isOk) {
            fetchJsonFromServer("builders", null, "GET").then((data) => {
                this.setState({
                    data: data,
                    loading: false
                });
            }).catch((error) => {
               console.log(error);
               this.props.err.setFetchError();
            });
        }
    }

    _formatSize = (builder : BuilderSummary) => formatSize(builder.size)
    _builderCell = (cellInfo: CellInfo) => {
        const row: BuilderSummary = cellInfo.row;
        return (<Link to={"/builder/" + row.name}>{row.name}</Link>);
    }

    render() {
        const columns = [{
            Header: "Builder name",
            accessor: "name",
            Cell: this._builderCell,
        },
        {
            Header: "Finished",
            accessor: "n_finished",
            maxWidth: 100,
        },
        {
            Header: "In progress",
            accessor: "n_in_progress",
            maxWidth: 100,
        },
        {
            Header: "Failed",
            accessor: "n_failed",
            maxWidth: 100,
        },
        {
            id: "Size",
            Header: "Total Size",
            accessor: this._formatSize,
            maxWidth: 200
        }
        ];

        return (
            <div>
            <h1>Builders</h1>
            <ReactTable data={this.state.data} loading={this.state.loading} columns={columns}/>
            </div>
        );
    }
}

export default Builders;
