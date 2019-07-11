import React from 'react';
import ReactTable, { CellInfo } from 'react-table';
import { fetchFromServer, fetchJsonFromServer } from './service';
import { formatSize } from './utils';
import {
    Link
} from 'react-router-dom';
import { ErrorContainer } from './Error';

interface Props {
    match: any,
    err: ErrorContainer
}

interface EntrySummary {
    name: string,
    size: number,
    config: any,
}

interface State {
    data: EntrySummary[],
    loading: boolean,
}

class Collection extends React.Component<Props, State> {

    constructor(props : Props) {
        super(props);
        console.log(props.match);
        this.state = {data: [], loading: true}
    }

    componentDidMount() {

        fetchJsonFromServer("entries/" + this.name, null, "GET").then((data) => {
            console.log("DATA", data)
            let columns = new Set();
            for (let e of data) {
                for (let key in e) {
                    // check if the property/key is defined in the object itself, not in parent
                    if (dictionary.hasOwnProperty(key)) {
                        console.log(key, dictionary[key]);
                    }
                }
            }
            this.setState({
                data: data,
                loading: false
            });
        }).catch((error) => {
            console.log(error);
            this.props.err.setFetchError();
        });
    }

    get name() : string {
        return this.props.match.params.name;
    }

    _formatSize = (collection : EntrySummary) => formatSize(collection.size)
    /*_collectionCell = (cellInfo: CellInfo) => {
        const row: EntrySummary = cellInfo.row;
        return (<Link to={"/collection/" + row.name}>{row.name}</Link>);
    }*/

    render() {
        const columns = [{
            Header: "Collection name",
            accessor: "name",
        },
        {
            Header: "Entries",
            accessor: "count"
        },
        {
            id: "Size",
            Header: "Size",
            accessor: this._formatSize
        }
        ];

        return (
            <div>
            <h1>Collection '{this.name}'</h1>
            {/*<ReactTable data={this.state.data} loading={this.state.loading} columns={columns}/>*/}
            </div>
        );
    }
}

export default Collection;
