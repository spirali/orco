import React from 'react';
import './App.css';
import {
  BrowserRouter,
  Link,
  Route,
  Switch
} from "react-router-dom";
import { Nav, Navbar, NavbarBrand, NavItem, NavLink } from "reactstrap";
import Builders from "./Builders";
import Builder from "./Builder";
import Executors from "./Executors";
import Status from "./Status";
import { ErrorDisplay, ErrorContainer } from "./Error";
import { Subscribe, Provider } from 'unstated';

class App extends React.Component {


  componentDidCatch(error: any, info: any) {
    console.log("ERROR: ", error);
  }

  render() {
  return (
    <div className="App">
    <BrowserRouter>
      <div>
        <Navbar color="light">
          <Nav>
            <NavItem>
              <NavLink tag={Link} to="/builders">
                Builders
              </NavLink>
            </NavItem>
            <NavItem>
              <NavLink tag={Link} to="/status">
                Status
              </NavLink>
            </NavItem>
          </Nav>
          <NavbarBrand>ORCO Browser</NavbarBrand>
        </Navbar>
        <Provider>
        <Subscribe to={[ErrorContainer]}>
        {(err : ErrorContainer) => (<div>

          <ErrorDisplay err={err}/>
          <div className="container">
            <Switch>
              <Route path="/executors" component={(p: any) => <Executors err={err} {...p} />}/>
              <Route path="/builder/:name" component={(p: any) => <Builder err={err} {...p} />}/>
              <Route path="/builders" component={(p: any) => <Builders err={err} {...p} />} />
              <Route path="/status" component={(p: any) => <Status err={err} {...p} />} />
              <Route path="/" component={(p: any) => <Builders err={err} {...p} />} />
            </Switch>
            </div>
            </div>)
        }</Subscribe>
        </Provider>
    </div>
    </BrowserRouter>
    </div>
);
  }
}

export default App;
