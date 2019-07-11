import React from 'react';
import './App.css';
import {
  BrowserRouter,
  Link,
  Route,
  Switch
} from "react-router-dom";
import { Nav, Navbar, NavbarBrand, NavItem, NavLink } from "reactstrap";
import Collections from "./Collections";
import Collection from "./Collection";
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
              <NavLink tag={Link} to="/sessions">
                Collections
              </NavLink>
            </NavItem>
            <NavItem>
              <NavLink tag={Link} to="/status">
                Status
              </NavLink>
            </NavItem>
            <NavItem>
              <NavLink tag={Link} to="/submit">
                Submit
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
              <Route path="/collection/:name" component={(p: any) => <Collection err={err} {...p} />}/>
              <Route path="/collections" component={(p: any) => <Collections err={err} {...p} />} />
              <Route path="/" component={(p: any) => <Collections err={err} {...p} />} />
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
