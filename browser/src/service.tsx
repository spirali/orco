

let SERVER_URL = "/";

if (window.location.href.startsWith("http://localhost:3000")) {
  console.log("DEV mode detected, connecting to http://localhost:8550/");
  SERVER_URL = "http://localhost:8550/";
}

export function fetchFromServer(
  link: string,
  body: string | null,
  method: string = "POST"
) {
  return fetch(SERVER_URL + link, {
    method,
    mode: "cors",
    cache: "no-cache",
    body
  });
}

export async function fetchJsonFromServer(
  link: string,
  body: any,
  method: string = "POST"
  ) {
  const response = await fetchFromServer(link, body ? JSON.stringify(body) : null, method);
  return response.json();
}
