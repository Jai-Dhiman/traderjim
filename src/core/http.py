from __future__ import annotations
"""HTTP client wrapper for Cloudflare Workers Python.

Uses the JavaScript fetch API via Pyodide interop.
"""

import json
from js import fetch, Headers, Object
from pyodide.ffi import to_js


async def request(
    method: str,
    url: str,
    headers: dict | None = None,
    params: dict | None = None,
    json_data: dict | None = None,
    timeout: float = 30.0,
) -> dict:
    """Make an HTTP request using the Workers fetch API.

    Args:
        method: HTTP method (GET, POST, PUT, DELETE, etc.)
        url: Full URL to request
        headers: Optional headers dict
        params: Optional query parameters
        json_data: Optional JSON body
        timeout: Request timeout in seconds (not fully supported in Workers)

    Returns:
        Parsed JSON response as dict

    Raises:
        Exception: On HTTP errors or network failures
    """
    # Build URL with query params
    if params:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{query_string}"

    # Build headers
    js_headers = Headers.new()
    if headers:
        for key, value in headers.items():
            js_headers.append(key, value)

    # Build fetch options
    options = {
        "method": method,
        "headers": js_headers,
    }

    if json_data is not None:
        options["body"] = json.dumps(json_data)

    # Convert to JS object
    js_options = to_js(options, dict_converter=Object.fromEntries)

    # Make request
    response = await fetch(url, js_options)

    # Check for errors
    if not response.ok:
        text = await response.text()
        raise Exception(f"HTTP {response.status}: {text}")

    # Handle empty responses
    if response.status == 204:
        return {}

    # Parse JSON response
    text = await response.text()
    if not text:
        return {}

    return json.loads(text)


async def get(url: str, headers: dict | None = None, params: dict | None = None) -> dict:
    """Make a GET request."""
    return await request("GET", url, headers=headers, params=params)


async def post(url: str, headers: dict | None = None, json_data: dict | None = None) -> dict:
    """Make a POST request."""
    return await request("POST", url, headers=headers, json_data=json_data)


async def put(url: str, headers: dict | None = None, json_data: dict | None = None) -> dict:
    """Make a PUT request."""
    return await request("PUT", url, headers=headers, json_data=json_data)


async def delete(url: str, headers: dict | None = None) -> dict:
    """Make a DELETE request."""
    return await request("DELETE", url, headers=headers)
