import os
import argparse
from typing import Literal
from .server import run

__all__ = ["run", "main"]


def main() -> None:
    """
    Main CLI entry point for the MCP server.
    Reads MCP_TRANSPORT environment variable and command-line arguments,
    then starts the server with the specified configuration.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Massive MCP Server")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the server to for HTTP/SSE transport (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("PORT", 8000)),
        help="Port to bind the server to for HTTP/SSE transport (default: 8000, or $PORT env variable)"
    )
    args = parser.parse_args()

    # Determine transport from environment variable
    mcp_transport_str = os.environ.get("MCP_TRANSPORT", "stdio")

    # These are currently the only supported transports
    supported_transports: dict[str, Literal["stdio", "sse", "streamable-http"]] = {
        "stdio": "stdio",
        "sse": "sse",
        "streamable-http": "streamable-http",
    }

    transport = supported_transports.get(mcp_transport_str, "stdio")

    # Check API key and print startup message
    massive_api_key = os.environ.get("MASSIVE_API_KEY", "")
    polygon_api_key = os.environ.get("POLYGON_API_KEY", "")

    if massive_api_key:
        print("Starting Massive MCP server with API key configured.")
    elif polygon_api_key:
        print("Warning: POLYGON_API_KEY is deprecated. Please migrate to MASSIVE_API_KEY.")
        print("Starting Massive MCP server with API key configured (using deprecated POLYGON_API_KEY).")
        # Set MASSIVE_API_KEY from POLYGON_API_KEY for backward compatibility
        os.environ["MASSIVE_API_KEY"] = polygon_api_key
    else:
        print("Warning: MASSIVE_API_KEY environment variable not set.")

    # Print server configuration for HTTP/SSE transports
    if transport in ["streamable-http", "sse"]:
        print(f"Server will listen on {args.host}:{args.port}")

    run(transport=transport, host=args.host, port=args.port)
