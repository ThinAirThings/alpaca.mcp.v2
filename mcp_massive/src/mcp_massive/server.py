import os
from typing import Optional, Any, Dict, Union, List, Literal
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from polygon import RESTClient
from importlib.metadata import version, PackageNotFoundError
from .formatters import json_to_csv
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.requests import Request

from datetime import datetime, date

MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY", "")
if not MASSIVE_API_KEY:
    print("Warning: MASSIVE_API_KEY environment variable not set.")

MCP_AUTH_KEY = os.environ.get("MCP_AUTH_KEY")

version_number = "MCP-Massive/unknown"
try:
    version_number = f"MCP-Massive/{version('mcp_massive')}"
except PackageNotFoundError:
    pass

polygon_client = RESTClient(MASSIVE_API_KEY)
polygon_client.headers["User-Agent"] += f" {version_number}"

poly_mcp = FastMCP("Massive")


# Authentication Middleware for HTTP Transport
class AuthenticationMiddleware(BaseHTTPMiddleware):
    """
    Middleware to authenticate HTTP requests using an api_key query parameter.
    Only applied when using HTTP transport for remote MCP access.
    """
    async def dispatch(self, request: Request, call_next):
        # Skip authentication for health check endpoints if any
        if request.url.path in ["/health", "/healthz"]:
            response = await call_next(request)
            return response

        # Check for api_key in query parameters
        api_key = request.query_params.get("api_key")
        expected_key = MCP_AUTH_KEY

        # If MCP_AUTH_KEY is not set, skip authentication (for backward compatibility)
        if expected_key is None:
            response = await call_next(request)
            return response

        # Validate api_key
        if not api_key or api_key != expected_key:
            return JSONResponse(
                status_code=401,
                content={
                    "error": "Unauthorized",
                    "message": "Invalid or missing api_key parameter. Please provide ?api_key=YOUR_KEY in the URL."
                }
            )

        # Authentication successful, proceed with request
        response = await call_next(request)
        return response


# Add authentication middleware to the FastMCP app (only applies to HTTP transport)
# This middleware will check for api_key query parameter when MCP_AUTH_KEY is set
if hasattr(poly_mcp, 'app'):
    poly_mcp.app.add_middleware(AuthenticationMiddleware)


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_aggs(
    ticker: str,
    multiplier: int,
    timespan: str,
    from_: Union[str, int, datetime, date],
    to: Union[str, int, datetime, date],
    adjusted: Optional[bool] = None,
    sort: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List aggregate bars for a ticker over a given date range in custom time window sizes.
    """
    try:
        results = polygon_client.get_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_,
            to=to,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
            params=params,
            raw=True,
        )

        # Parse the binary data to string and then to JSON
        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_aggs(
    ticker: str,
    multiplier: int,
    timespan: str,
    from_: Union[str, int, datetime, date],
    to: Union[str, int, datetime, date],
    adjusted: Optional[bool] = None,
    sort: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Iterate through aggregate bars for a ticker over a given date range.
    """
    try:
        results = polygon_client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_,
            to=to,
            adjusted=adjusted,
            sort=sort,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_grouped_daily_aggs(
    date: str,
    adjusted: Optional[bool] = None,
    include_otc: Optional[bool] = None,
    locale: Optional[str] = None,
    market_type: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get grouped daily bars for entire market for a specific date.
    """
    try:
        results = polygon_client.get_grouped_daily_aggs(
            date=date,
            adjusted=adjusted,
            include_otc=include_otc,
            locale=locale,
            market_type=market_type,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_daily_open_close_agg(
    ticker: str,
    date: str,
    adjusted: Optional[bool] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get daily open, close, high, and low for a specific ticker and date.
    """
    try:
        results = polygon_client.get_daily_open_close_agg(
            ticker=ticker, date=date, adjusted=adjusted, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_previous_close_agg(
    ticker: str,
    adjusted: Optional[bool] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get previous day's open, close, high, and low for a specific ticker.
    """
    try:
        results = polygon_client.get_previous_close_agg(
            ticker=ticker, adjusted=adjusted, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_trades(
    ticker: str,
    timestamp: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get trades for a ticker symbol.
    """
    try:
        results = polygon_client.list_trades(
            ticker=ticker,
            timestamp=timestamp,
            timestamp_lt=timestamp_lt,
            timestamp_lte=timestamp_lte,
            timestamp_gt=timestamp_gt,
            timestamp_gte=timestamp_gte,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_last_trade(
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get the most recent trade for a ticker symbol.
    """
    try:
        results = polygon_client.get_last_trade(ticker=ticker, params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_last_crypto_trade(
    from_: str,
    to: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get the most recent trade for a crypto pair.
    """
    try:
        results = polygon_client.get_last_crypto_trade(
            from_=from_, to=to, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_quotes(
    ticker: str,
    timestamp: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_lte: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gt: Optional[Union[str, int, datetime, date]] = None,
    timestamp_gte: Optional[Union[str, int, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get quotes for a ticker symbol.
    """
    try:
        results = polygon_client.list_quotes(
            ticker=ticker,
            timestamp=timestamp,
            timestamp_lt=timestamp_lt,
            timestamp_lte=timestamp_lte,
            timestamp_gt=timestamp_gt,
            timestamp_gte=timestamp_gte,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_last_quote(
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get the most recent quote for a ticker symbol.
    """
    try:
        results = polygon_client.get_last_quote(ticker=ticker, params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_last_forex_quote(
    from_: str,
    to: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get the most recent forex quote.
    """
    try:
        results = polygon_client.get_last_forex_quote(
            from_=from_, to=to, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_real_time_currency_conversion(
    from_: str,
    to: str,
    amount: Optional[float] = None,
    precision: Optional[int] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get real-time currency conversion.
    """
    try:
        results = polygon_client.get_real_time_currency_conversion(
            from_=from_,
            to=to,
            amount=amount,
            precision=precision,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_universal_snapshots(
    type: str,
    ticker_any_of: Optional[List[str]] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get universal snapshots for multiple assets of a specific type.
    """
    try:
        results = polygon_client.list_universal_snapshots(
            type=type,
            ticker_any_of=ticker_any_of,
            order=order,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_all(
    market_type: str,
    tickers: Optional[List[str]] = None,
    include_otc: Optional[bool] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get a snapshot of all tickers in a market.
    """
    try:
        results = polygon_client.get_snapshot_all(
            market_type=market_type,
            tickers=tickers,
            include_otc=include_otc,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_direction(
    market_type: str,
    direction: str,
    include_otc: Optional[bool] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get gainers or losers for a market.
    """
    try:
        results = polygon_client.get_snapshot_direction(
            market_type=market_type,
            direction=direction,
            include_otc=include_otc,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_ticker(
    market_type: str,
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get snapshot for a specific ticker.
    """
    try:
        results = polygon_client.get_snapshot_ticker(
            market_type=market_type, ticker=ticker, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_option(
    underlying_asset: str,
    option_contract: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get real-time market data snapshot for a single option contract.

    Retrieve current market data including Greeks, implied volatility, pricing, and
    volume metrics for a specific option contract.

    **Use Case**: Get live market data for a specific known contract. Use this when you need
    real-time updates on a single contract, such as monitoring an existing position or
    checking current pricing before entering a trade.

    **Comparison with list_snapshot_options_chain**:
    - Use THIS endpoint when: You need data for ONE specific contract
    - Use list_snapshot_options_chain when: You need data for MULTIPLE contracts or entire chain

    **Typical Workflow**:
    1. Discover contracts using list_options_contracts
    2. Compare multiple contracts using list_snapshot_options_chain
    3. Monitor specific chosen contract using THIS endpoint
    4. Get contract specifications using get_options_contract if needed

    Args:
        underlying_asset: The underlying stock ticker (e.g., 'AAPL')
        option_contract: The option contract identifier without the O: prefix
                        Example: 'AAPL251121C00230000' for O:AAPL251121C00230000
        params: Additional query parameters

    Returns:
        CSV formatted snapshot with fields including:
        - Greeks (delta, gamma, theta, vega): Risk metrics
        - Implied volatility: Market's volatility expectation
        - Last quote (bid/ask/midpoint): Current pricing
        - Last trade (price/size): Most recent execution
        - Day data (OHLC, volume, vwap): Daily statistics
        - Open interest: Total outstanding contracts (liquidity)
        - Break-even price: Stock price needed for zero profit/loss
    """
    try:
        results = polygon_client.get_snapshot_option(
            underlying_asset=underlying_asset,
            option_contract=option_contract,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_snapshot_crypto_book(
    ticker: str,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get snapshot for a crypto ticker's order book.
    """
    try:
        results = polygon_client.get_snapshot_crypto_book(
            ticker=ticker, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_market_holidays(
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get upcoming market holidays and their open/close times.
    """
    try:
        results = polygon_client.get_market_holidays(params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_market_status(
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get current trading status of exchanges and financial markets.
    """
    try:
        results = polygon_client.get_market_status(params=params, raw=True)

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_tickers(
    ticker: Optional[str] = None,
    type: Optional[str] = None,
    market: Optional[str] = None,
    exchange: Optional[str] = None,
    cusip: Optional[str] = None,
    cik: Optional[str] = None,
    date: Optional[Union[str, datetime, date]] = None,
    search: Optional[str] = None,
    active: Optional[bool] = None,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Query supported ticker symbols across stocks, indices, forex, and crypto.
    """
    try:
        results = polygon_client.list_tickers(
            ticker=ticker,
            type=type,
            market=market,
            exchange=exchange,
            cusip=cusip,
            cik=cik,
            date=date,
            search=search,
            active=active,
            sort=sort,
            order=order,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ticker_details(
    ticker: str,
    date: Optional[Union[str, datetime, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get detailed information about a specific ticker.
    """
    try:
        results = polygon_client.get_ticker_details(
            ticker=ticker, date=date, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_ticker_news(
    ticker: Optional[str] = None,
    published_utc: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get recent news articles for a stock ticker.
    """
    try:
        results = polygon_client.list_ticker_news(
            ticker=ticker,
            published_utc=published_utc,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_ticker_types(
    asset_class: Optional[str] = None,
    locale: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List all ticker types supported by Massive.com.
    """
    try:
        results = polygon_client.get_ticker_types(
            asset_class=asset_class, locale=locale, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_splits(
    ticker: Optional[str] = None,
    execution_date: Optional[Union[str, datetime, date]] = None,
    reverse_split: Optional[bool] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get historical stock splits.
    """
    try:
        results = polygon_client.list_splits(
            ticker=ticker,
            execution_date=execution_date,
            reverse_split=reverse_split,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_dividends(
    ticker: Optional[str] = None,
    ex_dividend_date: Optional[Union[str, datetime, date]] = None,
    frequency: Optional[int] = None,
    dividend_type: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get historical cash dividends.
    """
    try:
        results = polygon_client.list_dividends(
            ticker=ticker,
            ex_dividend_date=ex_dividend_date,
            frequency=frequency,
            dividend_type=dividend_type,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_conditions(
    asset_class: Optional[str] = None,
    data_type: Optional[str] = None,
    id: Optional[int] = None,
    sip: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List conditions used by Massive.com.
    """
    try:
        results = polygon_client.list_conditions(
            asset_class=asset_class,
            data_type=data_type,
            id=id,
            sip=sip,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_exchanges(
    asset_class: Optional[str] = None,
    locale: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List exchanges known by Massive.com.
    """
    try:
        results = polygon_client.get_exchanges(
            asset_class=asset_class, locale=locale, params=params, raw=True
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_stock_financials(
    ticker: Optional[str] = None,
    cik: Optional[str] = None,
    company_name: Optional[str] = None,
    company_name_search: Optional[str] = None,
    sic: Optional[str] = None,
    filing_date: Optional[Union[str, datetime, date]] = None,
    filing_date_lt: Optional[Union[str, datetime, date]] = None,
    filing_date_lte: Optional[Union[str, datetime, date]] = None,
    filing_date_gt: Optional[Union[str, datetime, date]] = None,
    filing_date_gte: Optional[Union[str, datetime, date]] = None,
    period_of_report_date: Optional[Union[str, datetime, date]] = None,
    period_of_report_date_lt: Optional[Union[str, datetime, date]] = None,
    period_of_report_date_lte: Optional[Union[str, datetime, date]] = None,
    period_of_report_date_gt: Optional[Union[str, datetime, date]] = None,
    period_of_report_date_gte: Optional[Union[str, datetime, date]] = None,
    timeframe: Optional[str] = None,
    include_sources: Optional[bool] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get fundamental financial data for companies.
    """
    try:
        results = polygon_client.vx.list_stock_financials(
            ticker=ticker,
            cik=cik,
            company_name=company_name,
            company_name_search=company_name_search,
            sic=sic,
            filing_date=filing_date,
            filing_date_lt=filing_date_lt,
            filing_date_lte=filing_date_lte,
            filing_date_gt=filing_date_gt,
            filing_date_gte=filing_date_gte,
            period_of_report_date=period_of_report_date,
            period_of_report_date_lt=period_of_report_date_lt,
            period_of_report_date_lte=period_of_report_date_lte,
            period_of_report_date_gt=period_of_report_date_gt,
            period_of_report_date_gte=period_of_report_date_gte,
            timeframe=timeframe,
            include_sources=include_sources,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_ipos(
    ticker: Optional[str] = None,
    listing_date: Optional[Union[str, datetime, date]] = None,
    listing_date_lt: Optional[Union[str, datetime, date]] = None,
    listing_date_lte: Optional[Union[str, datetime, date]] = None,
    listing_date_gt: Optional[Union[str, datetime, date]] = None,
    listing_date_gte: Optional[Union[str, datetime, date]] = None,
    ipo_status: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve upcoming or historical IPOs.
    """
    try:
        results = polygon_client.vx.list_ipos(
            ticker=ticker,
            listing_date=listing_date,
            listing_date_lt=listing_date_lt,
            listing_date_lte=listing_date_lte,
            listing_date_gt=listing_date_gt,
            listing_date_gte=listing_date_gte,
            ipo_status=ipo_status,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_short_interest(
    ticker: Optional[str] = None,
    settlement_date: Optional[Union[str, datetime, date]] = None,
    settlement_date_lt: Optional[Union[str, datetime, date]] = None,
    settlement_date_lte: Optional[Union[str, datetime, date]] = None,
    settlement_date_gt: Optional[Union[str, datetime, date]] = None,
    settlement_date_gte: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve short interest data for stocks.
    """
    try:
        results = polygon_client.list_short_interest(
            ticker=ticker,
            settlement_date=settlement_date,
            settlement_date_lt=settlement_date_lt,
            settlement_date_lte=settlement_date_lte,
            settlement_date_gt=settlement_date_gt,
            settlement_date_gte=settlement_date_gte,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_short_volume(
    ticker: Optional[str] = None,
    date: Optional[Union[str, datetime, date]] = None,
    date_lt: Optional[Union[str, datetime, date]] = None,
    date_lte: Optional[Union[str, datetime, date]] = None,
    date_gt: Optional[Union[str, datetime, date]] = None,
    date_gte: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve short volume data for stocks.
    """
    try:
        results = polygon_client.list_short_volume(
            ticker=ticker,
            date=date,
            date_lt=date_lt,
            date_lte=date_lte,
            date_gt=date_gt,
            date_gte=date_gte,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_treasury_yields(
    date: Optional[Union[str, datetime, date]] = None,
    date_any_of: Optional[str] = None,
    date_lt: Optional[Union[str, datetime, date]] = None,
    date_lte: Optional[Union[str, datetime, date]] = None,
    date_gt: Optional[Union[str, datetime, date]] = None,
    date_gte: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Retrieve treasury yield data.
    """
    try:
        results = polygon_client.list_treasury_yields(
            date=date,
            date_lt=date_lt,
            date_lte=date_lte,
            date_gt=date_gt,
            date_gte=date_gte,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_inflation(
    date: Optional[Union[str, datetime, date]] = None,
    date_any_of: Optional[str] = None,
    date_gt: Optional[Union[str, datetime, date]] = None,
    date_gte: Optional[Union[str, datetime, date]] = None,
    date_lt: Optional[Union[str, datetime, date]] = None,
    date_lte: Optional[Union[str, datetime, date]] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get inflation data from the Federal Reserve.
    """
    try:
        results = polygon_client.list_inflation(
            date=date,
            date_any_of=date_any_of,
            date_gt=date_gt,
            date_gte=date_gte,
            date_lt=date_lt,
            date_lte=date_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_benzinga_analyst_insights(
    date: Optional[Union[str, date]] = None,
    date_any_of: Optional[str] = None,
    date_gt: Optional[Union[str, date]] = None,
    date_gte: Optional[Union[str, date]] = None,
    date_lt: Optional[Union[str, date]] = None,
    date_lte: Optional[Union[str, date]] = None,
    ticker: Optional[str] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    last_updated: Optional[str] = None,
    last_updated_any_of: Optional[str] = None,
    last_updated_gt: Optional[str] = None,
    last_updated_gte: Optional[str] = None,
    last_updated_lt: Optional[str] = None,
    last_updated_lte: Optional[str] = None,
    firm: Optional[str] = None,
    firm_any_of: Optional[str] = None,
    firm_gt: Optional[str] = None,
    firm_gte: Optional[str] = None,
    firm_lt: Optional[str] = None,
    firm_lte: Optional[str] = None,
    rating_action: Optional[str] = None,
    rating_action_any_of: Optional[str] = None,
    rating_action_gt: Optional[str] = None,
    rating_action_gte: Optional[str] = None,
    rating_action_lt: Optional[str] = None,
    rating_action_lte: Optional[str] = None,
    benzinga_firm_id: Optional[str] = None,
    benzinga_firm_id_any_of: Optional[str] = None,
    benzinga_firm_id_gt: Optional[str] = None,
    benzinga_firm_id_gte: Optional[str] = None,
    benzinga_firm_id_lt: Optional[str] = None,
    benzinga_firm_id_lte: Optional[str] = None,
    benzinga_rating_id: Optional[str] = None,
    benzinga_rating_id_any_of: Optional[str] = None,
    benzinga_rating_id_gt: Optional[str] = None,
    benzinga_rating_id_gte: Optional[str] = None,
    benzinga_rating_id_lt: Optional[str] = None,
    benzinga_rating_id_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List Benzinga analyst insights.
    """
    try:
        results = polygon_client.list_benzinga_analyst_insights(
            date=date,
            date_any_of=date_any_of,
            date_gt=date_gt,
            date_gte=date_gte,
            date_lt=date_lt,
            date_lte=date_lte,
            ticker=ticker,
            ticker_any_of=ticker_any_of,
            ticker_gt=ticker_gt,
            ticker_gte=ticker_gte,
            ticker_lt=ticker_lt,
            ticker_lte=ticker_lte,
            last_updated=last_updated,
            last_updated_any_of=last_updated_any_of,
            last_updated_gt=last_updated_gt,
            last_updated_gte=last_updated_gte,
            last_updated_lt=last_updated_lt,
            last_updated_lte=last_updated_lte,
            firm=firm,
            firm_any_of=firm_any_of,
            firm_gt=firm_gt,
            firm_gte=firm_gte,
            firm_lt=firm_lt,
            firm_lte=firm_lte,
            rating_action=rating_action,
            rating_action_any_of=rating_action_any_of,
            rating_action_gt=rating_action_gt,
            rating_action_gte=rating_action_gte,
            rating_action_lt=rating_action_lt,
            rating_action_lte=rating_action_lte,
            benzinga_firm_id=benzinga_firm_id,
            benzinga_firm_id_any_of=benzinga_firm_id_any_of,
            benzinga_firm_id_gt=benzinga_firm_id_gt,
            benzinga_firm_id_gte=benzinga_firm_id_gte,
            benzinga_firm_id_lt=benzinga_firm_id_lt,
            benzinga_firm_id_lte=benzinga_firm_id_lte,
            benzinga_rating_id=benzinga_rating_id,
            benzinga_rating_id_any_of=benzinga_rating_id_any_of,
            benzinga_rating_id_gt=benzinga_rating_id_gt,
            benzinga_rating_id_gte=benzinga_rating_id_gte,
            benzinga_rating_id_lt=benzinga_rating_id_lt,
            benzinga_rating_id_lte=benzinga_rating_id_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_benzinga_analysts(
    benzinga_id: Optional[str] = None,
    benzinga_id_any_of: Optional[str] = None,
    benzinga_id_gt: Optional[str] = None,
    benzinga_id_gte: Optional[str] = None,
    benzinga_id_lt: Optional[str] = None,
    benzinga_id_lte: Optional[str] = None,
    benzinga_firm_id: Optional[str] = None,
    benzinga_firm_id_any_of: Optional[str] = None,
    benzinga_firm_id_gt: Optional[str] = None,
    benzinga_firm_id_gte: Optional[str] = None,
    benzinga_firm_id_lt: Optional[str] = None,
    benzinga_firm_id_lte: Optional[str] = None,
    firm_name: Optional[str] = None,
    firm_name_any_of: Optional[str] = None,
    firm_name_gt: Optional[str] = None,
    firm_name_gte: Optional[str] = None,
    firm_name_lt: Optional[str] = None,
    firm_name_lte: Optional[str] = None,
    full_name: Optional[str] = None,
    full_name_any_of: Optional[str] = None,
    full_name_gt: Optional[str] = None,
    full_name_gte: Optional[str] = None,
    full_name_lt: Optional[str] = None,
    full_name_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List Benzinga analysts.
    """
    try:
        results = polygon_client.list_benzinga_analysts(
            benzinga_id=benzinga_id,
            benzinga_id_any_of=benzinga_id_any_of,
            benzinga_id_gt=benzinga_id_gt,
            benzinga_id_gte=benzinga_id_gte,
            benzinga_id_lt=benzinga_id_lt,
            benzinga_id_lte=benzinga_id_lte,
            benzinga_firm_id=benzinga_firm_id,
            benzinga_firm_id_any_of=benzinga_firm_id_any_of,
            benzinga_firm_id_gt=benzinga_firm_id_gt,
            benzinga_firm_id_gte=benzinga_firm_id_gte,
            benzinga_firm_id_lt=benzinga_firm_id_lt,
            benzinga_firm_id_lte=benzinga_firm_id_lte,
            firm_name=firm_name,
            firm_name_any_of=firm_name_any_of,
            firm_name_gt=firm_name_gt,
            firm_name_gte=firm_name_gte,
            firm_name_lt=firm_name_lt,
            firm_name_lte=firm_name_lte,
            full_name=full_name,
            full_name_any_of=full_name_any_of,
            full_name_gt=full_name_gt,
            full_name_gte=full_name_gte,
            full_name_lt=full_name_lt,
            full_name_lte=full_name_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_benzinga_consensus_ratings(
    ticker: str,
    date: Optional[Union[str, date]] = None,
    date_gt: Optional[Union[str, date]] = None,
    date_gte: Optional[Union[str, date]] = None,
    date_lt: Optional[Union[str, date]] = None,
    date_lte: Optional[Union[str, date]] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List Benzinga consensus ratings for a ticker.
    """
    try:
        results = polygon_client.list_benzinga_consensus_ratings(
            ticker=ticker,
            date=date,
            date_gt=date_gt,
            date_gte=date_gte,
            date_lt=date_lt,
            date_lte=date_lte,
            limit=limit,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_benzinga_earnings(
    date: Optional[Union[str, date]] = None,
    date_any_of: Optional[str] = None,
    date_gt: Optional[Union[str, date]] = None,
    date_gte: Optional[Union[str, date]] = None,
    date_lt: Optional[Union[str, date]] = None,
    date_lte: Optional[Union[str, date]] = None,
    ticker: Optional[str] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    importance: Optional[int] = None,
    importance_any_of: Optional[str] = None,
    importance_gt: Optional[int] = None,
    importance_gte: Optional[int] = None,
    importance_lt: Optional[int] = None,
    importance_lte: Optional[int] = None,
    last_updated: Optional[str] = None,
    last_updated_any_of: Optional[str] = None,
    last_updated_gt: Optional[str] = None,
    last_updated_gte: Optional[str] = None,
    last_updated_lt: Optional[str] = None,
    last_updated_lte: Optional[str] = None,
    date_status: Optional[str] = None,
    date_status_any_of: Optional[str] = None,
    date_status_gt: Optional[str] = None,
    date_status_gte: Optional[str] = None,
    date_status_lt: Optional[str] = None,
    date_status_lte: Optional[str] = None,
    eps_surprise_percent: Optional[float] = None,
    eps_surprise_percent_any_of: Optional[str] = None,
    eps_surprise_percent_gt: Optional[float] = None,
    eps_surprise_percent_gte: Optional[float] = None,
    eps_surprise_percent_lt: Optional[float] = None,
    eps_surprise_percent_lte: Optional[float] = None,
    revenue_surprise_percent: Optional[float] = None,
    revenue_surprise_percent_any_of: Optional[str] = None,
    revenue_surprise_percent_gt: Optional[float] = None,
    revenue_surprise_percent_gte: Optional[float] = None,
    revenue_surprise_percent_lt: Optional[float] = None,
    revenue_surprise_percent_lte: Optional[float] = None,
    fiscal_year: Optional[int] = None,
    fiscal_year_any_of: Optional[str] = None,
    fiscal_year_gt: Optional[int] = None,
    fiscal_year_gte: Optional[int] = None,
    fiscal_year_lt: Optional[int] = None,
    fiscal_year_lte: Optional[int] = None,
    fiscal_period: Optional[str] = None,
    fiscal_period_any_of: Optional[str] = None,
    fiscal_period_gt: Optional[str] = None,
    fiscal_period_gte: Optional[str] = None,
    fiscal_period_lt: Optional[str] = None,
    fiscal_period_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List Benzinga earnings.
    """
    try:
        results = polygon_client.list_benzinga_earnings(
            date=date,
            date_any_of=date_any_of,
            date_gt=date_gt,
            date_gte=date_gte,
            date_lt=date_lt,
            date_lte=date_lte,
            ticker=ticker,
            ticker_any_of=ticker_any_of,
            ticker_gt=ticker_gt,
            ticker_gte=ticker_gte,
            ticker_lt=ticker_lt,
            ticker_lte=ticker_lte,
            importance=importance,
            importance_any_of=importance_any_of,
            importance_gt=importance_gt,
            importance_gte=importance_gte,
            importance_lt=importance_lt,
            importance_lte=importance_lte,
            last_updated=last_updated,
            last_updated_any_of=last_updated_any_of,
            last_updated_gt=last_updated_gt,
            last_updated_gte=last_updated_gte,
            last_updated_lt=last_updated_lt,
            last_updated_lte=last_updated_lte,
            date_status=date_status,
            date_status_any_of=date_status_any_of,
            date_status_gt=date_status_gt,
            date_status_gte=date_status_gte,
            date_status_lt=date_status_lt,
            date_status_lte=date_status_lte,
            eps_surprise_percent=eps_surprise_percent,
            eps_surprise_percent_any_of=eps_surprise_percent_any_of,
            eps_surprise_percent_gt=eps_surprise_percent_gt,
            eps_surprise_percent_gte=eps_surprise_percent_gte,
            eps_surprise_percent_lt=eps_surprise_percent_lt,
            eps_surprise_percent_lte=eps_surprise_percent_lte,
            revenue_surprise_percent=revenue_surprise_percent,
            revenue_surprise_percent_any_of=revenue_surprise_percent_any_of,
            revenue_surprise_percent_gt=revenue_surprise_percent_gt,
            revenue_surprise_percent_gte=revenue_surprise_percent_gte,
            revenue_surprise_percent_lt=revenue_surprise_percent_lt,
            revenue_surprise_percent_lte=revenue_surprise_percent_lte,
            fiscal_year=fiscal_year,
            fiscal_year_any_of=fiscal_year_any_of,
            fiscal_year_gt=fiscal_year_gt,
            fiscal_year_gte=fiscal_year_gte,
            fiscal_year_lt=fiscal_year_lt,
            fiscal_year_lte=fiscal_year_lte,
            fiscal_period=fiscal_period,
            fiscal_period_any_of=fiscal_period_any_of,
            fiscal_period_gt=fiscal_period_gt,
            fiscal_period_gte=fiscal_period_gte,
            fiscal_period_lt=fiscal_period_lt,
            fiscal_period_lte=fiscal_period_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_benzinga_firms(
    benzinga_id: Optional[str] = None,
    benzinga_id_any_of: Optional[str] = None,
    benzinga_id_gt: Optional[str] = None,
    benzinga_id_gte: Optional[str] = None,
    benzinga_id_lt: Optional[str] = None,
    benzinga_id_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List Benzinga firms.
    """
    try:
        results = polygon_client.list_benzinga_firms(
            benzinga_id=benzinga_id,
            benzinga_id_any_of=benzinga_id_any_of,
            benzinga_id_gt=benzinga_id_gt,
            benzinga_id_gte=benzinga_id_gte,
            benzinga_id_lt=benzinga_id_lt,
            benzinga_id_lte=benzinga_id_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_benzinga_guidance(
    date: Optional[Union[str, date]] = None,
    date_any_of: Optional[str] = None,
    date_gt: Optional[Union[str, date]] = None,
    date_gte: Optional[Union[str, date]] = None,
    date_lt: Optional[Union[str, date]] = None,
    date_lte: Optional[Union[str, date]] = None,
    ticker: Optional[str] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    positioning: Optional[str] = None,
    positioning_any_of: Optional[str] = None,
    positioning_gt: Optional[str] = None,
    positioning_gte: Optional[str] = None,
    positioning_lt: Optional[str] = None,
    positioning_lte: Optional[str] = None,
    importance: Optional[int] = None,
    importance_any_of: Optional[str] = None,
    importance_gt: Optional[int] = None,
    importance_gte: Optional[int] = None,
    importance_lt: Optional[int] = None,
    importance_lte: Optional[int] = None,
    last_updated: Optional[str] = None,
    last_updated_any_of: Optional[str] = None,
    last_updated_gt: Optional[str] = None,
    last_updated_gte: Optional[str] = None,
    last_updated_lt: Optional[str] = None,
    last_updated_lte: Optional[str] = None,
    fiscal_year: Optional[int] = None,
    fiscal_year_any_of: Optional[str] = None,
    fiscal_year_gt: Optional[int] = None,
    fiscal_year_gte: Optional[int] = None,
    fiscal_year_lt: Optional[int] = None,
    fiscal_year_lte: Optional[int] = None,
    fiscal_period: Optional[str] = None,
    fiscal_period_any_of: Optional[str] = None,
    fiscal_period_gt: Optional[str] = None,
    fiscal_period_gte: Optional[str] = None,
    fiscal_period_lt: Optional[str] = None,
    fiscal_period_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List Benzinga guidance.
    """
    try:
        results = polygon_client.list_benzinga_guidance(
            date=date,
            date_any_of=date_any_of,
            date_gt=date_gt,
            date_gte=date_gte,
            date_lt=date_lt,
            date_lte=date_lte,
            ticker=ticker,
            ticker_any_of=ticker_any_of,
            ticker_gt=ticker_gt,
            ticker_gte=ticker_gte,
            ticker_lt=ticker_lt,
            ticker_lte=ticker_lte,
            positioning=positioning,
            positioning_any_of=positioning_any_of,
            positioning_gt=positioning_gt,
            positioning_gte=positioning_gte,
            positioning_lt=positioning_lt,
            positioning_lte=positioning_lte,
            importance=importance,
            importance_any_of=importance_any_of,
            importance_gt=importance_gt,
            importance_gte=importance_gte,
            importance_lt=importance_lt,
            importance_lte=importance_lte,
            last_updated=last_updated,
            last_updated_any_of=last_updated_any_of,
            last_updated_gt=last_updated_gt,
            last_updated_gte=last_updated_gte,
            last_updated_lt=last_updated_lt,
            last_updated_lte=last_updated_lte,
            fiscal_year=fiscal_year,
            fiscal_year_any_of=fiscal_year_any_of,
            fiscal_year_gt=fiscal_year_gt,
            fiscal_year_gte=fiscal_year_gte,
            fiscal_year_lt=fiscal_year_lt,
            fiscal_year_lte=fiscal_year_lte,
            fiscal_period=fiscal_period,
            fiscal_period_any_of=fiscal_period_any_of,
            fiscal_period_gt=fiscal_period_gt,
            fiscal_period_gte=fiscal_period_gte,
            fiscal_period_lt=fiscal_period_lt,
            fiscal_period_lte=fiscal_period_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_benzinga_news(
    published: Optional[str] = None,
    published_any_of: Optional[str] = None,
    published_gt: Optional[str] = None,
    published_gte: Optional[str] = None,
    published_lt: Optional[str] = None,
    published_lte: Optional[str] = None,
    last_updated: Optional[str] = None,
    last_updated_any_of: Optional[str] = None,
    last_updated_gt: Optional[str] = None,
    last_updated_gte: Optional[str] = None,
    last_updated_lt: Optional[str] = None,
    last_updated_lte: Optional[str] = None,
    tickers: Optional[str] = None,
    tickers_all_of: Optional[str] = None,
    tickers_any_of: Optional[str] = None,
    channels: Optional[str] = None,
    channels_all_of: Optional[str] = None,
    channels_any_of: Optional[str] = None,
    tags: Optional[str] = None,
    tags_all_of: Optional[str] = None,
    tags_any_of: Optional[str] = None,
    author: Optional[str] = None,
    author_any_of: Optional[str] = None,
    author_gt: Optional[str] = None,
    author_gte: Optional[str] = None,
    author_lt: Optional[str] = None,
    author_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List Benzinga news.
    """
    try:
        results = polygon_client.list_benzinga_news(
            published=published,
            published_any_of=published_any_of,
            published_gt=published_gt,
            published_gte=published_gte,
            published_lt=published_lt,
            published_lte=published_lte,
            last_updated=last_updated,
            last_updated_any_of=last_updated_any_of,
            last_updated_gt=last_updated_gt,
            last_updated_gte=last_updated_gte,
            last_updated_lt=last_updated_lt,
            last_updated_lte=last_updated_lte,
            tickers=tickers,
            tickers_all_of=tickers_all_of,
            tickers_any_of=tickers_any_of,
            channels=channels,
            channels_all_of=channels_all_of,
            channels_any_of=channels_any_of,
            tags=tags,
            tags_all_of=tags_all_of,
            tags_any_of=tags_any_of,
            author=author,
            author_any_of=author_any_of,
            author_gt=author_gt,
            author_gte=author_gte,
            author_lt=author_lt,
            author_lte=author_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_benzinga_ratings(
    date: Optional[Union[str, date]] = None,
    date_any_of: Optional[str] = None,
    date_gt: Optional[Union[str, date]] = None,
    date_gte: Optional[Union[str, date]] = None,
    date_lt: Optional[Union[str, date]] = None,
    date_lte: Optional[Union[str, date]] = None,
    ticker: Optional[str] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    importance: Optional[int] = None,
    importance_any_of: Optional[str] = None,
    importance_gt: Optional[int] = None,
    importance_gte: Optional[int] = None,
    importance_lt: Optional[int] = None,
    importance_lte: Optional[int] = None,
    last_updated: Optional[str] = None,
    last_updated_any_of: Optional[str] = None,
    last_updated_gt: Optional[str] = None,
    last_updated_gte: Optional[str] = None,
    last_updated_lt: Optional[str] = None,
    last_updated_lte: Optional[str] = None,
    rating_action: Optional[str] = None,
    rating_action_any_of: Optional[str] = None,
    rating_action_gt: Optional[str] = None,
    rating_action_gte: Optional[str] = None,
    rating_action_lt: Optional[str] = None,
    rating_action_lte: Optional[str] = None,
    price_target_action: Optional[str] = None,
    price_target_action_any_of: Optional[str] = None,
    price_target_action_gt: Optional[str] = None,
    price_target_action_gte: Optional[str] = None,
    price_target_action_lt: Optional[str] = None,
    price_target_action_lte: Optional[str] = None,
    benzinga_id: Optional[str] = None,
    benzinga_id_any_of: Optional[str] = None,
    benzinga_id_gt: Optional[str] = None,
    benzinga_id_gte: Optional[str] = None,
    benzinga_id_lt: Optional[str] = None,
    benzinga_id_lte: Optional[str] = None,
    benzinga_analyst_id: Optional[str] = None,
    benzinga_analyst_id_any_of: Optional[str] = None,
    benzinga_analyst_id_gt: Optional[str] = None,
    benzinga_analyst_id_gte: Optional[str] = None,
    benzinga_analyst_id_lt: Optional[str] = None,
    benzinga_analyst_id_lte: Optional[str] = None,
    benzinga_firm_id: Optional[str] = None,
    benzinga_firm_id_any_of: Optional[str] = None,
    benzinga_firm_id_gt: Optional[str] = None,
    benzinga_firm_id_gte: Optional[str] = None,
    benzinga_firm_id_lt: Optional[str] = None,
    benzinga_firm_id_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List Benzinga ratings.
    """
    try:
        results = polygon_client.list_benzinga_ratings(
            date=date,
            date_any_of=date_any_of,
            date_gt=date_gt,
            date_gte=date_gte,
            date_lt=date_lt,
            date_lte=date_lte,
            ticker=ticker,
            ticker_any_of=ticker_any_of,
            ticker_gt=ticker_gt,
            ticker_gte=ticker_gte,
            ticker_lt=ticker_lt,
            ticker_lte=ticker_lte,
            importance=importance,
            importance_any_of=importance_any_of,
            importance_gt=importance_gt,
            importance_gte=importance_gte,
            importance_lt=importance_lt,
            importance_lte=importance_lte,
            last_updated=last_updated,
            last_updated_any_of=last_updated_any_of,
            last_updated_gt=last_updated_gt,
            last_updated_gte=last_updated_gte,
            last_updated_lt=last_updated_lt,
            last_updated_lte=last_updated_lte,
            rating_action=rating_action,
            rating_action_any_of=rating_action_any_of,
            rating_action_gt=rating_action_gt,
            rating_action_gte=rating_action_gte,
            rating_action_lt=rating_action_lt,
            rating_action_lte=rating_action_lte,
            price_target_action=price_target_action,
            price_target_action_any_of=price_target_action_any_of,
            price_target_action_gt=price_target_action_gt,
            price_target_action_gte=price_target_action_gte,
            price_target_action_lt=price_target_action_lt,
            price_target_action_lte=price_target_action_lte,
            benzinga_id=benzinga_id,
            benzinga_id_any_of=benzinga_id_any_of,
            benzinga_id_gt=benzinga_id_gt,
            benzinga_id_gte=benzinga_id_gte,
            benzinga_id_lt=benzinga_id_lt,
            benzinga_id_lte=benzinga_id_lte,
            benzinga_analyst_id=benzinga_analyst_id,
            benzinga_analyst_id_any_of=benzinga_analyst_id_any_of,
            benzinga_analyst_id_gt=benzinga_analyst_id_gt,
            benzinga_analyst_id_gte=benzinga_analyst_id_gte,
            benzinga_analyst_id_lt=benzinga_analyst_id_lt,
            benzinga_analyst_id_lte=benzinga_analyst_id_lte,
            benzinga_firm_id=benzinga_firm_id,
            benzinga_firm_id_any_of=benzinga_firm_id_any_of,
            benzinga_firm_id_gt=benzinga_firm_id_gt,
            benzinga_firm_id_gte=benzinga_firm_id_gte,
            benzinga_firm_id_lt=benzinga_firm_id_lt,
            benzinga_firm_id_lte=benzinga_firm_id_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_aggregates(
    ticker: str,
    resolution: str,
    window_start: Optional[str] = None,
    window_start_lt: Optional[str] = None,
    window_start_lte: Optional[str] = None,
    window_start_gt: Optional[str] = None,
    window_start_gte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get aggregates for a futures contract in a given time range.
    """
    try:
        results = polygon_client.list_futures_aggregates(
            ticker=ticker,
            resolution=resolution,
            window_start=window_start,
            window_start_lt=window_start_lt,
            window_start_lte=window_start_lte,
            window_start_gt=window_start_gt,
            window_start_gte=window_start_gte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_contracts(
    product_code: Optional[str] = None,
    first_trade_date: Optional[Union[str, date]] = None,
    last_trade_date: Optional[Union[str, date]] = None,
    as_of: Optional[Union[str, date]] = None,
    active: Optional[str] = None,
    type: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get a paginated list of futures contracts.
    """
    try:
        results = polygon_client.list_futures_contracts(
            product_code=product_code,
            first_trade_date=first_trade_date,
            last_trade_date=last_trade_date,
            as_of=as_of,
            active=active,
            type=type,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_futures_contract_details(
    ticker: str,
    as_of: Optional[Union[str, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get details for a single futures contract at a specified point in time.
    """
    try:
        results = polygon_client.get_futures_contract_details(
            ticker=ticker,
            as_of=as_of,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_products(
    name: Optional[str] = None,
    name_search: Optional[str] = None,
    as_of: Optional[Union[str, date]] = None,
    trading_venue: Optional[str] = None,
    sector: Optional[str] = None,
    sub_sector: Optional[str] = None,
    asset_class: Optional[str] = None,
    asset_sub_class: Optional[str] = None,
    type: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get a list of futures products (including combos).
    """
    try:
        results = polygon_client.list_futures_products(
            name=name,
            name_search=name_search,
            as_of=as_of,
            trading_venue=trading_venue,
            sector=sector,
            sub_sector=sub_sector,
            asset_class=asset_class,
            asset_sub_class=asset_sub_class,
            type=type,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_futures_product_details(
    product_code: str,
    type: Optional[str] = None,
    as_of: Optional[Union[str, date]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get details for a single futures product as it was at a specific day.
    """
    try:
        results = polygon_client.get_futures_product_details(
            product_code=product_code,
            type=type,
            as_of=as_of,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_quotes(
    ticker: str,
    timestamp: Optional[str] = None,
    timestamp_lt: Optional[str] = None,
    timestamp_lte: Optional[str] = None,
    timestamp_gt: Optional[str] = None,
    timestamp_gte: Optional[str] = None,
    session_end_date: Optional[str] = None,
    session_end_date_lt: Optional[str] = None,
    session_end_date_lte: Optional[str] = None,
    session_end_date_gt: Optional[str] = None,
    session_end_date_gte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get quotes for a futures contract in a given time range.
    """
    try:
        results = polygon_client.list_futures_quotes(
            ticker=ticker,
            timestamp=timestamp,
            timestamp_lt=timestamp_lt,
            timestamp_lte=timestamp_lte,
            timestamp_gt=timestamp_gt,
            timestamp_gte=timestamp_gte,
            session_end_date=session_end_date,
            session_end_date_lt=session_end_date_lt,
            session_end_date_lte=session_end_date_lte,
            session_end_date_gt=session_end_date_gt,
            session_end_date_gte=session_end_date_gte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_trades(
    ticker: str,
    timestamp: Optional[str] = None,
    timestamp_lt: Optional[str] = None,
    timestamp_lte: Optional[str] = None,
    timestamp_gt: Optional[str] = None,
    timestamp_gte: Optional[str] = None,
    session_end_date: Optional[str] = None,
    session_end_date_lt: Optional[str] = None,
    session_end_date_lte: Optional[str] = None,
    session_end_date_gt: Optional[str] = None,
    session_end_date_gte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get trades for a futures contract in a given time range.
    """
    try:
        results = polygon_client.list_futures_trades(
            ticker=ticker,
            timestamp=timestamp,
            timestamp_lt=timestamp_lt,
            timestamp_lte=timestamp_lte,
            timestamp_gt=timestamp_gt,
            timestamp_gte=timestamp_gte,
            session_end_date=session_end_date,
            session_end_date_lt=session_end_date_lt,
            session_end_date_lte=session_end_date_lte,
            session_end_date_gt=session_end_date_gt,
            session_end_date_gte=session_end_date_gte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_schedules(
    session_end_date: Optional[str] = None,
    trading_venue: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get trading schedules for multiple futures products on a specific date.
    """
    try:
        results = polygon_client.list_futures_schedules(
            session_end_date=session_end_date,
            trading_venue=trading_venue,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_schedules_by_product_code(
    product_code: str,
    session_end_date: Optional[str] = None,
    session_end_date_lt: Optional[str] = None,
    session_end_date_lte: Optional[str] = None,
    session_end_date_gt: Optional[str] = None,
    session_end_date_gte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get schedule data for a single futures product across many trading dates.
    """
    try:
        results = polygon_client.list_futures_schedules_by_product_code(
            product_code=product_code,
            session_end_date=session_end_date,
            session_end_date_lt=session_end_date_lt,
            session_end_date_lte=session_end_date_lte,
            session_end_date_gt=session_end_date_gt,
            session_end_date_gte=session_end_date_gte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_futures_market_statuses(
    product_code_any_of: Optional[str] = None,
    product_code: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get market statuses for futures products.
    """
    try:
        results = polygon_client.list_futures_market_statuses(
            product_code_any_of=product_code_any_of,
            product_code=product_code,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_futures_snapshot(
    ticker: Optional[str] = None,
    ticker_any_of: Optional[str] = None,
    ticker_gt: Optional[str] = None,
    ticker_gte: Optional[str] = None,
    ticker_lt: Optional[str] = None,
    ticker_lte: Optional[str] = None,
    product_code: Optional[str] = None,
    product_code_any_of: Optional[str] = None,
    product_code_gt: Optional[str] = None,
    product_code_gte: Optional[str] = None,
    product_code_lt: Optional[str] = None,
    product_code_lte: Optional[str] = None,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get snapshots for futures contracts.
    """
    try:
        results = polygon_client.get_futures_snapshot(
            ticker=ticker,
            ticker_any_of=ticker_any_of,
            ticker_gt=ticker_gt,
            ticker_gte=ticker_gte,
            ticker_lt=ticker_lt,
            ticker_lte=ticker_lte,
            product_code=product_code,
            product_code_any_of=product_code_any_of,
            product_code_gt=product_code_gt,
            product_code_gte=product_code_gte,
            product_code_lt=product_code_lt,
            product_code_lte=product_code_lte,
            limit=limit,
            sort=sort,
            params=params,
            raw=True,
        )

        return json_to_csv(results.data.decode("utf-8"))
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_options_contracts(
    underlying_ticker: Optional[str] = None,
    ticker: Optional[str] = None,
    contract_type: Optional[str] = None,
    expiration_date: Optional[str] = None,
    expiration_date_gte: Optional[str] = None,
    expiration_date_lte: Optional[str] = None,
    expiration_date_gt: Optional[str] = None,
    expiration_date_lt: Optional[str] = None,
    strike_price_gte: Optional[str] = None,
    strike_price_lte: Optional[str] = None,
    expired: Optional[bool] = False,
    limit: Optional[int] = 10,
    sort: Optional[str] = None,
    order: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    List currently active options contracts.

    Search and filter available option contracts by underlying ticker, expiration,
    strike price, and other criteria. Essential for finding available options to trade.

    **Use Case**: Use this endpoint to discover available option contracts before trading.
    This is typically the first step in options analysis - find contracts that match your
    criteria (ticker, expiration, strike range), then use get_options_contract for detailed
    specifications or list_snapshot_options_chain for live market data with Greeks and IV.

    **Options Ticker Format**: Results include tickers in format O:[TICKER][YYMMDD][C/P][STRIKE*1000]
    Example: O:AAPL251121C00230000 = AAPL call expiring Nov 21, 2025 with $230 strike

    **Example Workflow**:
    1. Use this endpoint to find all AAPL call options expiring in next 30 days with strikes 220-240
    2. Review the list of available contracts (ticker, expiration_date, strike_price)
    3. Use list_snapshot_options_chain to get live pricing and Greeks for contracts of interest
    4. Use get_options_contract to get detailed specifications for specific contracts

    Args:
        underlying_ticker: Filter by underlying stock ticker (e.g., 'AAPL')
        ticker: Filter by specific options ticker (e.g., 'O:AAPL251121C00230000')
        contract_type: Filter by 'call' or 'put'
        expiration_date: Exact expiration date (YYYY-MM-DD)
        expiration_date_gte: Expiration date >= this date (YYYY-MM-DD)
        expiration_date_lte: Expiration date <= this date (YYYY-MM-DD)
        expiration_date_gt: Expiration date > this date (YYYY-MM-DD)
        expiration_date_lt: Expiration date < this date (YYYY-MM-DD)
        strike_price_gte: Strike price >= this value (numeric)
        strike_price_lte: Strike price <= this value (numeric)
        expired: Include expired contracts (default: False, only active contracts)
        limit: Maximum number of results (default: 10)
        sort: Sort field (default: 'expiration_date')
        order: Sort order 'asc' or 'desc' (default: 'asc')
        params: Additional query parameters

    Returns:
        CSV formatted list of option contracts with fields:
        - ticker: Options contract identifier
        - underlying_ticker: Stock symbol
        - contract_type: 'call' or 'put'
        - expiration_date: Contract expiration date
        - strike_price: Strike/exercise price
        - shares_per_contract: Usually 100 shares per contract
        - exercise_style: 'american' (can exercise anytime) or 'european' (only at expiration)
    """
    try:
        # list_options_contracts returns an iterator even with raw=True
        # We need to collect results and format them
        results_list = []
        count = 0
        for contract in polygon_client.list_options_contracts(
            underlying_ticker=underlying_ticker,
            contract_type=contract_type,
            expiration_date=expiration_date,
            expiration_date_gte=expiration_date_gte,
            expiration_date_lte=expiration_date_lte,
            expiration_date_gt=expiration_date_gt,
            expiration_date_lt=expiration_date_lt,
            strike_price_gte=strike_price_gte,
            strike_price_lte=strike_price_lte,
            expired=expired,
            limit=limit,
            sort=sort,
            order=order,
            params=params,
        ):
            # Convert OptionsContract object to dict
            contract_dict = {
                'ticker': contract.ticker,
                'underlying_ticker': contract.underlying_ticker,
                'contract_type': contract.contract_type,
                'expiration_date': str(contract.expiration_date) if hasattr(contract, 'expiration_date') else None,
                'strike_price': contract.strike_price if hasattr(contract, 'strike_price') else None,
                'shares_per_contract': contract.shares_per_contract if hasattr(contract, 'shares_per_contract') else None,
                'exercise_style': contract.exercise_style if hasattr(contract, 'exercise_style') else None,
            }
            results_list.append(contract_dict)
            count += 1
            if limit and count >= limit:
                break

        # Format as results dict for json_to_csv
        results_dict = {"results": results_list}

        return json_to_csv(results_dict)
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def get_options_contract(
    ticker: str,
    as_of: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get detailed information about a specific option contract.

    Retrieve contract specifications including strike price, expiration date,
    contract type, underlying ticker, and other details.

    **Use Case**: Retrieve static contract specifications for a known options ticker.
    Use this endpoint after discovering contracts with list_options_contracts when you need
    detailed information about a specific contract's specifications and structure.

    **Important**: This endpoint returns CONTRACT SPECIFICATIONS (static data), not live market
    data. For real-time pricing, Greeks, implied volatility, and trading metrics, use
    list_snapshot_options_chain or get_snapshot_option instead.

    **Typical Workflow**:
    1. Discovery: Use list_options_contracts to find available contracts
    2. Specifications: Use this endpoint to get detailed contract specs
    3. Market Data: Use list_snapshot_options_chain for live pricing/Greeks/IV
    4. Trading Decision: Make informed decision based on specs and market data

    Args:
        ticker: The options ticker (e.g., 'O:AAPL251121C00230000')
                Format: O:[TICKER][YYMMDD][C/P][STRIKE*1000]
        as_of: Date to get contract details as of (YYYY-MM-DD format).
               Optional - defaults to most recent data
        params: Additional query parameters

    Returns:
        CSV formatted contract details with fields:
        - ticker: Options contract identifier
        - underlying_ticker: The underlying stock symbol
        - contract_type: 'call' (right to buy) or 'put' (right to sell)
        - expiration_date: Date when contract expires
        - strike_price: Exercise/strike price of the option
        - shares_per_contract: Number of shares per contract (typically 100)
        - exercise_style: 'american' (exercise anytime) or 'european' (exercise at expiration only)
        - primary_exchange: Exchange where contract trades (e.g., 'BATO')
        - cfi: Classification of Financial Instruments code
    """
    try:
        # get_options_contract returns an OptionsContract object, not raw response
        contract = polygon_client.get_options_contract(
            ticker=ticker,
            as_of=as_of,
            params=params,
        )

        # Convert OptionsContract object to dict
        contract_dict = {
            'ticker': contract.ticker,
            'underlying_ticker': contract.underlying_ticker,
            'contract_type': contract.contract_type,
            'expiration_date': str(contract.expiration_date) if hasattr(contract, 'expiration_date') else None,
            'strike_price': contract.strike_price if hasattr(contract, 'strike_price') else None,
            'shares_per_contract': contract.shares_per_contract if hasattr(contract, 'shares_per_contract') else None,
            'exercise_style': contract.exercise_style if hasattr(contract, 'exercise_style') else None,
            'primary_exchange': contract.primary_exchange if hasattr(contract, 'primary_exchange') else None,
            'cfi': contract.cfi if hasattr(contract, 'cfi') else None,
        }

        # Format as results dict for json_to_csv
        results_dict = {"results": contract_dict}

        return json_to_csv(results_dict)
    except Exception as e:
        return f"Error: {e}"


@poly_mcp.tool(annotations=ToolAnnotations(readOnlyHint=True))
async def list_snapshot_options_chain(
    underlying_ticker: str,
    strike_price_gte: Optional[float] = None,
    strike_price_lte: Optional[float] = None,
    expiration_date_gte: Optional[str] = None,
    expiration_date_lte: Optional[str] = None,
    contract_type: Optional[str] = None,
    limit: Optional[int] = 10,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Get snapshots for an entire options chain.

    Retrieve market data snapshots for all option contracts of an underlying ticker.
    Includes Greeks (delta, gamma, theta, vega), implied volatility, prices,
    open interest, and other metrics for each contract in the chain.

    **Use Case**: Get live market data for multiple option contracts at once. Essential for:
    - Analyzing options strategies (spreads, straddles, butterflies, iron condors)
    - Comparing contracts across different strikes and expirations
    - Assessing risk exposure through Greeks
    - Evaluating contract liquidity through open interest
    - Finding best entry/exit points based on IV and pricing

    **Key Data Points Explained**:

    *Greeks (Risk Metrics)*:
    - **Delta** (greeks_delta): Price sensitivity to $1 change in underlying stock
      Range: 0 to 1 for calls, 0 to -1 for puts. High delta = moves more with stock price
    - **Gamma** (greeks_gamma): Rate of change of delta. High gamma = delta changes quickly
    - **Theta** (greeks_theta): Time decay per day. How much option loses value each day
      Negative value = option loses value over time (always negative for long positions)
    - **Vega** (greeks_vega): Sensitivity to 1% change in implied volatility
      High vega = option price sensitive to volatility changes

    *Pricing & Volatility*:
    - **Implied Volatility** (implied_volatility): Market's expectation of future volatility
      Expressed as annualized percentage. Higher IV = more expensive options
    - **Break-Even Price** (break_even_price): Stock price needed at expiration for zero profit/loss
    - **Last Quote** (ask/bid/midpoint): Current market pricing for the contract

    *Liquidity & Volume*:
    - **Open Interest** (open_interest): Total outstanding contracts. Higher = more liquid
    - **Day Volume** (day_volume): Contracts traded today. Higher = more active

    **Trading Applications**:
    - Strategy Building: Compare Greeks across strikes to build spreads or combos
    - Risk Management: Monitor delta (directional risk) and theta (time decay risk)
    - Entry Timing: Use IV to identify overpriced or underpriced options
    - Liquidity Check: Check open_interest before entering positions

    **Typical Workflow**:
    1. Use list_options_contracts to narrow down contract universe
    2. Use this endpoint to get live data with Greeks and IV for comparison
    3. Analyze Greeks to understand risk profile
    4. Select contracts and build strategy
    5. Monitor positions using get_snapshot_option

    Args:
        underlying_ticker: The underlying stock ticker (e.g., 'AAPL')
        strike_price_gte: Filter by strike price >= this value (numeric)
        strike_price_lte: Filter by strike price <= this value (numeric)
        expiration_date_gte: Filter by expiration date >= this date (YYYY-MM-DD)
        expiration_date_lte: Filter by expiration date <= this date (YYYY-MM-DD)
        contract_type: Filter by 'call' or 'put'
        limit: Maximum number of results (default: 10)
        params: Additional query parameters (can include filters like 'expiration_date.gte', 'strike_price.gte')

    Returns:
        CSV formatted options chain data with fields:
        - ticker: Option contract identifier
        - break_even_price: Stock price needed for zero profit/loss at expiration
        - day_change/day_change_percent: Daily price movement
        - day_open/high/low/close: Daily OHLC prices
        - day_volume: Contracts traded today
        - day_vwap: Volume-weighted average price
        - implied_volatility: Market's volatility expectation (annualized %)
        - open_interest: Total outstanding contracts (liquidity indicator)
        - greeks_delta/gamma/theta/vega: Risk sensitivity metrics
        - last_quote_ask/bid/midpoint: Current market pricing
        - last_trade_price/size: Most recent trade execution
    """
    try:
        # Build params dict for filters
        filter_params = params or {}

        if strike_price_gte is not None:
            filter_params['strike_price.gte'] = strike_price_gte
        if strike_price_lte is not None:
            filter_params['strike_price.lte'] = strike_price_lte
        if expiration_date_gte is not None:
            filter_params['expiration_date.gte'] = expiration_date_gte
        if expiration_date_lte is not None:
            filter_params['expiration_date.lte'] = expiration_date_lte
        if contract_type is not None:
            filter_params['contract_type'] = contract_type

        # list_snapshot_options_chain returns an iterator of OptionContractSnapshot objects
        results_list = []
        count = 0
        for snapshot in polygon_client.list_snapshot_options_chain(
            underlying_ticker,  # Note: SDK actually uses underlying_asset
            params=filter_params,
        ):
            # Convert OptionContractSnapshot object to dict
            snapshot_dict = {
                'ticker': snapshot.ticker if hasattr(snapshot, 'ticker') else None,
                'break_even_price': snapshot.break_even_price if hasattr(snapshot, 'break_even_price') else None,
                'day_change': snapshot.day.change if hasattr(snapshot, 'day') and hasattr(snapshot.day, 'change') else None,
                'day_change_percent': snapshot.day.change_percent if hasattr(snapshot, 'day') and hasattr(snapshot.day, 'change_percent') else None,
                'day_open': snapshot.day.open if hasattr(snapshot, 'day') and hasattr(snapshot.day, 'open') else None,
                'day_high': snapshot.day.high if hasattr(snapshot, 'day') and hasattr(snapshot.day, 'high') else None,
                'day_low': snapshot.day.low if hasattr(snapshot, 'day') and hasattr(snapshot.day, 'low') else None,
                'day_close': snapshot.day.close if hasattr(snapshot, 'day') and hasattr(snapshot.day, 'close') else None,
                'day_volume': snapshot.day.volume if hasattr(snapshot, 'day') and hasattr(snapshot.day, 'volume') else None,
                'day_vwap': snapshot.day.vwap if hasattr(snapshot, 'day') and hasattr(snapshot.day, 'vwap') else None,
                'implied_volatility': snapshot.implied_volatility if hasattr(snapshot, 'implied_volatility') else None,
                'open_interest': snapshot.open_interest if hasattr(snapshot, 'open_interest') else None,
                'greeks_delta': snapshot.greeks.delta if hasattr(snapshot, 'greeks') and hasattr(snapshot.greeks, 'delta') else None,
                'greeks_gamma': snapshot.greeks.gamma if hasattr(snapshot, 'greeks') and hasattr(snapshot.greeks, 'gamma') else None,
                'greeks_theta': snapshot.greeks.theta if hasattr(snapshot, 'greeks') and hasattr(snapshot.greeks, 'theta') else None,
                'greeks_vega': snapshot.greeks.vega if hasattr(snapshot, 'greeks') and hasattr(snapshot.greeks, 'vega') else None,
                'last_quote_ask': snapshot.last_quote.ask if hasattr(snapshot, 'last_quote') and hasattr(snapshot.last_quote, 'ask') else None,
                'last_quote_bid': snapshot.last_quote.bid if hasattr(snapshot, 'last_quote') and hasattr(snapshot.last_quote, 'bid') else None,
                'last_quote_midpoint': snapshot.last_quote.midpoint if hasattr(snapshot, 'last_quote') and hasattr(snapshot.last_quote, 'midpoint') else None,
                'last_trade_price': snapshot.last_trade.price if hasattr(snapshot, 'last_trade') and hasattr(snapshot.last_trade, 'price') else None,
                'last_trade_size': snapshot.last_trade.size if hasattr(snapshot, 'last_trade') and hasattr(snapshot.last_trade, 'size') else None,
            }
            results_list.append(snapshot_dict)
            count += 1
            if limit and count >= limit:
                break

        # Format as a results dict for json_to_csv
        results_dict = {"results": results_list}

        return json_to_csv(results_dict)
    except Exception as e:
        return f"Error: {e}"


# Directly expose the MCP server object
# It will be run from entrypoint.py


def run(
    transport: Literal["stdio", "sse", "streamable-http"] = "stdio",
    host: str = "127.0.0.1",
    port: int = 8000
) -> None:
    """Run the Massive MCP server."""
    if transport in ["streamable-http", "sse"]:
        poly_mcp.settings.host = host
        poly_mcp.settings.port = port
    poly_mcp.run(transport)
