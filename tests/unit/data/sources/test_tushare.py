from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime

import pytest

from quant_research.data.contracts import JsonValue
from quant_research.domain.errors import QuantError
from quant_research.infrastructure.tushare.client import (
    TushareClient,
    TushareConfig,
)


class _Frame:
    def __init__(self, fields: tuple[str, ...]) -> None:
        self.columns = fields
        self._fields = fields

    def to_dict(self, orient: str) -> list[dict[str, object]]:
        assert orient == "records"
        return []


class _Gateway:
    def connect(self, token: str) -> object:
        assert token == "token"
        return object()

    def call(
        self,
        api: object,
        endpoint: str,
        params: Mapping[str, JsonValue],
        fields: tuple[str, ...],
    ) -> _Frame:
        del api, endpoint, params
        return _Frame(fields)


def _client() -> TushareClient:
    return TushareClient(
        _Gateway(),
        TushareConfig(
            token="token",
            benchmark_indexes=("000300.SH",),
            max_attempts=1,
            retry_backoff_seconds=(),
        ),
        clock=lambda: datetime(2026, 8, 26, tzinfo=UTC),
    )


def test_only_index_daily_requests_may_contain_ts_code() -> None:
    client = _client()
    day = date(2026, 8, 25)
    endpoints = (
        "stock_basic",
        "fund_basic",
        "index_basic",
        "trade_cal",
        "daily_vip",
        "adj_factor",
        "fund_daily",
        "fund_adj",
        "daily_basic",
        "suspend_d",
        "stock_st",
        "fina_indicator_vip",
        "index_classify",
        "index_member_all",
    )
    for endpoint in endpoints:
        assert all(
            "ts_code" not in request
            for request in client.requests(endpoint, day, day)
        )
    assert client.requests("index_daily", day, day)[0]["ts_code"] == "000300.SH"


def test_dynamic_token_provider_reconnects_when_dashboard_setting_changes() -> None:
    tokens = ["first-token"]
    connected: list[str] = []

    class _DynamicGateway(_Gateway):
        def connect(self, token: str) -> object:
            connected.append(token)
            return object()

    client = TushareClient(
        _DynamicGateway(),
        TushareConfig(
            token="",
            token_provider=lambda: tokens[0],
            benchmark_indexes=("000300.SH",),
            max_attempts=1,
            retry_backoff_seconds=(),
        ),
    )

    client.login()
    client.login()
    tokens[0] = "second-token"
    client.login()

    assert connected == ["first-token", "second-token"]


def test_missing_dynamic_token_fails_before_gateway_connection() -> None:
    connected: list[str] = []

    class _MissingGateway(_Gateway):
        def connect(self, token: str) -> object:
            connected.append(token)
            return object()

    client = TushareClient(
        _MissingGateway(),
        TushareConfig(
            token="",
            token_provider=lambda: None,
            benchmark_indexes=("000300.SH",),
            max_attempts=1,
            retry_backoff_seconds=(),
        ),
    )

    with pytest.raises(QuantError) as missing:
        client.login()

    assert missing.value.detail.code == "TUSHARE_TOKEN_MISSING"
    assert connected == []


def test_fetch_rejects_per_instrument_non_index_request() -> None:
    client = _client()
    client.login()
    with pytest.raises(ValueError, match="only index_daily"):
        tuple(
            client.fetch(
                "daily_vip",
                {"endpoint": "daily_vip", "ts_code": "600000.SH"},
            )
        )


def test_market_endpoints_require_one_trade_date() -> None:
    with pytest.raises(ValueError, match="one market trade date"):
        _client().requests(
            "daily_vip", date(2026, 8, 24), date(2026, 8, 25)
        )
