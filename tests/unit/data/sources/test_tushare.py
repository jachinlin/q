from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from threading import Thread

import pytest

from quant_research.data.contracts import JsonValue
from quant_research.domain.errors import QuantError
from quant_research.infrastructure.tushare.client import (
    TushareClient,
    TushareConfig,
    TushareSdkGateway,
)
from quant_research.infrastructure.tushare.rate_limit import TushareRateLimiter


class _Frame:
    def __init__(
        self,
        fields: tuple[str, ...],
        records: list[dict[str, object]] | None = None,
    ) -> None:
        self.columns = fields
        self._fields = fields
        self._records = records or []

    def to_dict(self, orient: str) -> list[dict[str, object]]:
        assert orient == "records"
        return self._records


class _Gateway:
    def connect(self, token: str, proxy_url: str | None) -> object:
        assert token == "token"
        assert proxy_url is None
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
        "daily",
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
            "ts_code" not in request for request in client.requests(endpoint, day, day)
        )
    assert client.requests("index_daily", day, day)[0]["ts_code"] == "000300.SH"


def test_dynamic_token_provider_reconnects_when_dashboard_setting_changes() -> None:
    tokens = ["first-token"]
    connected: list[str] = []

    class _DynamicGateway(_Gateway):
        def connect(self, token: str, proxy_url: str | None) -> object:
            assert proxy_url is None
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
        def connect(self, token: str, proxy_url: str | None) -> object:
            assert proxy_url is None
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


def test_dynamic_proxy_provider_reconnects_with_normalized_url() -> None:
    proxies: list[str | None] = [None]
    connected: list[tuple[str, str | None]] = []

    class _ProxyGateway(_Gateway):
        def connect(self, token: str, proxy_url: str | None) -> object:
            connected.append((token, proxy_url))
            return object()

    client = TushareClient(
        _ProxyGateway(),
        TushareConfig(
            token="token",
            proxy_provider=lambda: proxies[0],
            benchmark_indexes=("000300.SH",),
            max_attempts=1,
            retry_backoff_seconds=(),
        ),
    )

    client.login()
    proxies[0] = "https://proxy.example.test/"
    client.login()
    client.login()

    assert connected == [
        ("token", None),
        ("token", "https://proxy.example.test"),
    ]


def test_sdk_gateway_writes_proxy_url_to_data_api_object() -> None:
    api = TushareSdkGateway().connect(
        "test-token",
        "https://proxy.example.test",
    )

    assert api._DataApi__http_url == "https://proxy.example.test"


def test_fetch_rejects_per_instrument_non_index_request() -> None:
    client = _client()
    client.login()
    with pytest.raises(ValueError, match="only index_daily"):
        tuple(
            client.fetch(
                "daily",
                {"endpoint": "daily", "ts_code": "600000.SH"},
            )
        )


def test_market_endpoints_require_one_trade_date() -> None:
    with pytest.raises(ValueError, match="one market trade date"):
        _client().requests("daily", date(2026, 8, 24), date(2026, 8, 25))


def test_fund_endpoints_start_with_their_documented_page_sizes() -> None:
    client = _client()
    day = date(2026, 8, 25)

    assert client.requests("fund_daily", day, day) == (
        {
            "endpoint": "fund_daily",
            "fields": (
                "ts_code,trade_date,open,high,low,close,pre_close,change,"
                "pct_chg,vol,amount"
            ),
            "trade_date": "20260825",
            "limit": 5000,
            "offset": 0,
        },
    )
    assert client.requests("fund_adj", day, day) == (
        {
            "endpoint": "fund_adj",
            "fields": "ts_code,trade_date,adj_factor",
            "trade_date": "20260825",
            "limit": 2000,
            "offset": 0,
        },
    )


def test_industry_membership_uses_complete_si_codes_and_both_history_slices() -> None:
    requests = _client().requests(
        "index_member_all",
        date(2026, 8, 25),
        date(2026, 8, 25),
    )

    assert len(requests) == 62
    assert all(str(request["l1_code"]).endswith(".SI") for request in requests)
    assert {
        (str(request["l1_code"]), str(request["is_new"])) for request in requests
    } >= {("801010.SI", "Y"), ("801010.SI", "N")}


def test_fund_adjustment_factor_paginates_and_rate_limits_every_page() -> None:
    class _PaginatedGateway(_Gateway):
        def __init__(self) -> None:
            self.calls: list[tuple[int, int]] = []

        def call(
            self,
            api: object,
            endpoint: str,
            params: Mapping[str, JsonValue],
            fields: tuple[str, ...],
        ) -> _Frame:
            del api
            assert endpoint == "fund_adj"
            limit = int(params["limit"])
            offset = int(params["offset"])
            self.calls.append((limit, offset))
            count = 2000 if offset == 0 else 129
            records = [
                {
                    "ts_code": f"FUND{offset + index:06d}.SH",
                    "trade_date": "20260825",
                    "adj_factor": "1",
                }
                for index in range(count)
            ]
            return _Frame(fields, records)

    class _CountingLimiter(TushareRateLimiter):
        def __init__(self) -> None:
            super().__init__(lambda: 480)
            self.calls = 0

        def acquire(self) -> None:
            self.calls += 1

    gateway = _PaginatedGateway()
    limiter = _CountingLimiter()
    client = TushareClient(
        gateway,
        TushareConfig(
            token="token",
            benchmark_indexes=("000300.SH",),
            max_attempts=1,
            retry_backoff_seconds=(),
        ),
        rate_limiter=limiter,
    )
    request = client.requests("fund_adj", date(2026, 8, 25), date(2026, 8, 25))[0]
    client.login()

    batch = next(iter(client.fetch("fund_adj", request)))

    assert len(batch.rows) == 2129
    assert batch.request == request
    assert gateway.calls == [(2000, 0), (2000, 2000)]
    assert limiter.calls == 2


def test_fund_pagination_rejects_duplicate_cross_page_keys() -> None:
    class _DuplicateGateway(_Gateway):
        def call(
            self,
            api: object,
            endpoint: str,
            params: Mapping[str, JsonValue],
            fields: tuple[str, ...],
        ) -> _Frame:
            del api, endpoint
            offset = int(params["offset"])
            if offset == 0:
                records = [
                    {
                        "ts_code": f"FUND{index:06d}.SH",
                        "trade_date": "20260825",
                        "adj_factor": "1",
                    }
                    for index in range(2000)
                ]
            else:
                records = [
                    {
                        "ts_code": "FUND000000.SH",
                        "trade_date": "20260825",
                        "adj_factor": "1",
                    }
                ]
            return _Frame(fields, records)

    client = TushareClient(
        _DuplicateGateway(),
        TushareConfig(
            token="token",
            benchmark_indexes=("000300.SH",),
            max_attempts=1,
            retry_backoff_seconds=(),
        ),
    )
    request = client.requests("fund_adj", date(2026, 8, 25), date(2026, 8, 25))[0]
    client.login()

    with pytest.raises(ValueError, match="duplicate keys"):
        tuple(client.fetch("fund_adj", request))


def test_daily_rejects_response_at_provider_row_limit() -> None:
    class _LimitGateway(_Gateway):
        def call(
            self,
            api: object,
            endpoint: str,
            params: Mapping[str, JsonValue],
            fields: tuple[str, ...],
        ) -> _Frame:
            del api, endpoint, params
            return _Frame(fields, [{}] * 6000)

    client = TushareClient(
        _LimitGateway(),
        TushareConfig(
            token="token",
            benchmark_indexes=("000300.SH",),
            max_attempts=1,
            retry_backoff_seconds=(),
        ),
    )
    client.login()
    request = client.requests("daily", date(2026, 8, 25), date(2026, 8, 25))[0]

    with pytest.raises(ValueError, match="may be truncated at 6000"):
        tuple(client.fetch("daily", request))


class _FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_rate_limiter_evenly_spaces_requests_and_applies_dynamic_rate() -> None:
    fake_time = _FakeTime()
    rate = [480]
    limiter = TushareRateLimiter(
        lambda: rate[0],
        monotonic=fake_time.monotonic,
        sleeper=fake_time.sleep,
    )

    limiter.acquire()
    limiter.acquire()
    rate[0] = 240
    limiter.acquire()

    assert fake_time.sleeps == [pytest.approx(0.125), pytest.approx(0.25)]


def test_shared_rate_limiter_serializes_clients_and_threads() -> None:
    fake_time = _FakeTime()
    limiter = TushareRateLimiter(
        lambda: 480,
        monotonic=fake_time.monotonic,
        sleeper=fake_time.sleep,
    )
    config = TushareConfig(
        token="token",
        benchmark_indexes=("000300.SH",),
        max_attempts=1,
        retry_backoff_seconds=(),
    )
    clients = (
        TushareClient(_Gateway(), config, rate_limiter=limiter),
        TushareClient(_Gateway(), config, rate_limiter=limiter),
    )
    for client in clients:
        client.login()
        tuple(client.fetch("trade_cal", {"endpoint": "trade_cal"}))

    threads = [Thread(target=limiter.acquire) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert fake_time.sleeps == [pytest.approx(0.125)] * 3


def test_invalid_dynamic_rate_limit_fails_before_gateway_call() -> None:
    calls: list[str] = []

    class _RecordingGateway(_Gateway):
        def call(
            self,
            api: object,
            endpoint: str,
            params: Mapping[str, JsonValue],
            fields: tuple[str, ...],
        ) -> _Frame:
            del api, params
            calls.append(endpoint)
            return _Frame(fields)

    client = TushareClient(
        _RecordingGateway(),
        TushareConfig(
            token="token",
            benchmark_indexes=("000300.SH",),
            max_attempts=1,
            retry_backoff_seconds=(),
            rate_limit_provider=lambda: 0,
        ),
    )
    client.login()

    with pytest.raises(ValueError, match="1 through 10000"):
        tuple(client.fetch("trade_cal", {"endpoint": "trade_cal"}))

    assert calls == []


def test_every_retry_acquires_a_new_rate_limit_slot() -> None:
    class _RetryGateway(_Gateway):
        def __init__(self) -> None:
            self.calls = 0

        def call(
            self,
            api: object,
            endpoint: str,
            params: Mapping[str, JsonValue],
            fields: tuple[str, ...],
        ) -> _Frame:
            del api, endpoint, params
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("retry")
            return _Frame(fields)

    class _CountingLimiter(TushareRateLimiter):
        def __init__(self) -> None:
            super().__init__(lambda: 480)
            self.calls = 0

        def acquire(self) -> None:
            self.calls += 1

    gateway = _RetryGateway()
    limiter = _CountingLimiter()
    client = TushareClient(
        gateway,
        TushareConfig(
            token="token",
            benchmark_indexes=("000300.SH",),
            max_attempts=2,
            retry_backoff_seconds=(0.0,),
        ),
        sleeper=lambda _: None,
        rate_limiter=limiter,
    )
    client.login()

    tuple(client.fetch("trade_cal", {"endpoint": "trade_cal"}))

    assert gateway.calls == limiter.calls == 2
