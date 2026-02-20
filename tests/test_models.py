import pytest

from threadify.models import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_MAX_IN_FLIGHT,
    ConnectOptions,
    StepResult,
    StepStatus,
    ValidationSeverity,
    derive_graphql_url,
    first_non_empty,
    now_iso,
    require_non_empty,
)


class TestDeriveGraphQLURL:
    def test_wss_to_https(self):
        assert (
            derive_graphql_url("wss://api.example.com/threads") == "https://api.example.com/graphql"
        )

    def test_ws_to_http(self):
        assert derive_graphql_url("ws://localhost:8080/threads") == "http://localhost:8080/graphql"

    def test_no_replacement_needed(self):
        url = "https://already-http.com/api"
        assert derive_graphql_url(url) == url

    def test_empty_string(self):
        assert derive_graphql_url("") == ""


class TestRequireNonEmpty:
    def test_valid(self):
        require_non_empty("test", "hello")  # Should not raise.

    def test_empty(self):
        with pytest.raises(ValueError, match="test"):
            require_non_empty("test", "")

    def test_whitespace(self):
        with pytest.raises(ValueError, match="field"):
            require_non_empty("field", "   ")

    def test_tab(self):
        with pytest.raises(ValueError, match="x"):
            require_non_empty("x", "\t")


class TestFirstNonEmpty:
    def test_first_valid(self):
        assert first_non_empty("hello", "world") == "hello"

    def test_skip_empty(self):
        assert first_non_empty("", "world") == "world"

    def test_skip_whitespace(self):
        assert first_non_empty("   ", "world") == "world"

    def test_all_empty(self):
        assert first_non_empty("", "", "   ") == ""

    def test_no_values(self):
        assert first_non_empty() == ""


class TestNowISO:
    def test_returns_string(self):
        result = now_iso()
        assert isinstance(result, str)
        assert len(result) > 0
        assert "T" in result  # ISO format has T separator.


class TestConnectOptions:
    def test_defaults(self):
        opts = ConnectOptions()
        opts.with_defaults()
        assert opts.ws_url == ""
        assert opts.max_in_flight == DEFAULT_MAX_IN_FLIGHT
        assert opts.connect_timeout == DEFAULT_CONNECT_TIMEOUT
        assert opts.graphql_url == ""

    def test_preserves_custom(self):
        opts = ConnectOptions(
            ws_url="wss://custom.com/threads",
            max_in_flight=50,
            connect_timeout=30.0,
        )
        opts.with_defaults()
        assert opts.ws_url == "wss://custom.com/threads"
        assert opts.max_in_flight == 50
        assert opts.connect_timeout == 30.0

    def test_validate_valid(self):
        opts = ConnectOptions(ws_url="wss://custom.com/threads", max_in_flight=1)
        opts.validate()  # Should not raise.

        opts2 = ConnectOptions(ws_url="wss://custom.com/threads", max_in_flight=100)
        opts2.validate()  # Should not raise.

    def test_validate_requires_ws_url(self):
        opts = ConnectOptions(ws_url="")
        with pytest.raises(ValueError, match="ws_url is required"):
            opts.validate()

    def test_validate_too_low(self):
        opts = ConnectOptions(ws_url="wss://custom.com/threads", max_in_flight=0)
        with pytest.raises(ValueError, match="max_in_flight"):
            opts.validate()

    def test_validate_too_high(self):
        opts = ConnectOptions(ws_url="wss://custom.com/threads", max_in_flight=101)
        with pytest.raises(ValueError, match="max_in_flight"):
            opts.validate()

    def test_validate_negative(self):
        opts = ConnectOptions(ws_url="wss://custom.com/threads", max_in_flight=-1)
        with pytest.raises(ValueError, match="max_in_flight"):
            opts.validate()


class TestStepStatus:
    def test_values(self):
        assert StepStatus.SUCCESS.value == "success"
        assert StepStatus.FAILED.value == "failed"
        assert StepStatus.ERROR.value == "error"
        assert StepStatus.IN_PROGRESS.value == "in_progress"
        assert StepStatus.SKIPPED.value == "skipped"


class TestValidationSeverity:
    def test_values(self):
        assert ValidationSeverity.INFO.value == "info"
        assert ValidationSeverity.WARNING.value == "warning"
        assert ValidationSeverity.CRITICAL.value == "critical"


class TestStepResult:
    def test_creation(self):
        result = StepResult(
            step_name="order_placed",
            thread_id="t-1",
            status="success",
            idempotency_key="abc123",
            timestamp="2026-01-01T00:00:00Z",
        )
        assert result.step_name == "order_placed"
        assert result.duplicate is False

    def test_duplicate(self):
        result = StepResult(
            step_name="x",
            thread_id="t",
            status="success",
            idempotency_key="k",
            timestamp="t",
            duplicate=True,
        )
        assert result.duplicate is True
