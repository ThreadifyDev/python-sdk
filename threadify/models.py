import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

# --- Constants ---

DEFAULT_CONNECT_TIMEOUT = 10.0  # seconds
DEFAULT_REQUEST_TIMEOUT = 10.0
DEFAULT_WAIT_TIMEOUT = 5.0
DEFAULT_MAX_IN_FLIGHT = 10
MIN_MAX_IN_FLIGHT = 1
MAX_MAX_IN_FLIGHT = 100
DEFAULT_PROCESSED_MAX_SIZE = 10_000

# Protocol actions
ACTION_CONNECT = "connect"
ACTION_START_THREAD = "startThread"
ACTION_JOIN_THREAD = "joinThread"
ACTION_RECORD_THREAD_EVENT = "recordThreadEvent"
ACTION_INVITE_PARTY = "inviteParty"
ACTION_ADD_REFS = "addRefs"
ACTION_THREAD_END = "threadEnd"
ACTION_CLOSE_THREAD = "closeThread"
ACTION_SUBSCRIBE = "subscribe"
ACTION_UNSUBSCRIBE = "unsubscribe"
ACTION_NOTIFICATION = "notification"
ACTION_NOTIFICATION_BATCH = "notification_batch"
ACTION_CLOSE_CONNECTION = "closeConnection"
ACTION_ACK_NOTIFICATION = "ack_notification"

# Protocol fields
FIELD_ACTION = "action"
FIELD_STATUS = "status"
FIELD_MESSAGE = "message"
FIELD_API_KEY = "apiKey"
FIELD_SERVICE_NAME = "serviceName"
FIELD_MAX_IN_FLIGHT = "maxInFlight"
FIELD_THREAD_ID = "threadId"
FIELD_STEP_NAME = "stepName"
FIELD_ROLE = "role"
FIELD_REFS = "refs"
FIELD_CONTRACT_NAME = "contractName"
FIELD_EVENT_TYPES = "eventTypes"
FIELD_NOTIFICATION = "notification"
FIELD_NOTIFICATIONS = "notifications"
FIELD_ACK_TOKEN = "ackToken"
FIELD_THREAD_TOKEN = "threadToken"
FIELD_NOTIFICATION_ID = "notificationId"
FIELD_NOTIFICATION_ID_ACK = "notification_id"
FIELD_THREAD_ID_ACK = "thread_id"
FIELD_PROCESSED = "processed"
FIELD_STEP_STATUS = "stepStatus"
FIELD_NOTIFICATION_TYPE = "notificationType"
FIELD_SOURCE = "source"
FIELD_CONTEXT = "context"
FIELD_FINISHED_AT = "finishedAt"
FIELD_STARTED_AT = "startedAt"
FIELD_IDEMPOTENCY_KEY = "idempotencyKey"
FIELD_IS_DUPLICATE = "isDuplicate"
FIELD_SUB_STEPS = "subSteps"
FIELD_THREADIFY_METADATA = "threadify_metadata"
FIELD_ACCESS_LEVEL = "accessLevel"
FIELD_EXPIRES_IN = "expiresIn"
FIELD_EXPIRES_AT = "expiresAt"
FIELD_REASON = "reason"
FIELD_THREAD_STATUS = "threadStatus"
FIELD_CLOSED_AT = "closedAt"
FIELD_COMPLETED_AT = "completedAt"
FIELD_CANCELLED_AT = "cancelledAt"
FIELD_DETAILS = "details"
FIELD_OWNER_ID = "ownerId"
FIELD_SEVERITY = "severity"
FIELD_TIMESTAMP = "timestamp"
FIELD_VIOLATION_TYPE = "violationType"

# Protocol status values
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_ERROR = "error"
STATUS_IN_PROGRESS = "in_progress"
STATUS_PASSED = "passed"
STATUS_VIOLATED = "violated"
STATUS_CANCELLED = "cancelled"
STATUS_COMPLETED = "completed"

# Protocol severity values
SEVERITY_INFO = "info"
SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"


# --- Enums ---


class StepStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class ValidationSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class ConnectOptions:
    """Configuration for connecting to the Threadify Engine."""

    service_name: str = ""
    ws_url: str = ""
    graphql_url: str = ""
    debug: bool = False
    max_in_flight: int = DEFAULT_MAX_IN_FLIGHT
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    logger: logging.Logger | None = None

    def with_defaults(self) -> "ConnectOptions":
        if not self.graphql_url and self.ws_url:
            self.graphql_url = derive_graphql_url(self.ws_url)
        if self.max_in_flight == 0:
            self.max_in_flight = DEFAULT_MAX_IN_FLIGHT
        if self.connect_timeout == 0:
            self.connect_timeout = DEFAULT_CONNECT_TIMEOUT
        return self

    def validate(self) -> None:
        if not self.ws_url or not self.ws_url.strip():
            raise ValueError("ws_url is required")
        if not (MIN_MAX_IN_FLIGHT <= self.max_in_flight <= MAX_MAX_IN_FLIGHT):
            raise ValueError(
                f"max_in_flight must be between {MIN_MAX_IN_FLIGHT} and {MAX_MAX_IN_FLIGHT}"
            )


@dataclass
class StepResult:
    """Result of recording a step event."""

    step_name: str
    thread_id: str
    status: str
    idempotency_key: str
    timestamp: str
    duplicate: bool = False


@dataclass
class SubStepData:
    """Data for a sub-step within a parent step."""

    name: str
    status: str = "success"
    payload: dict[str, Any] | None = None
    recorded_at: str = ""

    def __post_init__(self):
        if not self.recorded_at:
            self.recorded_at = now_iso()


@dataclass
class InviteOptions:
    """Options for inviting a party to join a thread."""

    role: str = ""
    access_level: str = "external"
    expires_in: str = "24h"


@dataclass
class InviteResponse:
    """Response from creating a party invitation."""

    token: str
    thread_id: str
    role: str
    access_level: str
    expires_at: str


@dataclass
class ThreadEndResponse:
    """Response from ending a thread."""

    thread_id: str
    status: str
    ended_at: str
    message: str = ""


@dataclass
class WaitOptions:
    """Options for waiting on a specific step notification."""

    timeout: float = DEFAULT_WAIT_TIMEOUT
    statuses: list[str] = field(default_factory=list)


@dataclass
class NotificationData:
    """Raw notification data from the server."""

    notification_id: str = ""
    thread_id: str = ""
    step_id: str = ""
    step_name: str = ""
    contract_name: str = ""
    status: str = ""
    step_status: str = ""
    severity: str = ""
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: str = ""
    violation_type: str = ""
    owner_id: str = ""
    source: str = ""
    notification_type: str = ""


@dataclass
class RefQuery:
    """Query parameters for fetching threads by reference."""

    ref_key: str = ""
    ref_value: str = ""
    status: str = ""
    started_after: str = ""
    started_before: str = ""
    limit: int = 50
    offset: int = 0


@dataclass
class CompleteDataOptions:
    """Options for ArchivedThread.get_complete_data."""

    step_history_limit: int = 50
    validation_limit: int = 10
    step_name: str = ""
    idempotency_key: str = ""
    status: str = ""


@dataclass
class HistoryQueryOptions:
    """Filter options for step history queries."""

    limit: int = 100
    offset: int = 0
    start_at: str = ""
    end_at: str = ""
    activity_type: str = ""
    actor: str = ""


def derive_graphql_url(ws_url: str) -> str:
    """Convert a WebSocket URL to its corresponding GraphQL URL."""
    out = ws_url.replace("ws://", "http://", 1)
    out = out.replace("wss://", "https://", 1)
    return out.replace("/threads", "/graphql", 1)


def require_non_empty(name: str, value: str) -> None:
    """Raise ValueError if value is empty or whitespace."""
    if not value or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def first_non_empty(*values: str) -> str:
    """Return the first non-empty, non-whitespace string."""
    for v in values:
        if v and v.strip():
            return v
    return ""


def now_iso() -> str:
    """Return the current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()
