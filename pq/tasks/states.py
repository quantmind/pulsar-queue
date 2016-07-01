from enum import Enum


class StatusType(Enum):
    SUCCESS = 1
    FAILURE = 2
    REVOKED = 3
    RETRY = 4
    STARTED = 5
    QUEUED = 6


# TODO remove these 6 lines after rest of pq code has been udpated to use enum
SUCCESS = StatusType.SUCCESS.value
FAILURE = StatusType.FAILURE.value
REVOKED = StatusType.REVOKED.value
RETRY = StatusType.RETRY.value
STARTED = StatusType.STARTED.value
QUEUED = StatusType.QUEUED.value

FULL_RUN_STATES = frozenset([
    StatusType.SUCCESS.value,
    StatusType.FAILURE.value
])
READY_STATES = frozenset([
    StatusType.SUCCESS.value,
    StatusType.FAILURE.value,
    StatusType.REVOKED.value
])
EXCEPTION_STATES = frozenset([
    StatusType.FAILURE.value,
    StatusType.REVOKED.value
])
UNREADY_STATES = frozenset([
    StatusType.QUEUED.value,
    StatusType.STARTED.value,
    StatusType.RETRY.value
])


def status_string(status):
    try:
        code = StatusType(status).name
    except ValueError:
        code = 'UNKNOWN'

    return code
