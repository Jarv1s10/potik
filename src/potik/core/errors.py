"""The errors that the core raises, and `PermanentError`, which tells `retry()` rules not to retry."""

__all__ = [
    "InvalidOffsetError",
    "InvalidOutputError",
    "PartitionsLostError",
    "PermanentError",
    "PotikError",
]


class PotikError(Exception):
    """The base of the errors that potik raises.

    Errors of the Kafka client stay `KafkaException`s, and invalid arguments raise `ValueError` or `TypeError`.
    """


class PermanentError(Exception):
    """Marks a failure that would repeat if the handler ran again, e.g. a value that does not decode.

    `retry()` and `retry_forever()` rules pass it to the next rule instead of retrying. Subclass it for such failures of your own. It does not derive from `PotikError`, so `except PotikError` does not catch your subclasses.
    """


class InvalidOutputError(PotikError, PermanentError):
    """Raised when a handler returns something other than a list or tuple of `ProducerRecord`s.

    A core handler that returns a single record raises it, for example. It comes from the handler call, so exception rules see it like any other handler exception. To stop the partition instead of letting a catch-all rule handle it, put `on(InvalidOutputError, reraise())` before that rule.
    """


class InvalidOffsetError(PotikError):
    """Raised when an offset policy refuses a partition, e.g. `resume_or_fail`, or returns an invalid offset.

    A valid offset is the committed one, or within the partition's log. The error fails the run.
    """


class PartitionsLostError(PotikError):
    """Raised when the consumer loses partitions without a revocation, so another member may already own them.

    It happens e.g. when the consumer misses its heartbeats. The error fails the run, which commits nothing more.
    """
