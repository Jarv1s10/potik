# A rule that raises asks the next rule

Exception rules are asked in order, like `except` clauses. In Python, an exception raised in an `except` clause propagates and skips the later clauses. `ExceptionMiddleware` did the same: a rule that raised, e.g. a recovery rule whose database was down, skipped every later rule, so a catch-all `dead_letter()` never ran. The partition then stopped with the rule's exception, and the handler's exception survived only as its `__context__`, so the partition's status, `error.type`, and spans named the rule's failure, not the record's.

A rule that raises now counts as returning None. `ExceptionMiddleware` logs the rule's exception, with its traceback and the record's topic, partition, and offset, and asks the next rule. An exception that no rule handles propagates as the handler raised it. Cancellations are not `Exception`s, so they still propagate at once.

We rejected propagating the rule's exception with the handler's attached, e.g. as an `ExceptionGroup`, because later rules would still not run, and the group would change what the partition's status and telemetry report. A broken rule now reaches the catch-all rule silently except for the log, which we accept: the catch-all rule is the app's own choice for any exception it does not expect, and a rule's failure is one of them.
