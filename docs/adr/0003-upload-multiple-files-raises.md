# upload_multiple_files raises instead of reporting per-file errors

`upload_multiple_files` caught `requests.RequestException` per file and returned
it as a `{"filename": ..., "error": ...}` entry, so a batch in which every
upload failed returned normally. That contradicted the error contract documented
for 1.13+ (public helpers raise on final failure rather than returning
`None`/`[]`/`False`), and it is how an expired token silently defeated a
consumer's resume mechanism in production: 120 files reported as staged, none
staged, and the next run re-downloaded all of them. From 1.15.0 it raises an
`ExceptionGroup` of the failures.

## Consequences

This is the only breaking change in 1.15.0. The function had no callers in this
library, no test, and no wiki entry when the decision was taken, which is why it
was changed outright rather than given an opt-in flag: a safe default nobody
opts into would leave the silent path as the one callers get by accident.

Successful responses are discarded when any file fails. Uploads are idempotent
under `conflict_behavior="replace"`, so re-running the batch costs bandwidth
rather than correctness.

A future maintainer may be tempted to restore the swallowing to avoid breaking a
consumer. Don't: the silence is the defect, not the contract.

## History

Reversed in 1.15.1, which restored the per-file error entries, and restored in
1.15.3. Both 1.15.1 and 1.15.2 are on PyPI with the swallowing behaviour, so a
consumer pinned to either does not have the contract this ADR describes.

The restoration also widened what is collected: every exception, not only
`requests.RequestException` and `TypeError`. An exception left uncaught inside
a worker thread never reaches the caller at all — it prints a traceback and
leaves that file's result entry empty — so a narrow `except` reintroduces the
silent failure through a different door.
