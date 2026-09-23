# Graph token refresh is reactive on HTTP 401, not proactive on expiry

Azure AD returns `expires_in` with every token, so we could refresh a few
minutes ahead of expiry and never serve a doomed request. `get_headers`
discards that value, and it is public and widely called, so we chose to trigger
re-minting from a 401 response rather than change a public function's contract
in the same release that fixes a production bug.

## Consequences

One request per token lifetime is wasted: it 401s, triggers the re-mint, and is
retried. Because the headers dictionary is updated in place rather than rebound,
that is one wasted request per hour rather than one per call.

The reactive path is needed regardless of any future proactive one: clock skew
and revoked or invalidated tokens produce 401s that no expiry calculation
predicts. Proactive refresh would therefore be an addition to this mechanism,
not a replacement, and would require `get_headers` to surface `expires_in`.
