# WCP Library

Shared utilities consumed by WCP's internal automation projects (Kestra flows,
RPA processes). Because every consumer inherits this library's behaviour, its
vocabulary is deliberately narrow: the same word must mean the same thing in
`graph/`, `credentials/`, and `sql/`.

## Language

### Microsoft Graph authentication

**Graph Credentials**:
An app registration's identity (client id, secret, tenant) held for the life of
a process, from which access tokens can be issued on demand.
_Avoid_: Session, connection, client

**Token**:
A bearer credential issued by Azure AD from Graph Credentials, valid for
approximately one hour.
_Avoid_: Auth, key, secret

**Mint** / **Re-mint**:
To obtain a fresh Token from Graph Credentials. "Re-mint" is specifically
replacing a Token whose lifetime has ended.
_Avoid_: Renew, refresh, rotate

### Webhook subscriptions

**Subscription**:
A Graph webhook registration that delivers change notifications to a URL, and
which expires on its own schedule independent of any Token.

**Renew**:
To extend a Subscription's expiry. Reserved exclusively for Subscriptions —
never used of Tokens, which are re-minted rather than renewed.
_Avoid_: Refresh, extend, re-mint

### Graph failure kinds

These three were historically conflated, which is what allowed an expired Token
to masquerade as a misconfiguration. They are distinct concepts with distinct
handling:

**Token expiry**:
Graph refuses a request because the Token's lifetime has ended. The same request
with a freshly minted Token succeeds. Surfaces as HTTP 401.

**Permission denial**:
Graph refuses a request because the app registration lacks the required
permission. A freshly minted Token is refused identically. Surfaces as HTTP 403.

**Throttling**:
Graph defers a request it is willing to serve later. Resolved by waiting, not by
minting. Surfaces as HTTP 429/503/504.

### Credential storage

**Vault**:
Password State, the system of record for credentials. Referenced by numeric
credential id, never by copying secrets into a consuming project.
_Avoid_: Password store, secret manager, PasswordState (as two words)
