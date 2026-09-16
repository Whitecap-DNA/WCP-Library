# WCP-Library

## Description
Commonly used functions for the D&A team.

## Installation
`pip install WCP-Library`

Requires Python 3.12 or newer.

## Modules

| Module | Purpose |
| --- | --- |
| `wcp_library` | `APPLICATION_PATH` for deployment-independent paths, and `divide_chunks` for batching a list. |
| `wcp_library.credentials` | Password Vault credential managers — API, FTP/SFTP, Internet, Oracle, Postgres, and Windows — each with a synchronous and an asynchronous flavour. |
| `wcp_library.sql` | Oracle and Postgres connection classes, sync and async, with optional connection pooling, explicit transactions, and dataframe import/upsert helpers. |
| `wcp_library.ftp` | `FTP` and `SFTP` clients for listing, downloading, and uploading files. |
| `wcp_library.graph` | Microsoft Graph helpers: authentication, SharePoint sites/files/lists, Outlook mail and attachments, and webhook subscription lifecycle management. |
| `wcp_library.browser_automation` | Selenium sessions for Chrome, Firefox, and Edge, plus locator-based and `WebElement`-based interaction helpers. |
| `wcp_library.emailing` | `MailServer` for sending mail through SMTP2GO, including a styled HTML error report. |
| `wcp_library.informatica` | `InformaticaSession` for running and monitoring Informatica Cloud tasks. |
| `wcp_library.logging` | `create_log` for standard file and console logging setup. |
| `wcp_library.time` | Timezone-aware current time, timezone conversion, and UTC/local timestamp helpers. |
| `wcp_library.retry` | Tenacity retry strategies shared by the SQL, Graph, and browser automation modules. |

## Usage
See [Wiki](https://github.com/Whitecap-DNA/WCP-Library/wiki/) for more information.

## Development

Install dependencies and run the test suite with Poetry:

```
poetry install
poetry run pytest
```

## Authors and acknowledgment
Mitch Petersen

## Stakeholders
D&A Developers

## Project status
In Development
