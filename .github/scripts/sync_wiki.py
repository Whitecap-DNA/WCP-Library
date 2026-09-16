"""Copy ``docs/Wiki Docs/`` onto the GitHub wiki's git repository.

The wiki is a separate git repo (``<repo>.wiki.git``) with no built-in link to
the main repository, so it has to be written explicitly. This script is the
copy step; the surrounding workflow does the checkout, commit and push.

Page filenames are the subtle part. Wiki pages use a U+2010 non-breaking
hyphen as the title separator (``Helper-‐-Emailing.md``) and every cross-link
inside the docs encodes it as ``%E2%80%90``, while several filenames under
``docs/Wiki Docs/`` have since drifted to a plain ASCII hyphen. Mapping a
drifted name straight through would create a second page under a near-identical
name and orphan every link pointing at the original, so an existing page is
matched with the hyphen character normalised away and written in place.

The repo is the single source of truth: pages are overwritten unconditionally,
without checking whether someone edited them in the browser. Pages no longer
present in the repo are reported, never deleted, because external links may
still point at them.
"""
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

DASH = "‐"  # U+2010 HYPHEN, the separator the wiki filenames use
ASCII_SEPARATOR = " - "
WIKI_SEPARATOR = f" {DASH} "


@dataclass
class SyncResult:
    """Outcome of a sync, as wiki page filenames.

    :ivar updated: Existing pages whose content changed.
    :ivar created: Pages that did not exist on the wiki before.
    :ivar unchanged: Pages already identical to the repo.
    :ivar orphaned: Wiki pages with no counterpart in the repo. Left in place.
    """

    updated: list[str] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    orphaned: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        """Whether anything was written.

        :return: True if any page was created or updated.
        """
        return bool(self.updated or self.created)


def _hyphen_key(name: str) -> str:
    """Collapse a page name to a form that ignores which hyphen character is used.

    :param name: A repo doc name or a wiki page stem.
    :return: The name with spaces and U+2010 both reduced to ASCII hyphens.
    """
    return name.replace(" ", "-").replace(DASH, "-")


def wiki_filename(doc_name: str) -> str:
    """Canonical wiki filename for a repo doc name.

    Used when no page exists yet. An ASCII title separator is canonicalised to
    U+2010 so a new page matches the convention the docs' cross-links rely on.

    :param doc_name: Filename from ``docs/Wiki Docs/`` (no extension).
    :return: The wiki page filename, including the ``.md`` extension.
    """
    return doc_name.replace(ASCII_SEPARATOR, WIKI_SEPARATOR).replace(" ", "-") + ".md"


def resolve_page(doc_name: str, existing: Iterable[str]) -> str | None:
    """Find the existing wiki page a repo doc corresponds to.

    Prefers an exact filename match, then falls back to comparing with the
    hyphen character normalised away, so a doc named with an ASCII hyphen still
    resolves to a page named with U+2010 and vice versa.

    :param doc_name: Filename from ``docs/Wiki Docs/`` (no extension).
    :param existing: Wiki page filenames currently in the clone.
    :return: The matching wiki filename, or None if the page does not exist.
    """
    existing = list(existing)

    exact = wiki_filename(doc_name)
    if exact in existing:
        return exact

    target = _hyphen_key(doc_name)
    for name in existing:
        stem = name[:-3] if name.endswith(".md") else name
        if _hyphen_key(stem) == target:
            return name
    return None


def sync(docs_dir: Path, wiki_dir: Path) -> SyncResult:
    """Copy every repo doc onto its wiki page.

    Content is compared with line endings normalised, so a page is rewritten
    only when its text actually differs. Writes use CRLF to match the endings
    the wiki already uses, keeping diffs to real content changes.

    :param docs_dir: The ``docs/Wiki Docs/`` directory.
    :param wiki_dir: A checkout of the wiki repository.
    :return: A :class:`SyncResult` describing what changed.
    :raises FileNotFoundError: If either directory does not exist.
    """
    docs_dir, wiki_dir = Path(docs_dir), Path(wiki_dir)
    if not docs_dir.is_dir():
        raise FileNotFoundError(f"docs directory not found: {docs_dir}")
    if not wiki_dir.is_dir():
        raise FileNotFoundError(f"wiki checkout not found: {wiki_dir}")

    existing = sorted(p.name for p in wiki_dir.glob("*.md"))
    result = SyncResult()
    touched: set[str] = set()

    docs = sorted(
        (p for p in docs_dir.iterdir() if p.is_file() and not p.name.startswith(".")),
        key=lambda p: p.name,
    )
    for doc in docs:
        target_name = resolve_page(doc.name, existing)
        is_new = target_name is None
        if target_name is None:
            target_name = wiki_filename(doc.name)
        touched.add(target_name)

        target = wiki_dir / target_name
        new_text = doc.read_text(encoding="utf-8").replace("\r\n", "\n")
        old_text = (
            target.read_text(encoding="utf-8").replace("\r\n", "\n")
            if target.exists()
            else None
        )

        if old_text == new_text:
            result.unchanged.append(target_name)
            continue

        target.write_text(new_text, encoding="utf-8", newline="\r\n")
        (result.created if is_new else result.updated).append(target_name)

    result.orphaned = [name for name in existing if name not in touched]
    return result


def main(argv: list[str] | None = None) -> int:
    """Run the sync and print a summary.

    :param argv: ``[docs_dir, wiki_dir]``; defaults to the repo layout.
    :return: Process exit status.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    docs_dir = Path(args[0]) if args else Path("docs/Wiki Docs")
    wiki_dir = Path(args[1]) if len(args) > 1 else Path("wiki")

    try:
        result = sync(docs_dir, wiki_dir)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    for label, names in (
        ("created", result.created),
        ("updated", result.updated),
        ("unchanged", result.unchanged),
    ):
        print(f"{label}: {len(names)}")
        for name in names:
            print(f"  {name}")

    if result.orphaned:
        print(
            f"\nnote: {len(result.orphaned)} wiki page(s) have no counterpart in "
            f"{docs_dir} and were left untouched:"
        )
        for name in result.orphaned:
            print(f"  {name}")

    print(f"\nchanged: {result.changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
