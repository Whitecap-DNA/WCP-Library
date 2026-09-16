"""Tests for the wiki sync script in .github/scripts/sync_wiki.py.

The script is repo tooling rather than part of the installed package, so it
is loaded by path. All filesystem work happens under pytest's tmp_path.

The behaviour under test that actually matters is the filename mapping: wiki
page files use a U+2010 non-breaking hyphen (``Helper-‐-Emailing.md``) and every
cross-link inside the docs encodes it as ``%E2%80%90``, while several repo
filenames have drifted to a plain ASCII hyphen. Mapping a drifted name onto a
fresh page would duplicate the page and orphan every link pointing at it.
"""
import importlib.util
from pathlib import Path

import pytest

DASH = "‐"  # U+2010 HYPHEN, the character the wiki filenames use
SCRIPT = (
    Path(__file__).resolve().parents[1] / ".github" / "scripts" / "sync_wiki.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("sync_wiki", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sync_wiki = _load()


# --------------------------- Helpers --------------------------- #


def _tree(tmp_path, docs, wiki):
    """Build a repo-docs dir and a wiki clone dir from {name: text} maps."""
    docs_dir = tmp_path / "docs" / "Wiki Docs"
    wiki_dir = tmp_path / "wiki"
    docs_dir.mkdir(parents=True)
    wiki_dir.mkdir(parents=True)
    for name, text in docs.items():
        (docs_dir / name).write_text(text, encoding="utf-8", newline="\n")
    for name, text in wiki.items():
        (wiki_dir / name).write_text(text, encoding="utf-8", newline="\r\n")
    return docs_dir, wiki_dir


# --------------------------- wiki_filename --------------------------- #


class TestWikiFilename:
    """Canonical wiki filename for a repo doc name."""

    def test_ascii_separator_is_canonicalised_to_u2010(self):
        assert (
            sync_wiki.wiki_filename("Helper - Browser Automation")
            == f"Helper-{DASH}-Browser-Automation.md"
        )

    def test_existing_u2010_separator_is_preserved(self):
        assert (
            sync_wiki.wiki_filename(f"Helper {DASH} Emailing")
            == f"Helper-{DASH}-Emailing.md"
        )

    def test_single_word_name(self):
        assert sync_wiki.wiki_filename("Home") == "Home.md"

    def test_spaces_become_hyphens_without_a_separator(self):
        assert sync_wiki.wiki_filename("#Extra Uses") == "#Extra-Uses.md"

    def test_multi_word_halves_keep_their_internal_spaces_as_hyphens(self):
        assert (
            sync_wiki.wiki_filename("Connection - FTP SFTP")
            == f"Connection-{DASH}-FTP-SFTP.md"
        )


# --------------------------- resolve_page --------------------------- #


class TestResolvePage:
    """Matching a repo doc onto an existing wiki page."""

    def test_ascii_doc_name_matches_u2010_wiki_page(self):
        existing = [f"Helper-{DASH}-Microsoft-Graph-API.md"]
        assert (
            sync_wiki.resolve_page("Helper - Microsoft Graph API", existing)
            == f"Helper-{DASH}-Microsoft-Graph-API.md"
        )

    def test_u2010_doc_name_matches_ascii_wiki_page(self):
        existing = ["Helper---Logging.md"]
        assert (
            sync_wiki.resolve_page(f"Helper {DASH} Logging", existing)
            == "Helper---Logging.md"
        )

    def test_exact_match_wins(self):
        existing = ["Home.md", "Home-Page.md"]
        assert sync_wiki.resolve_page("Home", existing) == "Home.md"

    def test_returns_none_when_nothing_matches(self):
        assert sync_wiki.resolve_page("Brand New Page", ["Home.md"]) is None


# --------------------------- sync --------------------------- #


class TestSync:
    """End-to-end behaviour against a temporary wiki clone."""

    def test_updates_existing_page_matched_across_hyphen_spellings(self, tmp_path):
        docs, wiki = _tree(
            tmp_path,
            {"Helper - Microsoft Graph API": "new content\n"},
            {f"Helper-{DASH}-Microsoft-Graph-API.md": "old content\n"},
        )
        result = sync_wiki.sync(docs, wiki)

        target = wiki / f"Helper-{DASH}-Microsoft-Graph-API.md"
        assert target.read_text(encoding="utf-8").replace("\r\n", "\n") == "new content\n"
        assert result.updated == [f"Helper-{DASH}-Microsoft-Graph-API.md"]
        assert result.created == []

    def test_does_not_create_a_duplicate_page_for_a_drifted_name(self, tmp_path):
        docs, wiki = _tree(
            tmp_path,
            {"Helper - Microsoft Graph API": "new content\n"},
            {f"Helper-{DASH}-Microsoft-Graph-API.md": "old content\n"},
        )
        sync_wiki.sync(docs, wiki)

        assert sorted(p.name for p in wiki.glob("*.md")) == [
            f"Helper-{DASH}-Microsoft-Graph-API.md"
        ]

    def test_writes_crlf_to_match_the_wiki(self, tmp_path):
        docs, wiki = _tree(
            tmp_path,
            {"Home": "line one\nline two\n"},
            {"Home.md": "old\n"},
        )
        sync_wiki.sync(docs, wiki)

        assert b"\r\n" in (wiki / "Home.md").read_bytes()
        assert b"\n\n" not in (wiki / "Home.md").read_bytes()

    def test_creates_a_missing_page_under_the_canonical_name(self, tmp_path):
        docs, wiki = _tree(
            tmp_path,
            {"Helper - Brand New": "fresh\n"},
            {"Home.md": "home\n"},
        )
        result = sync_wiki.sync(docs, wiki)

        created = wiki / f"Helper-{DASH}-Brand-New.md"
        assert created.exists()
        assert result.created == [f"Helper-{DASH}-Brand-New.md"]

    def test_identical_content_is_not_rewritten(self, tmp_path):
        docs, wiki = _tree(
            tmp_path,
            {"Home": "same\n"},
            {"Home.md": "same\n"},
        )
        result = sync_wiki.sync(docs, wiki)

        assert result.updated == []
        assert result.created == []
        assert result.unchanged == ["Home.md"]

    def test_is_idempotent(self, tmp_path):
        docs, wiki = _tree(
            tmp_path,
            {"Home": "content\n"},
            {"Home.md": "stale\n"},
        )
        first = sync_wiki.sync(docs, wiki)
        second = sync_wiki.sync(docs, wiki)

        assert first.updated == ["Home.md"]
        assert second.updated == []
        assert second.unchanged == ["Home.md"]

    def test_orphan_wiki_pages_are_reported_but_not_deleted(self, tmp_path):
        docs, wiki = _tree(
            tmp_path,
            {"Home": "home\n"},
            {"Home.md": "home\n", "Retired-Page.md": "still here\n"},
        )
        result = sync_wiki.sync(docs, wiki)

        assert (wiki / "Retired-Page.md").exists()
        assert result.orphaned == ["Retired-Page.md"]

    def test_ignores_dotfiles_in_the_docs_directory(self, tmp_path):
        docs, wiki = _tree(tmp_path, {"Home": "home\n"}, {"Home.md": "home\n"})
        (docs / ".DS_Store").write_bytes(b"\x00")

        result = sync_wiki.sync(docs, wiki)

        assert result.created == []
        assert not (wiki / ".DS_Store.md").exists()

    def test_missing_docs_directory_is_an_error(self, tmp_path):
        wiki = tmp_path / "wiki"
        wiki.mkdir()
        with pytest.raises(FileNotFoundError):
            sync_wiki.sync(tmp_path / "nope", wiki)
