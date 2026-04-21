"""Round-2: exercise the parent_id SQL fix-up on a deep tree."""

from __future__ import annotations

from typing import Iterator, List, Tuple

import pytest

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner import orchestrator as orch_mod
from duplicator_search_destroy.scanner.files import WalkedFile, WalkedFolder
from duplicator_search_destroy.scanner.orchestrator import Orchestrator


def _tree_walk(host, share, *, max_depth=64, cancel=None, on_progress=None, subpath=""):
    """Yield root + a\\b\\c hierarchy with files at the leaves."""
    folders: List[Tuple[str, int, str | None]] = [
        ("", 0, None),
        ("a", 1, ""),
        ("a\\b", 2, "a"),
        ("a\\b\\c", 3, "a\\b"),
    ]
    for rel, depth, parent in folders:
        folder = WalkedFolder(
            relative_path=rel,
            name=rel.split("\\")[-1] if rel else share,
            depth=depth,
            parent_rel_path=parent,
            file_count=1 if rel == "a\\b\\c" else 0,
            total_size=10 if rel == "a\\b\\c" else 0,
            created_at=0.0,
            modified_at=0.0,
        )

        def files_for(this_rel=rel):
            if this_rel == "a\\b\\c":
                yield WalkedFile(
                    relative_path="a\\b\\c\\leaf.txt",
                    name="leaf.txt",
                    extension="txt",
                    size=10,
                    created_at=0.0,
                    modified_at=0.0,
                    accessed_at=0.0,
                    folder_rel_path="a\\b\\c",
                )

        yield folder, files_for()


@pytest.fixture()
def patched(monkeypatch):
    monkeypatch.setattr(orch_mod, "walk_share", _tree_walk)
    monkeypatch.setattr(orch_mod, "register_session", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod, "unregister_session", lambda *a, **k: None)


def test_parent_id_resolution_deep(tmp_db: Database, patched):
    hid = tmp_db.upsert_host("10.0.0.1", hostname="srv1", status="online")
    tmp_db.upsert_share(hid, "DATA", accessible=True)
    Orchestrator(tmp_db).scan_files()

    rows = {
        r["relative_path"]: r
        for r in tmp_db.query("SELECT relative_path, parent_id, id, depth FROM folders")
    }
    assert rows[""]["parent_id"] is None
    assert rows["a"]["parent_id"] == rows[""]["id"]
    assert rows["a\\b"]["parent_id"] == rows["a"]["id"]
    assert rows["a\\b\\c"]["parent_id"] == rows["a\\b"]["id"]
