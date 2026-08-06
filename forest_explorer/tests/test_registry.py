"""Contract tests for the product registry.

These check the registry is internally coherent and safe to resolve. They do not
need any data present, so they run in a code-only checkout.

    python -m pytest forest_explorer/tests/test_registry.py
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY = REPO_ROOT / "forest_explorer" / "registry" / "products.yaml"

VALID_KINDS = {"raw", "intermediate", "final", "analysis", "qa", "lookup"}
VALID_LIFECYCLE = {"active", "planned", "experimental", "deprecated", "retired"}
VALID_FORMATS = {"parquet_file", "parquet_dataset", "gpkg_layer", "csv", "csv_glob"}
VALID_ACCESS = {"catalog_only", "prepared_download", "filtered_extract",
                "guided_combined"}
VALID_REVIEW = {"certified", "constrained", "domain_review_required",
                "blocked_by_defect", "not_reviewed"}

REQUIRED_FIELDS = {"id", "title", "family", "kind", "lifecycle", "path", "format",
                   "one_row_is", "access_mode", "review_status"}

# The project's rule: a product whose meaning is unsettled is described, not served.
REVIEW_ALLOWS_ACCESS = {
    "certified": None,                       # any access mode
    "constrained": None,                     # any, once its constraints are shown
    "domain_review_required": {"catalog_only"},
    "blocked_by_defect": {"catalog_only"},
    "not_reviewed": {"catalog_only"},
}


@pytest.fixture(scope="module")
def registry() -> dict:
    with open(REGISTRY, encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def products(registry) -> list[dict]:
    return registry["products"]


@pytest.fixture(scope="module")
def grains(registry) -> dict:
    return registry["grains"]


def test_every_product_has_the_required_fields(products):
    for p in products:
        missing = REQUIRED_FIELDS - set(p)
        assert not missing, f"{p.get('id', '<no id>')} is missing {sorted(missing)}"


def test_product_ids_are_unique(products):
    ids = [p["id"] for p in products]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    assert not dupes, f"duplicate product ids: {dupes}"


def test_enumerated_fields_use_known_values(products):
    for p in products:
        assert p["kind"] in VALID_KINDS, f"{p['id']}: kind={p['kind']!r}"
        assert p["lifecycle"] in VALID_LIFECYCLE, f"{p['id']}: {p['lifecycle']!r}"
        assert p["format"] in VALID_FORMATS, f"{p['id']}: format={p['format']!r}"
        assert p["access_mode"] in VALID_ACCESS, f"{p['id']}: {p['access_mode']!r}"
        assert p["review_status"] in VALID_REVIEW, f"{p['id']}: {p['review_status']!r}"
        intended = p.get("intended_access_mode")
        assert intended is None or intended in VALID_ACCESS, (
            f"{p['id']}: intended_access_mode={intended!r}"
        )


def test_grain_ids_resolve(products, grains):
    for p in products:
        gid = p.get("grain_id")
        if gid is not None:
            assert gid in grains, f"{p['id']} points at unknown grain {gid!r}"


def test_grain_parents_resolve_and_do_not_loop(grains):
    for gid, g in grains.items():
        seen, cur = [gid], g.get("parent_grain_id")
        while cur is not None:
            assert cur in grains, f"grain {gid!r} has unknown parent {cur!r}"
            assert cur not in seen, f"grain hierarchy loops: {seen + [cur]}"
            seen.append(cur)
            cur = grains[cur].get("parent_grain_id")


def test_product_keys_match_their_declared_grain(products, grains):
    """A product cannot claim a grain and then key itself differently."""
    for p in products:
        gid = p.get("grain_id")
        if gid is not None:
            assert list(p["keys"]) == list(grains[gid]["keys"]), (
                f"{p['id']} claims grain {gid!r} but its keys differ:\n"
                f"  product: {p['keys']}\n  grain:   {grains[gid]['keys']}"
            )


def test_products_with_keys_declare_a_grain(products):
    for p in products:
        if p.get("keys"):
            assert p.get("grain_id"), (
                f"{p['id']} declares keys but no grain_id; add it to the grains block"
            )


def test_derived_from_references_resolve(products):
    ids = {p["id"] for p in products}
    for p in products:
        parent = p.get("derived_from")
        if parent is not None:
            assert parent in ids, f"{p['id']} derives from unknown product {parent!r}"


def test_nothing_derives_from_itself(products):
    for p in products:
        assert p.get("derived_from") != p["id"], f"{p['id']} derives from itself"


def test_every_family_is_declared(registry, products):
    known = set(registry.get("families", {}))
    for p in products:
        assert p["family"] in known, f"{p['id']} uses undeclared family {p['family']!r}"


def test_no_product_still_uses_the_old_parent_grain_field(products):
    """`parent_grain` conflated grain hierarchy with production lineage.

    It is now `grain_id` (which grain a row sits at, hierarchy declared once in
    the grains block) and `derived_from` (which product built this one).
    """
    for p in products:
        assert "parent_grain" not in p, (
            f"{p['id']} still uses parent_grain; split it into grain_id and "
            "derived_from"
        )


def test_gpkg_layer_products_name_their_layer(products):
    for p in products:
        if p["format"] == "gpkg_layer":
            assert p.get("gpkg_layer"), f"{p['id']} must declare gpkg_layer"


def test_paths_are_relative_and_contain_no_machine_specific_parts(products):
    """A registry path must resolve inside a configured data root, always."""
    for p in products:
        path = p["path"]
        assert not path.startswith("/"), f"{p['id']}: absolute path {path!r}"
        assert ":" not in path, f"{p['id']}: drive letter or scheme in {path!r}"
        assert "\\" not in path, f"{p['id']}: backslash in {path!r}; use POSIX"
        assert ".." not in Path(path).parts, f"{p['id']}: traversal in {path!r}"
        assert "~" not in path, f"{p['id']}: home directory in {path!r}"


def test_paths_are_unique_except_for_shared_containers(products):
    """Two products may share a path only when they are layers of one file."""
    seen: dict[str, dict] = {}
    for p in products:
        prior = seen.get(p["path"])
        if prior is not None:
            assert p["format"] == prior["format"] == "gpkg_layer", (
                f"{p['id']} and {prior['id']} share path {p['path']!r} "
                "but are not GeoPackage layers"
            )
            assert p["gpkg_layer"] != prior["gpkg_layer"], (
                f"{p['id']} and {prior['id']} name the same layer"
            )
        else:
            seen[p["path"]] = p


def test_extractable_products_declare_a_key(products):
    """Anything the interface may slice must state what identifies a row."""
    needs_key = {"filtered_extract", "guided_combined"}
    for p in products:
        if p["access_mode"] in needs_key:
            assert p.get("keys"), (
                f"{p['id']} is offered as {p['access_mode']} but declares no key"
            )


def test_review_status_gates_access_mode(products):
    """A product whose meaning is unsettled is described, not served.

    This is the design document's own rule: domain_review_required,
    blocked_by_defect, and not_reviewed all mean unavailable. Where a product is
    held back this way, record what it should become in intended_access_mode
    rather than deleting the intent.
    """
    for p in products:
        allowed = REVIEW_ALLOWS_ACCESS[p["review_status"]]
        if allowed is not None:
            assert p["access_mode"] in allowed, (
                f"{p['id']} is {p['review_status']} but offered as "
                f"{p['access_mode']}; it must be one of {sorted(allowed)}. "
                "Use intended_access_mode to record the eventual intent."
            )


def test_intended_access_is_only_set_where_access_is_held_back(products):
    for p in products:
        intended = p.get("intended_access_mode")
        if intended is not None:
            assert p["access_mode"] == "catalog_only", (
                f"{p['id']} sets intended_access_mode but is already served as "
                f"{p['access_mode']}"
            )
            assert intended != "catalog_only", (
                f"{p['id']} intends catalog_only, which is what it already is"
            )


def test_planned_products_are_not_offered(products):
    """Nothing that has never been built can be offered for extraction."""
    for p in products:
        if p["lifecycle"] == "planned":
            assert p["access_mode"] == "catalog_only", (
                f"{p['id']} is planned but offered as {p['access_mode']}"
            )


def test_constrained_products_say_what_the_limit_is(products):
    """"Reviewed with limits" is meaningless without the limit written down."""
    for p in products:
        if p["review_status"] == "constrained":
            assert p.get("caveats"), (
                f"{p['id']} is constrained but lists no caveat explaining how"
            )


def test_facets_are_plain_column_names(products):
    for p in products:
        for f in p.get("facets") or []:
            assert isinstance(f, str) and f and " " not in f, (
                f"{p['id']}: facet {f!r} is not a column name"
            )
