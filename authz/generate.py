"""Generate the OpenFGA JSON wire model from the project's restricted DSL.

Only the constructs used by model.fga are accepted; unsupported edits fail loudly.
"""
import json
import pathlib
import re

root = pathlib.Path(__file__).parent
model = {"schema_version": "1.1", "type_definitions": []}
current = None
for line in (root / "model.fga").read_text().splitlines():
    line = line.strip()
    if not line or line in ("model", "schema 1.1", "relations"):
        continue
    if line.startswith("type "):
        current = {"type": line[5:]}
        model["type_definitions"].append(current)
        continue
    match = re.fullmatch(r"define (\w+): (.+)", line)
    if not match or current is None:
        raise ValueError(f"Unsupported DSL: {line}")
    name, expr = match.groups()
    relations = current.setdefault("relations", {})
    if expr.startswith("[") and expr.endswith("]"):
        relations[name] = {"this": {}}
        metadata = current.setdefault("metadata", {}).setdefault("relations", {})
        metadata[name] = {"directly_related_user_types": [{"type": t.strip()} for t in expr[1:-1].split(",")]}
    elif " from " in expr:
        relation, parent = expr.split(" from ")
        relations[name] = {"tupleToUserset": {"tupleset": {"relation": parent}, "computedUserset": {"relation": relation}}}
    elif " and " in expr:
        relations[name] = {"intersection": {"child": [{"computedUserset": {"relation": r}} for r in expr.split(" and ")]}}
    else:
        raise ValueError(f"Unsupported relation: {expr}")
(root / "model.json").write_text(json.dumps(model, indent=2) + "\n")
