"""Extract the published notebook algorithms verbatim; --check detects drift."""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPECS = {
    "quality_rules": ("00_position_quality_and_anomalies.py", {
        "MAX_EPISODE_S", "WINDOW_STEP_S", "FUTURE_TOLERANCE_S", "MAX_MOTION_KNOTS",
        "MIN_WINDOW_S", "field_checks", "motion_checks", "fit_circle",
        "WINDOW_MEMBER_SCHEMA", "WINDOW_MEMBER_COLUMNS", "window_members_group",
        "rule_struct", "observation_decisions",
    }),
    "tracks": ("01_identity_tracks_and_late_data.py", {
        "AIS_SCHEMA", "AIS_COLUMNS", "WINDOW_S", "MAX_GAP_S", "MAX_SPEED_MPS",
        "TIE_MARGIN_M", "H3_RES", "H3_K", "ASSIGN_SCHEMA", "ASSIGN_COLS", "POSITION_COLS",
        "prepare_reports", "digest", "resolve_group", "candidate_edges", "resolve_distributed",
        "as_nodes", "replay_inputs", "process_micro_batch",
    }),
    "callings": ("02_port_and_berth_callings.py", {"merge_micro_batch"}),
}
for module, (notebook, names) in SPECS.items():
    source = (ROOT / "notebooks" / notebook).read_text()
    lines = source.splitlines(keepends=True)
    selected = []
    for node in ast.parse(source).body:
        key = node.name if isinstance(node, ast.FunctionDef) else (
            node.targets[0].id if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) else None)
        imported = isinstance(node, (ast.Import, ast.ImportFrom))
        if module == "quality_rules" and imported:
            imported = not (isinstance(node, ast.ImportFrom) and node.module.startswith("matplotlib")) and not (
                isinstance(node, ast.Import) and any(a.name.startswith("matplotlib") for a in node.names))
        if imported or key in names:
            selected.append("".join(lines[node.lineno - 1:node.end_lineno]).rstrip())
    result = f'"""Extracted from {notebook}; regenerate with tools/sync_notebook_code.py."""\n\n' + "\n\n".join(selected) + "\n"
    target = ROOT / "pipeline" / "ais_pipeline" / f"{module}.py"
    if "--check" in sys.argv:
        assert target.read_text() == result, f"Regenerate {target} from its notebook"
    else:
        target.write_text(result)
print("Notebook algorithm parity verified" if "--check" in sys.argv else "Notebook algorithms extracted")
