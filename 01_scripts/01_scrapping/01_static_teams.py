import importlib.util
import os

import pandas as pd
from nba_api.stats.static import teams as teams_static


def load_tools():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "01_function_tools.py")
    spec = importlib.util.spec_from_file_location("tools", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(cfg, season, force=False):  # noqa: ARG001
    tools = load_tools()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = tools.resolve_output_root(cfg["output_root"], script_dir)
    season_folder = tools.season_to_folder(season)
    out_dir = os.path.join(output_root, season_folder, "01_static_teams")
    tools.ensure_dir(out_dir)

    out_path = os.path.join(out_dir, "teams.parquet")
    if tools.output_exists(out_path) and not force:
        tools.log_status("SKIP", f"static_teams existing: {out_path}")
        return

    tools.log_status("DOWNLOAD", f"static_teams -> {out_path}")
    df = pd.DataFrame(teams_static.get_teams())
    df["SEASON"] = season
    df["ENDPOINT"] = "static_teams"
    tools.write_df(df, out_path, cfg.get("output_format", "parquet"))
    tools.log_status("OK", f"static_teams rows={len(df)}")


def run_update(cfg, season):
    run(cfg, season, force=True)
