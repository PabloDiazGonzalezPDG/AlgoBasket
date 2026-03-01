import importlib.util
import os

from nba_api.stats.endpoints import leaguegamelog


def load_tools():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "01_function_tools.py")
    spec = importlib.util.spec_from_file_location("tools", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(cfg, season, force=False):
    tools = load_tools()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = tools.resolve_output_root(cfg["output_root"], script_dir)
    season_folder = tools.season_to_folder(season)
    base_dir = os.path.join(output_root, season_folder, "05_player_season")
    tools.ensure_dir(base_dir)

    rate_limit = cfg.get("rate_limit", {})
    season_types = tools.get_season_types(cfg)

    for season_type in season_types:
        slug = tools.slugify(season_type)
        out_path = os.path.join(base_dir, f"player_gamelog__season_type={slug}.parquet")
        if tools.output_exists(out_path) and not force:
            tools.log_status("SKIP", f"player_season season_type={season_type} existing: {out_path}")
            continue

        def _call():
            return leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star=season_type,
                player_or_team_abbreviation="P",
            )

        resp = tools.safe_call(
            _call,
            rate_limit["sleep_seconds"],
            rate_limit["max_retries"],
            rate_limit["backoff_seconds"],
        )
        df = resp.get_data_frames()[0]
        df["SEASON"] = season
        df["SEASON_TYPE"] = season_type
        df["ENDPOINT"] = "player_gamelog"
        tools.write_df(df, out_path, cfg.get("output_format", "parquet"))
        tools.log_status("OK", f"player_season season_type={season_type} rows={len(df)}")


def run_update(cfg, season):
    run(cfg, season, force=True)
