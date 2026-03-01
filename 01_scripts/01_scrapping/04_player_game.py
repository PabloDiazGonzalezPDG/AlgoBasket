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


def fetch_game_ids(season, season_type, tools, rate_limit):
    def _call():
        return leaguegamelog.LeagueGameLog(
            season=season,
            season_type_all_star=season_type,
            player_or_team_abbreviation="T",
        )

    resp = tools.safe_call(
        _call,
        rate_limit["sleep_seconds"],
        rate_limit["max_retries"],
        rate_limit["backoff_seconds"],
    )
    df = resp.get_data_frames()[0]
    col = "GAME_ID" if "GAME_ID" in df.columns else "Game_ID"
    return sorted(df[col].astype(str).unique().tolist())


def run(cfg, season, force=False):
    tools = load_tools()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = tools.resolve_output_root(cfg["output_root"], script_dir)
    season_folder = tools.season_to_folder(season)
    base_dir = os.path.join(output_root, season_folder, "04_player_game")
    tools.ensure_dir(base_dir)

    rate_limit = cfg.get("rate_limit", {})
    season_types = tools.get_season_types(cfg)
    endpoints = tools.get_player_game_endpoints(season)

    for season_type in season_types:
        game_ids = fetch_game_ids(season, season_type, tools, rate_limit)
        if not game_ids:
            continue

        for endpoint in endpoints:
            slug = endpoint["slug"]
            out_dir = os.path.join(base_dir, slug)
            tools.ensure_dir(out_dir)
            log_path = os.path.join(out_dir, "_downloaded_game_ids.txt")
            logged = tools.read_log_ids(log_path)
            existing = tools.find_existing_ids(out_dir, f"{slug}__")
            remaining_ids = [gid for gid in game_ids if str(gid) not in existing]
            tools.log_status("LEFT", f"player_game {slug} {len(remaining_ids)}")

            for game_id in remaining_ids:
                out_path = os.path.join(out_dir, f"{slug}__{game_id}.parquet")

                def _call():
                    return endpoint["cls"](game_id=game_id)

                resp = tools.safe_call(
                    _call,
                    rate_limit["sleep_seconds"],
                    rate_limit["max_retries"],
                    rate_limit["backoff_seconds"],
                )
                df_box = resp.get_data_frames()[endpoint["data_index"]]
                df_box = df_box.copy()
                df_box["SEASON"] = season
                df_box["GAME_ID"] = str(game_id)
                df_box["SEASON_TYPE"] = season_type
                df_box["ENDPOINT"] = slug
                tools.write_df(df_box, out_path, cfg.get("output_format", "parquet"))
                tools.append_log_id(log_path, str(game_id))
                logged.add(str(game_id))
                existing.add(str(game_id))
                tools.log_status("OK", f"player_game {slug} game={game_id} rows={len(df_box)}")


def run_update(cfg, season):
    run(cfg, season, force=False)
