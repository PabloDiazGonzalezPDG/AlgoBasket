import importlib.util
import os

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog, teamgamelogs


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


def fetch_team_gamelogs(season, season_type, tools, rate_limit):
    def _call():
        return teamgamelogs.TeamGameLogs(
            season_nullable=season,
            season_type_nullable=season_type,
        )

    resp = tools.safe_call(
        _call,
        rate_limit["sleep_seconds"],
        rate_limit["max_retries"],
        rate_limit["backoff_seconds"],
    )
    df = resp.get_data_frames()[0]
    if "Game_ID" in df.columns and "GAME_ID" not in df.columns:
        df = df.rename(columns={"Game_ID": "GAME_ID"})
    return df


def run(cfg, season, force=False):
    tools = load_tools()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = tools.resolve_output_root(cfg["output_root"], script_dir)
    season_folder = tools.season_to_folder(season)
    base_dir = os.path.join(output_root, season_folder, "03_team_game")
    tools.ensure_dir(base_dir)

    rate_limit = cfg.get("rate_limit", {})
    season_types = tools.get_season_types(cfg)

    games_base_dir = os.path.join(base_dir, "games_base")
    tools.ensure_dir(games_base_dir)
    base_log_path = os.path.join(games_base_dir, "_downloaded_game_ids.txt")
    base_logged = tools.read_log_ids(base_log_path)
    base_existing = tools.find_existing_ids(games_base_dir, "games_base__")
    force_games_base = bool(cfg.get("force_games_base", False))

    for season_type in season_types:
        df = fetch_team_gamelogs(season, season_type, tools, rate_limit)
        if df.empty:
            continue
        df["SEASON"] = season
        df["SEASON_TYPE"] = season_type
        df["ENDPOINT"] = "games_base"

        base_all_ids = df["GAME_ID"].astype(str).unique().tolist()
        if force_games_base:
            base_remaining = base_all_ids
        else:
            base_remaining = [gid for gid in base_all_ids if gid not in base_existing]

        for game_id, group in df.groupby("GAME_ID"):
            if str(game_id) not in base_remaining and not force_games_base:
                continue
            out_path = os.path.join(games_base_dir, f"games_base__{game_id}.parquet")
            group = group.copy()
            group["GAME_ID"] = str(game_id)
            tools.write_df(group, out_path, cfg.get("output_format", "parquet"))
            tools.append_log_id(base_log_path, str(game_id))
            base_logged.add(str(game_id))
            base_existing.add(str(game_id))
            tools.log_status("OK", f"team_game games_base game={game_id} rows={len(group)}")

        game_ids = fetch_game_ids(season, season_type, tools, rate_limit)
        endpoints = tools.get_team_game_endpoints(season)

        for endpoint in endpoints:
            slug = endpoint["slug"]
            out_dir = os.path.join(base_dir, slug)
            tools.ensure_dir(out_dir)
            log_path = os.path.join(out_dir, "_downloaded_game_ids.txt")
            logged = tools.read_log_ids(log_path)
            existing = tools.find_existing_ids(out_dir, f"{slug}__")
            remaining_ids = [gid for gid in game_ids if str(gid) not in existing]
            tools.log_status("LEFT", f"team_game {slug} {len(remaining_ids)}")

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
                tools.log_status("OK", f"team_game {slug} game={game_id} rows={len(df_box)}")


def run_update(cfg, season):
    run(cfg, season, force=False)
