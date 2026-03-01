import importlib.util
import os

import pandas as pd
from nba_api.stats.endpoints import commonallplayers


def load_tools():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "01_function_tools.py")
    spec = importlib.util.spec_from_file_location("tools", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def resultsets_to_df(resp, skip_tables=None):
    skip_tables = set(skip_tables or [])
    data = resp.get_dict().get("resultSets", [])
    frames = []
    for table in data:
        name = table.get("name")
        if name in skip_tables:
            continue
        headers = table.get("headers", [])
        rows = table.get("rowSet", [])
        df = pd.DataFrame(rows, columns=headers)
        df["TABLE_NAME"] = name
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run(cfg, season, force=False):
    tools = load_tools()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_root = tools.resolve_output_root(cfg["output_root"], script_dir)
    season_folder = tools.season_to_folder(season)
    base_dir = os.path.join(output_root, season_folder, "02_team_season")
    tools.ensure_dir(base_dir)

    rate_limit = cfg.get("rate_limit", {})
    season_types = tools.get_season_types(cfg)
    team_ids = tools.get_team_ids(cfg)

    players_dir = os.path.join(base_dir, "common_all_players")
    tools.ensure_dir(players_dir)
    players_path = os.path.join(players_dir, f"common_all_players__season={season}.parquet")
    if tools.output_exists(players_path) and not force:
        tools.log_status("SKIP", f"team_season common_all_players existing: {players_path}")
    else:
        tools.log_status("DOWNLOAD", f"team_season common_all_players season={season} -> {players_path}")
        def _call():
            return commonallplayers.CommonAllPlayers(league_id="00", season=season, is_only_current_season=1)

        resp = tools.safe_call(
            _call,
            rate_limit["sleep_seconds"],
            rate_limit["max_retries"],
            rate_limit["backoff_seconds"],
        )
        df_players = resp.get_data_frames()[0]
        df_players["SEASON"] = season
        df_players["ENDPOINT"] = "common_all_players"
        tools.write_df(df_players, players_path, cfg.get("output_format", "parquet"))
        tools.log_status("OK", f"team_season common_all_players rows={len(df_players)}")

    for endpoint in tools.TEAM_SEASON_ENDPOINTS:
        slug = endpoint["slug"]
        out_dir = os.path.join(base_dir, slug)
        tools.ensure_dir(out_dir)

        for team_id in team_ids:
            out_path = os.path.join(out_dir, f"{slug}__team_id={team_id}.parquet")
            if tools.output_exists(out_path) and not force:
                tools.log_status("SKIP", f"team_season {slug} team={team_id} existing: {out_path}")
                continue

            tools.log_status("DOWNLOAD", f"team_season {slug} team={team_id} -> {out_path}")
            frames = []
            loop_season_types = season_types
            if slug == "common_team_roster":
                loop_season_types = [""]

            for season_type in loop_season_types:
                def _call():
                    if slug == "team_info_common":
                        return endpoint["cls"](
                            team_id=team_id,
                            season_nullable=season,
                            season_type_nullable=season_type,
                        )
                    if slug == "common_team_roster":
                        return endpoint["cls"](team_id=team_id, season=season)
                    return endpoint["cls"](
                        team_id=team_id,
                        season=season,
                        season_type_all_star=season_type,
                    )

                resp = tools.safe_call(
                    _call,
                    rate_limit["sleep_seconds"],
                    rate_limit["max_retries"],
                    rate_limit["backoff_seconds"],
                )
                if slug == "team_info_common":
                    df = resultsets_to_df(resp, skip_tables={"AvailableSeasons"})
                else:
                    df = resultsets_to_df(resp)
                if df.empty:
                    continue
                df = df.copy()
                df["SEASON"] = season
                df["SEASON_TYPE"] = season_type
                df["TEAM_ID"] = team_id
                df["ENDPOINT"] = slug
                frames.append(df)

            final_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            tools.write_df(final_df, out_path, cfg.get("output_format", "parquet"))
            tools.log_status("OK", f"team_season {slug} team={team_id} rows={len(final_df)}")


def run_update(cfg, season):
    run(cfg, season, force=True)
