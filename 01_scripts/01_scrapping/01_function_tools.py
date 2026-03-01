import os
import time
from datetime import datetime

import yaml
from nba_api.stats.static import teams as teams_static
from nba_api.stats.endpoints import (
    boxscoreadvancedv2,
    boxscoreadvancedv3,
    boxscorefourfactorsv2,
    boxscorefourfactorsv3,
    boxscorematchupsv3,
    boxscoremiscv2,
    boxscoremiscv3,
    boxscorescoringv2,
    boxscorescoringv3,
    boxscoresummaryv2,
    boxscoresummaryv3,
    boxscoretraditionalv2,
    boxscoretraditionalv3,
    boxscoreusagev2,
    boxscoreusagev3,
    commonteamroster,
    teamdashlineups,
    teamdashptpass,
    teamdashptreb,
    teamdashptshots,
    teamdashboardbygeneralsplits,
    teamdashboardbyshootingsplits,
    teaminfocommon,
    teamplayeronoffdetails,
    teamplayeronoffsummary,
)

PARQUET_AVAILABLE = None
PARQUET_WARNED = False

STATUS_ICONS = {
    "RUN": "🚀",
    "CHECK": "🔍",
    "SKIP": "⏭️",
    "DOWNLOAD": "⬇️",
    "OK": "✅",
    "LEFT": "🧮",
    "RETRY": "🔁",
    "WARN": "⚠️",
    "INFO": "ℹ️",
}


def print_info(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def log_status(status, msg):
    icon = STATUS_ICONS.get(status, STATUS_ICONS["INFO"])
    print_info(f"{icon} {status} {msg}")


def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def season_to_folder(season_str):
    return season_str.replace("-", "_")


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def resolve_output_root(output_root, script_dir):
    if os.path.isabs(output_root):
        return output_root
    repo_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    return os.path.join(repo_root, output_root)


def get_season_types(cfg):
    if cfg.get("season_types"):
        return cfg["season_types"]
    if cfg.get("season_type"):
        return [cfg["season_type"]]
    return ["Regular Season", "Playoffs"]


def get_steps(cfg):
    if cfg.get("steps"):
        return cfg["steps"]
    return ["static_teams", "team_season", "team_game", "player_game", "player_season"]


def get_team_ids(cfg):
    if cfg.get("teams") == "all":
        return [t["id"] for t in teams_static.get_teams()]
    return cfg.get("teams", [])


def slugify(value):
    return value.lower().replace(" ", "_")


def safe_call(fn, sleep_seconds, max_retries, backoff_seconds):
    attempt = 0
    while True:
        try:
            result = fn()
            time.sleep(sleep_seconds)
            return result
        except Exception as exc:  # noqa: BLE001
            attempt += 1
            if attempt > max_retries:
                raise
            wait = backoff_seconds * attempt
            log_status("RETRY", f"{attempt}/{max_retries} after error: {exc}")
            time.sleep(wait)


def write_df(df, path, output_format="parquet"):
    global PARQUET_AVAILABLE  # noqa: PLW0603
    global PARQUET_WARNED  # noqa: PLW0603
    ext = os.path.splitext(path)[1].lower()
    if output_format == "parquet":
        if PARQUET_AVAILABLE is False:
            if not PARQUET_WARNED:
                log_status("WARN", "parquet not available; falling back to csv")
                PARQUET_WARNED = True
            output_format = "csv"
        else:
            try:
                df.to_parquet(path, index=False)
                PARQUET_AVAILABLE = True
                return path
            except Exception:  # noqa: BLE001
                PARQUET_AVAILABLE = False
                if not PARQUET_WARNED:
                    log_status("WARN", "parquet failed; falling back to csv")
                    PARQUET_WARNED = True
                output_format = "csv"

    if output_format == "csv":
        if ext != ".csv":
            path = os.path.splitext(path)[0] + ".csv"
        df.to_csv(path, index=False)
        return path

    raise ValueError(f"unknown output_format: {output_format}")


def output_exists(base_path):
    root, _ = os.path.splitext(base_path)
    return os.path.exists(root + ".parquet") or os.path.exists(root + ".csv")


def read_log_ids(path):
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def append_log_id(path, value):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{value}\n")


def find_existing_ids(out_dir, prefix):
    if not os.path.isdir(out_dir):
        return set()
    ids = set()
    for name in os.listdir(out_dir):
        if not name.startswith(prefix):
            continue
        root, ext = os.path.splitext(name)
        if ext not in {".parquet", ".csv"}:
            continue
        value = root[len(prefix):]
        if value:
            ids.add(value)
    return ids


TEAM_SEASON_ENDPOINTS = [
    {"slug": "team_info_common", "cls": teaminfocommon.TeamInfoCommon},
    {"slug": "team_dashboard_by_general_splits", "cls": teamdashboardbygeneralsplits.TeamDashboardByGeneralSplits},
    {"slug": "team_dashboard_by_shooting_splits", "cls": teamdashboardbyshootingsplits.TeamDashboardByShootingSplits},
    {"slug": "team_dash_lineups", "cls": teamdashlineups.TeamDashLineups},
    {"slug": "team_player_onoff_summary", "cls": teamplayeronoffsummary.TeamPlayerOnOffSummary},
    {"slug": "team_player_onoff_details", "cls": teamplayeronoffdetails.TeamPlayerOnOffDetails},
    {"slug": "team_dash_pt_shots", "cls": teamdashptshots.TeamDashPtShots},
    {"slug": "team_dash_pt_pass", "cls": teamdashptpass.TeamDashPtPass},
    {"slug": "team_dash_pt_reb", "cls": teamdashptreb.TeamDashPtReb},
    {"slug": "common_team_roster", "cls": commonteamroster.CommonTeamRoster},
]


def _season_start_year(season):
    try:
        return int(str(season).split("-")[0])
    except Exception:  # noqa: BLE001
        return 0


def _traditional_boxscore_cls(season):
    if _season_start_year(season) >= 2025:
        return boxscoretraditionalv3.BoxScoreTraditionalV3
    return boxscoretraditionalv2.BoxScoreTraditionalV2


def _summary_boxscore_cls(season):
    if _season_start_year(season) >= 2025:
        return boxscoresummaryv3.BoxScoreSummaryV3
    return boxscoresummaryv2.BoxScoreSummaryV2


def _use_v3_boxscores(season):
    return _season_start_year(season) >= 2025


def get_team_game_endpoints(season):
    traditional_cls = _traditional_boxscore_cls(season)
    use_v3 = _use_v3_boxscores(season)
    summary_cls = _summary_boxscore_cls(season)
    summary_slug = "boxscore_summary_v3" if summary_cls is boxscoresummaryv3.BoxScoreSummaryV3 else "boxscore_summary_v2"
    advanced_cls = boxscoreadvancedv3.BoxScoreAdvancedV3 if use_v3 else boxscoreadvancedv2.BoxScoreAdvancedV2
    advanced_slug = "boxscore_advanced_v3" if use_v3 else "boxscore_advanced_v2"
    four_factors_cls = boxscorefourfactorsv3.BoxScoreFourFactorsV3 if use_v3 else boxscorefourfactorsv2.BoxScoreFourFactorsV2
    four_factors_slug = "boxscore_four_factors_v3" if use_v3 else "boxscore_four_factors_v2"
    misc_cls = boxscoremiscv3.BoxScoreMiscV3 if use_v3 else boxscoremiscv2.BoxScoreMiscV2
    misc_slug = "boxscore_misc_v3" if use_v3 else "boxscore_misc_v2"
    traditional_index = 2 if use_v3 else 0
    team_index_v3 = 1
    return [
        {"slug": "boxscore_traditional", "cls": traditional_cls, "data_index": traditional_index},
        {"slug": summary_slug, "cls": summary_cls, "data_index": 0},
        {"slug": advanced_slug, "cls": advanced_cls, "data_index": team_index_v3 if use_v3 else 0},
        {"slug": four_factors_slug, "cls": four_factors_cls, "data_index": team_index_v3 if use_v3 else 0},
        {"slug": misc_slug, "cls": misc_cls, "data_index": team_index_v3 if use_v3 else 0},
    ]


def get_player_game_endpoints(season):
    traditional_cls = _traditional_boxscore_cls(season)
    use_v3 = _use_v3_boxscores(season)
    advanced_cls = boxscoreadvancedv3.BoxScoreAdvancedV3 if use_v3 else boxscoreadvancedv2.BoxScoreAdvancedV2
    advanced_slug = "boxscore_advanced_v3" if use_v3 else "boxscore_advanced_v2"
    usage_cls = boxscoreusagev3.BoxScoreUsageV3 if use_v3 else boxscoreusagev2.BoxScoreUsageV2
    usage_slug = "boxscore_usage_v3" if use_v3 else "boxscore_usage_v2"
    scoring_cls = boxscorescoringv3.BoxScoreScoringV3 if use_v3 else boxscorescoringv2.BoxScoreScoringV2
    scoring_slug = "boxscore_scoring_v3" if use_v3 else "boxscore_scoring_v2"
    misc_cls = boxscoremiscv3.BoxScoreMiscV3 if use_v3 else boxscoremiscv2.BoxScoreMiscV2
    misc_slug = "boxscore_misc_v3" if use_v3 else "boxscore_misc_v2"
    traditional_index = 0 if use_v3 else 1
    return [
        {"slug": "boxscore_traditional", "cls": traditional_cls, "data_index": traditional_index},
        {"slug": advanced_slug, "cls": advanced_cls, "data_index": 0 if use_v3 else 1},
        {"slug": usage_slug, "cls": usage_cls, "data_index": 0 if use_v3 else 1},
        {"slug": scoring_slug, "cls": scoring_cls, "data_index": 0 if use_v3 else 1},
        {"slug": misc_slug, "cls": misc_cls, "data_index": 0 if use_v3 else 1},
        {"slug": "boxscore_matchups_v3", "cls": boxscorematchupsv3.BoxScoreMatchupsV3, "data_index": 0},
    ]
