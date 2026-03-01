# NBA Statistics Project

## Overview
This repo collects NBA data with a scripted scraping pipeline and stores outputs
by season and endpoint under the configured data root.

## Installation
Create a virtual environment and install requirements:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage
Run the full pipeline (the only command I use):

```bash
.venv/bin/python 01_scripts/01_scrapping/00_run_all.py --config 01_scripts/01_scrapping/00_config_example.yaml
```

Notes:
- Steps are configurable via `steps` in the config (defaults to all).
- Outputs are cached; existing files are skipped unless `--force` is used.
- The logger prints status tags (RUN, SKIP, DOWNLOAD, OK, LEFT, RETRY, WARN).
- Parquet is preferred; if it fails, the pipeline falls back to CSV.
- Parquet outputs are ignored by git via `.gitignore`.

## Data Sources
Data is pulled from `nba_api` endpoints and stored by season and endpoint.

By default the pipeline runs for both Regular Season and Playoffs; override with
`season_type` or `season_types` in the config.

## Output Layout
All data goes under:
`02_data/01_raw/<SEASON_FOLDER>/<DATASET_FOLDER>/...`

Season folders look like `2025_26` (from `2025-26`).

All outputs include metadata columns: `SEASON`, `SEASON_TYPE` (if applicable), `GAME_ID` (if applicable), `TEAM_ID` (if applicable), `ENDPOINT`.

## Tables (what each parquet contains)
This is a practical, high-level guide to what each dataset is and the key columns you should expect (not every column).

### `01_static_teams`
Source: `nba_api.stats.static.teams.get_teams()`
- File: `teams.parquet`
- What it is: static catalog of NBA teams.
- Key columns: `id` (TEAM_ID), `full_name`, `abbreviation`, `city`, `nickname`, plus `SEASON`, `ENDPOINT`.

### `02_team_season`
Team-level season snapshots (per team, per season type where applicable).
- `common_all_players/`
  - File: `common_all_players__season=<SEASON>.parquet`
  - What it is: current-season player list.
  - Key columns: `PERSON_ID` (PLAYER_ID), `DISPLAY_FIRST_LAST`, `TEAM_ID`, `ROSTERSTATUS`, plus `SEASON`, `ENDPOINT`.
- `team_info_common/`
  - What it is: team metadata for the season.
  - Key columns: `TEAM_ID`, `TEAM_NAME`, `W`, `L`, `CONF_RANK` (if present), plus `SEASON`, `SEASON_TYPE`, `ENDPOINT`.
- `team_dashboard_by_general_splits/`
  - What it is: team splits by common situations.
  - Key columns: `TEAM_ID`, `GROUP_SET`, `GROUP_VALUE`, `GP`, `W`, `L`, plus per-game stat columns and metadata.
- `team_dashboard_by_shooting_splits/`
  - What it is: team shooting splits (zones, shot types).
  - Key columns: `TEAM_ID`, `GROUP_SET`, `GROUP_VALUE`, `FGA`, `FGM`, `FG_PCT`, `FG3A`, `FG3M`, `FG3_PCT`, plus metadata.
- `team_dash_lineups/`
  - What it is: lineup performance and usage.
  - Key columns: `TEAM_ID`, `GROUP_ID` (lineup), `MIN`, `NET_RATING`, `OFF_RATING`, `DEF_RATING`, plus metadata.
- `team_player_onoff_summary/`
  - What it is: on/off splits aggregated by player.
  - Key columns: `TEAM_ID`, `VS_PLAYER_ID`, `MIN`, `NET_RATING`, `OFF_RATING`, `DEF_RATING`, plus metadata.
- `team_player_onoff_details/`
  - What it is: on/off splits with more detailed breakdowns.
  - Key columns: `TEAM_ID`, `VS_PLAYER_ID`, `GROUP_SET`, `GROUP_VALUE`, `MIN`, ratings, plus metadata.
- `team_dash_pt_shots/`
  - What it is: tracking-style shot profiles (pullups, catch-and-shoot, etc.).
  - Key columns: `TEAM_ID`, `SHOT_TYPE`, `FGA`, `FGM`, `FG_PCT`, `EFG_PCT`, plus metadata.
- `team_dash_pt_pass/`
  - What it is: passing and assist creation splits.
  - Key columns: `TEAM_ID`, `PASS`, `AST`, `AST_PCT`, `PTS`, plus metadata.
- `team_dash_pt_reb/`
  - What it is: rebounding opportunity and conversion splits.
  - Key columns: `TEAM_ID`, `REB`, `REB_CHANCE`, `REB_CHANCE_PCT`, plus metadata.
- `common_team_roster/`
  - What it is: full team roster for the season.
  - Key columns: `TEAM_ID`, `PLAYER_ID`, `PLAYER`, `NUM`, `POSITION`, `HEIGHT`, `WEIGHT`, plus `SEASON`, `ENDPOINT`.

### `03_team_game`
Game-level team tables (per game).
- `games_base/`
  - What it is: team game logs (one row per team per game).
  - Key columns: `GAME_ID`, `TEAM_ID`, `TEAM_NAME`, `GAME_DATE`, `MATCHUP`, `WL`, plus boxscore-like totals.
- `boxscore_traditional/`
  - What it is: traditional team boxscore.
  - Key columns: `GAME_ID`, `TEAM_ID`, `PTS`, `FGM`, `FGA`, `FG3M`, `FG3A`, `FTM`, `FTA`, `REB`, `AST`, `TOV`, plus metadata.
- `boxscore_summary_v2` / `boxscore_summary_v3`
  - What it is: team summary lines (pace/tempo style fields if present).
  - Key columns: `GAME_ID`, `TEAM_ID`, `MIN`, `PTS`, plus summary fields and metadata.
- `boxscore_advanced_v2` / `boxscore_advanced_v3`
  - What it is: advanced team boxscore.
  - Key columns: `GAME_ID`, `TEAM_ID`, `OFF_RATING`, `DEF_RATING`, `NET_RATING`, `PACE`, plus metadata.
- `boxscore_four_factors_v2` / `boxscore_four_factors_v3`
  - What it is: Four Factors team splits.
  - Key columns: `GAME_ID`, `TEAM_ID`, `EFG_PCT`, `TOV_PCT`, `OREB_PCT`, `FT_RATE`, plus metadata.
- `boxscore_misc_v2` / `boxscore_misc_v3`
  - What it is: misc team boxscore stats.
  - Key columns: `GAME_ID`, `TEAM_ID`, `PTS_OFF_TOV`, `PTS_2ND_CHANCE`, `PTS_FASTBREAK`, plus metadata.

### `04_player_game`
Game-level player tables (per game).
- `boxscore_traditional/`
  - What it is: traditional player boxscore.
  - Key columns: `GAME_ID`, `PLAYER_ID`, `TEAM_ID`, `MIN`, `PTS`, `FGM`, `FGA`, `FG3M`, `FG3A`, `FTM`, `FTA`, `REB`, `AST`, `TOV`, plus metadata.
- `boxscore_advanced_v2` / `boxscore_advanced_v3`
  - What it is: advanced player boxscore.
  - Key columns: `GAME_ID`, `PLAYER_ID`, `TEAM_ID`, `OFF_RATING`, `DEF_RATING`, `NET_RATING`, `USG_PCT`, plus metadata.
- `boxscore_usage_v2` / `boxscore_usage_v3`
  - What it is: usage and play-type style splits.
  - Key columns: `GAME_ID`, `PLAYER_ID`, `TEAM_ID`, `USG_PCT`, `PCT_*` usage fields, plus metadata.
- `boxscore_scoring_v2` / `boxscore_scoring_v3`
  - What it is: scoring breakdowns (paint, fastbreak, etc.).
  - Key columns: `GAME_ID`, `PLAYER_ID`, `TEAM_ID`, `PCT_PTS_*`, `PTS_*`, plus metadata.
- `boxscore_misc_v2` / `boxscore_misc_v3`
  - What it is: misc player boxscore stats.
  - Key columns: `GAME_ID`, `PLAYER_ID`, `TEAM_ID`, `PTS_OFF_TOV`, `PTS_2ND_CHANCE`, `PTS_FASTBREAK`, plus metadata.
- `boxscore_matchups_v3`
  - What it is: player matchup tracking.
  - Key columns: `GAME_ID`, `PLAYER_ID`, `MATCHUP_PLAYER_ID`, `MIN`, `FGM`, `FGA`, `FG_PCT`, plus metadata.

### `05_player_season`
Season-level player game logs (one row per player per game).
- File: `player_gamelog__season_type=<SEASON_TYPE>.parquet`
- What it is: player game log per season type.
- Key columns: `GAME_ID`, `PLAYER_ID`, `TEAM_ID`, `GAME_DATE`, `MATCHUP`, `WL`, plus core boxscore totals and metadata.

## Project Structure
- `01_scripts/01_scrapping/`: scraping pipeline entrypoints and endpoint logic.
- `02_data/`: output root for scraped data.

## Contributing

## License
