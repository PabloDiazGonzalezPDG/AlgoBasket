# Numbered NBA API scripts (simple)

One helper file: `01_function_tools.py`.

## Scripts (one per chapter)
- `00_run_all.py`: run all steps for each season; supports `--force` to overwrite.
- `00_update_all.py`: update mode (append vs refresh rules).
- `01_static_teams.py`: teams list from `nba_api.stats.static.teams.get_teams()`.
- `02_team_season.py`: team season endpoints (info, dashboards, rosters).
- `03_team_game.py`: team game endpoints (games_base + team boxscores per game).
- `04_player_game.py`: player game endpoints (player boxscores per game).
- `05_player_season.py`: player season logs (LeagueGameLog with P).

## Output layout
All data goes under:
`02_data/01_raw/<SEASON_FOLDER>/<DATASET_FOLDER>/...`

Season folders look like `2025_26` (from `2025-26`).

Dataset folders:
- `01_static_teams/`
- `02_team_season/<endpoint_slug>/`
- `03_team_game/<endpoint_slug>/`
- `04_player_game/<endpoint_slug>/`
- `05_player_season/`

File naming:
- per game: `<slug>__<GAME_ID>.parquet`
- per team: `<slug>__team_id=<TEAM_ID>.parquet`

All outputs include metadata columns: `SEASON`, `GAME_ID` (if applicable), `ENDPOINT`.

## Update rules
Append (skip if file exists):
- `03_team_game`
- `04_player_game`

Refresh (overwrite):
- `01_static_teams`
- `02_team_season`
- `05_player_season`

## Example commands
```bash
python 00_run_all.py --config 00_config_example.yaml --force
python 00_update_all.py --config 00_config_example.yaml
```

This code is intentionally SIMPLE.
