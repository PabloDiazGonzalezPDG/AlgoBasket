import argparse
import importlib.util
import os


def load_tools():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "01_function_tools.py")
    spec = importlib.util.spec_from_file_location("tools", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_step_module(step_name):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    step_map = {
        "static_teams": "01_static_teams.py",
        "team_season": "02_team_season.py",
        "team_game": "03_team_game.py",
        "player_game": "04_player_game.py",
        "player_season": "05_player_season.py",
    }
    filename = step_map[step_name]
    path = os.path.join(script_dir, filename)
    spec = importlib.util.spec_from_file_location(step_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    tools = load_tools()
    cfg = tools.load_yaml(args.config)
    seasons = cfg.get("seasons", [])
    steps = tools.get_steps(cfg)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    tools.resolve_output_root(cfg["output_root"], script_dir)

    for season in seasons:
        for step in steps:
            tools.log_status("RUN", f"season={season} step={step}")
            module = load_step_module(step)
            module.run(cfg, season, force=args.force)


if __name__ == "__main__":
    main()
