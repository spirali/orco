
from orco import Runtime, LocalExecutor, run_cli
import time, random
import itertools

runtime = Runtime("mydb.db")


def train_player(config):
    time.sleep(random.randint(5, 15) / 10)
    return {"strength": random.randint(0, 10)}


def play_game(config, deps):
    time.sleep(0.3)
    r1 = random.randint(0, deps[0].value["strength"] * 2)
    r2 = random.randint(0, deps[1].value["strength"] * 2)
    return r1 - r2


def game_deps(config):
    return [players.ref(config["player1"]), players.ref(config["player2"])]


def play_tournament(config, deps):
    return [d.value for d in deps]


def tournament_deps(config):
    return [plays.ref({"player1": p1, "player2": p2})
            for (p1, p2) in itertools.product(config["players"], config["players"])]


players = runtime.register_collection("players", build_fn=train_player)
plays = runtime.register_collection("plays", build_fn=play_game, dep_fn=game_deps)
tournaments = runtime.register_collection("tournaments", build_fn=play_tournament, dep_fn=tournament_deps)

run_cli(runtime)
