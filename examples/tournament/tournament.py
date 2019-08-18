from orco import Runtime, run_cli
import random
import itertools

runtime = Runtime("mydb.db")


# Build function for "players"
def train_player(config, deps):
    # We will simulate trained players by dictionary with key "strength"
    return {"strength": random.randint(0, 10)}


# Dependancy function for "plays"
# To play a game we need, we need both players
def game_deps(config):
    return [players.ref(config["player1"]), players.ref(config["player2"])]


# Build a function for "plays"
# Because of "game_deps", in the "inputs" we find two players
def play_game(config, inputs):
    # Simulation of playing a game between two players,
    # They just throw k-sided dices, where k is trength of the player
    # The difference of throw is the result
    r1 = random.randint(0, inputs[0].value["strength"] * 2)
    r2 = random.randint(0, inputs[1].value["strength"] * 2)
    return r1 - r2


# Dependancy function "tournaments"
# For evaluating tournament, we need to know results of plays between
# each pair of players.
def tournament_deps(config):
    return [
        plays.ref({
            "player1": p1,
            "player2": p2
        }) for (p1, p2) in itertools.product(config["players"], config["players"])
    ]


# Build function for a tournament, return score for each player
def play_tournament(config, inputs):
    score = {}
    for play in inputs:
        player1 = play.config["player1"]
        player2 = play.config["player2"]
        score.setdefault(player1, 0)
        score.setdefault(player2, 0)
        score[player1] += play.value
        score[player2] -= play.value
    return score


players = runtime.register_collection("players", build_fn=train_player)
plays = runtime.register_collection("plays", build_fn=play_game, dep_fn=game_deps)
tournaments = runtime.register_collection(
    "tournaments", build_fn=play_tournament, dep_fn=tournament_deps)

run_cli(runtime)
