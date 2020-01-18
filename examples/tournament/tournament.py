from orco import Runtime, run_cli
import random
import itertools

runtime = Runtime("mydb.db")


# Build function for "players"
def train_player(config, inputs):
    # We will simulate trained players by a dictionary with a "strength" key
    return {"strength": random.randint(0, 10)}


# Dependency function for "games"
# To play a game we need both of its players computed
def game_deps(config):
    return [players.task(config["player1"]), players.task(config["player2"])]


# Build function for "games"
# Because of "game_deps", in the "inputs" we find the two computed players
def play_game(config, inputs):
    # Simulation of playing a game between two players,
    # They just throw k-sided dices, where k is trength of the player
    # The difference of throw is the result

    # 'inputs' is a list of two instances of Entry, hence we use the value getter
    # to obtain the actual player
    r1 = random.randint(0, inputs[0].value["strength"] * 2)
    r2 = random.randint(0, inputs[1].value["strength"] * 2)
    return r1 - r2


# Dependency function for "tournaments"
# For evaluating a tournament, we need to know the results of games between
# each pair of its players.
def tournament_deps(config):
    return [
        games.task({
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


players = runtime.register_builder("players", build_fn=train_player)
games = runtime.register_builder("games", build_fn=play_game, dep_fn=game_deps)
tournaments = runtime.register_builder(
    "tournaments", build_fn=play_tournament, dep_fn=tournament_deps)

run_cli(runtime)
