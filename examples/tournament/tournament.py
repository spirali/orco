import orco
import random
import itertools


# Function that trains "players"
@orco.builder()
def train_player(config):
    # We will simulate trained players by a dictionary with a "strength" key
    return {"strength": random.randint(0, 10)}


# Build function for "games"
@orco.builder()
def play_game(config):
    player1 = train_player(config["player1"])
    player2 = train_player(config["player2"])
    yield

    # Simulation of playing a game between two players,
    # They just throw k-sided dices, where k is trength of the player
    # The difference of throw is the result

    r1 = random.randint(0, player1.value["strength"] * 2)
    r2 = random.randint(0, player2.value["strength"] * 2)
    return r1 - r2


# Build function for a tournament, return score for each player
@orco.builder()
def play_tournament(config):
    # For evaluating a tournament, we need to know the results of games between
    # each pair of its players.
    games = [
        play_game({"player1": p1, "player2": p2})
        for (p1, p2) in itertools.product(config["players"], config["players"])
    ]
    yield

    score = {}
    for game in games:
        player1 = game.config["player1"]
        player2 = game.config["player2"]
        score.setdefault(player1, 0)
        score.setdefault(player2, 0)
        score[player1] += game.value
        score[player2] -= game.value
    return score


runtime = orco.Runtime("mydb.db")
orco.run_cli(runtime)
