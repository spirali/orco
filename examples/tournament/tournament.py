import itertools
import random
import collections

import orco


Player = collections.namedtuple("Player", ["strength"])

# Function that trains "players"
@orco.builder()
def train_player(_player_name):
    # We will simulate trained players by a simple object with "strength"
    return Player(strength=random.randint(0, 10))


# Build function for "games"
@orco.builder()
def play_game(p1, p2):
    player1 = train_player(p1)
    player2 = train_player(p2)
    yield

    # Simulation of playing a game between two players,
    # They just throw k-sided dices, where k is trength of the player
    # The difference of throw is the result

    r1 = random.randint(0, player1.value.strength * 2)
    r2 = random.randint(0, player2.value.strength * 2)
    return r1 - r2


# Build function for a tournament, return score for each player
@orco.builder()
def play_tournament(players):
    # For evaluating a tournament, we need to know the results of games between
    # each pair of its players.
    games = {
        (p1, p2): play_game(p1, p2)
        for (p1, p2) in itertools.product(players, players)
    }
    yield

    score = {}
    for (p1, p2), game in games.items():
        score.setdefault(p1, 0)
        score.setdefault(p2, 0)
        score[p1] += game.value
        score[p2] -= game.value
    return score


orco.run_cli()
