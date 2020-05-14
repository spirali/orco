.phony: test, pytest, test_tournament, test_simple, test_adder

PYTHON=python3

test: test_simple test_adder test_tournament 

EXAMPLE_DB=exampledb.sqlite3
EXAMPLE_DB_URL=sqlite:///${EXAMPLE_DB}

test_simple:
	rm -f "$(EXAMPLE_DB)"
	PYTHONPATH=.. ${PYTHON} simple/simple.py --db "$(EXAMPLE_DB_URL)" \
		compute make_experiment '{"difficulty": 3}'
	rm -f "$(EXAMPLE_DB)"

test_adder:
	rm -f "$(EXAMPLE_DB)"
	PYTHONPATH=.. ${PYTHON} adder/adder_cli.py --db "$(EXAMPLE_DB_URL)" \
		compute add '{"$$product": {"a": [0,100], "b": {"$$range": 3}}}'
	rm -f "$(EXAMPLE_DB)"

test_tournament:
	rm -f "$(EXAMPLE_DB)"
	PYTHONPATH=.. ${PYTHON} tournament/tournament.py --db "$(EXAMPLE_DB_URL)" \
		compute play_tournament '{"players": ["a", "b", "c"]}'
	rm -f "$(EXAMPLE_DB)"
