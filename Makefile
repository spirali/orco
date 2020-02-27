.phony: test, pytest, test_tournament, test_simple, test_adder

test: pytest test_simple test_adder test_tournament 

pytest:
	PYTHONPATH=. python -m pytest

EXAMPLE_DB=examples/exampledb.sqlite3

test_simple:
	rm -f "$(EXAMPLE_DB)"
	PYTHONPATH=. python examples/simple/simple.py --db "$(EXAMPLE_DB)" \
		compute make_experiment '{"difficulty": 3}'
	rm -f "$(EXAMPLE_DB)"

test_adder:
	rm -f "$(EXAMPLE_DB)"
	PYTHONPATH=. python examples/adder/adder_cli.py --db "$(EXAMPLE_DB)" \
		compute add '{"a": 1, "b": 2}'
	rm -f "$(EXAMPLE_DB)"

test_tournament:
	rm -f "$(EXAMPLE_DB)"
	PYTHONPATH=. python examples/tournament/tournament.py --db "$(EXAMPLE_DB)" \
		compute play_tournament '{"players": ["a", "b", "c"]}'
	rm -f "$(EXAMPLE_DB)"
