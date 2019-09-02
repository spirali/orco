import pytest

from orco.cfggen import build_config


def test_config_simple():
    config = build_config({
        "a": 5,
        "b": "hello",
        "c": ["hello", "world"],
        "d": {
            "key": {
                "orco": ["organized", "computing"]
            }
        }
    })
    assert config["a"] == 5
    assert config["b"] == "hello"
    assert config["c"] == ["hello", "world"]
    assert config["d"] == {"key": {"orco": ["organized", "computing"]}}


def test_config_task():
    config = build_config({
        "a": [{
            "$ref": "b"
        }, {
            "$ref": "c"
        }],
        "b": "hello",
        "c": ["hello", "world"],
    })
    assert config["a"] == ["hello", ["hello", "world"]]


def test_config_task_cycle():
    with pytest.raises(Exception, match=".*cycle.*"):
        build_config({
            "a": [{
                "$ref": "b"
            }, {
                "$ref": "c"
            }],
            "b": {
                "$ref": "a"
            },
            "c": ["hello", "world"],
        })


def test_config_range():
    assert build_config({"$range": 5}) == list(range(5))

    assert build_config({"$range": [2, 5]}) == list(range(2, 5))

    assert build_config({"$range": [3, 40, 5]}) == list(range(3, 40, 5))


def test_config_concat():
    assert build_config({"$+": [[1, 2], [3, 4]]}) == [1, 2, 3, 4]

    assert build_config({
        "a": {
            "$+": [{
                "$ref": "b"
            }, {
                "$ref": "c"
            }, {
                "$ref": "b"
            }, [4, 5]]
        },
        "b": [1, 2, 3],
        "c": [4, 5, 6]
    })["a"] == [1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5]


def test_config_product():
    assert build_config({
        "$product": [{
            "$range": 2
        }, [3, 4]]
    }) == [(0, 3), (0, 4), (1, 3), (1, 4)]

    assert build_config({
        "b": 1,
        "a": {
            "$product": {
                "a": [{
                    "$ref": "b"
                }, 2],
                "b": ["a", "b"],
                "c": [4, 5]
            }
        }
    })["a"] == [
        {
            "a": 1,
            "b": "a",
            "c": 4
        },
        {
            "a": 1,
            "b": "a",
            "c": 5
        },
        {
            "a": 1,
            "b": "b",
            "c": 4
        },
        {
            "a": 1,
            "b": "b",
            "c": 5
        },
        {
            "a": 2,
            "b": "a",
            "c": 4
        },
        {
            "a": 2,
            "b": "a",
            "c": 5
        },
        {
            "a": 2,
            "b": "b",
            "c": 4
        },
        {
            "a": 2,
            "b": "b",
            "c": 5
        },
    ]


def test_config_product_nested_unwrapped():
    assert build_config({
            "$product": {
                "a": {
                    "$product": {
                        "x": [1, 2],
                        "y": [3, 4]
                    }
                },
                "b": ["a", "b"],
            }
        }) == [{
            'a': {
                'x': 1,
                'y': 3
            },
            'b': 'a'
        }, {
            'a': {
                'x': 1,
                'y': 3
            },
            'b': 'b'
        }, {
            'a': {
                'x': 1,
                'y': 4
            },
            'b': 'a'
        }, {
            'a': {
                'x': 1,
                'y': 4
            },
            'b': 'b'
        }, {
            'a': {
                'x': 2,
                'y': 3
            },
            'b': 'a'
        }, {
            'a': {
                'x': 2,
                'y': 3
            },
            'b': 'b'
        }, {
            'a': {
                'x': 2,
                'y': 4
            },
            'b': 'a'
        }, {
            'a': {
                'x': 2,
                'y': 4
            },
            'b': 'b'
        }]


def test_config_product_nested_wrapped():
    assert build_config({
            "$product": {
                "a": [{
                    "$product": {
                        "x": [1, 2],
                        "y": [3, 4]
                    }
                }],
                "b": ["a", "b"],
            }
        }) == [{
            'a': [{
                'x': 1,
                'y': 3
            }, {
                'x': 1,
                'y': 4
            }, {
                'x': 2,
                'y': 3
            }, {
                'x': 2,
                'y': 4
            }],
            'b': 'a'
        }, {
            'a': [{
                'x': 1,
                'y': 3
            }, {
                'x': 1,
                'y': 4
            }, {
                'x': 2,
                'y': 3
            }, {
                'x': 2,
                'y': 4
            }],
            'b': 'b'
        }]


def test_config_zip():
    assert build_config({
            "$product": {
                "a": {
                    "$zip": [["a", "b", "c"], [1, 2, 3]]
                },
                "b": ["a", "b"],
            }
        }) == [{
            'a': ('a', 1),
            'b': 'a'
        }, {
            'a': ('a', 1),
            'b': 'b'
        }, {
            'a': ('b', 2),
            'b': 'a'
        }, {
            'a': ('b', 2),
            'b': 'b'
        }, {
            'a': ('c', 3),
            'b': 'a'
        }, {
            'a': ('c', 3),
            'b': 'b'
        }]


def test_config_top_level_product():
    configurations = build_config({
        "$product": {
            "train_iterations": [100, 200, 300],
            "batch_size": [128, 256],
            "architecture": ["model1", "model2"]
        }
    })
    assert configurations == [{
        'train_iterations': 100,
        'batch_size': 128,
        'architecture': 'model1'
    }, {
        'train_iterations': 100,
        'batch_size': 128,
        'architecture': 'model2'
    }, {
        'train_iterations': 100,
        'batch_size': 256,
        'architecture': 'model1'
    }, {
        'train_iterations': 100,
        'batch_size': 256,
        'architecture': 'model2'
    }, {
        'train_iterations': 200,
        'batch_size': 128,
        'architecture': 'model1'
    }, {
        'train_iterations': 200,
        'batch_size': 128,
        'architecture': 'model2'
    }, {
        'train_iterations': 200,
        'batch_size': 256,
        'architecture': 'model1'
    }, {
        'train_iterations': 200,
        'batch_size': 256,
        'architecture': 'model2'
    }, {
        'train_iterations': 300,
        'batch_size': 128,
        'architecture': 'model1'
    }, {
        'train_iterations': 300,
        'batch_size': 128,
        'architecture': 'model2'
    }, {
        'train_iterations': 300,
        'batch_size': 256,
        'architecture': 'model1'
    }, {
        'train_iterations': 300,
        'batch_size': 256,
        'architecture': 'model2'
    }]
