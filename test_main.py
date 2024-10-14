"""
Test Lib

"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query


def test_extract():
    ext_test = extract()
    assert ext_test is not None


def test_load():
    l_test = load()
    assert l_test == "Load Success"


def test_query():
    q_test = query()
    assert q_test == "Query Success"


if __name__ == "__main__":
    test_extract()
    test_load()
    test_query()
