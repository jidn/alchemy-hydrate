import io

from alchemy_hydrate import LowerCaseDictReader


def test_dict_reder():
    csv_data = io.StringIO("Name,Age,City\nAlice,30,New York\nBob,25,Chicago\n")
    lower_case_reader = LowerCaseDictReader(csv_data)
    assert lower_case_reader.fieldnames is not None
    assert ["name", "age", "city"] == list(lower_case_reader.fieldnames)

    lower_case_reader.fieldnames = [
        header.lower() for header in lower_case_reader.fieldnames
    ]
    assert list(lower_case_reader) == [
        {"name": "Alice", "age": "30", "city": "New York"},
        {"name": "Bob", "age": "25", "city": "Chicago"},
    ]
