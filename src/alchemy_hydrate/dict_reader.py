import csv


class LowerCaseDictReader(csv.DictReader):
    """A CSV DictReader that forces all header names to lowercase.

    Use it the same as any csv.DictReader.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fieldnames = (
            [header.lower() for header in self.fieldnames] if self.fieldnames else None
        )
