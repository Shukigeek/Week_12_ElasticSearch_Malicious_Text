import csv


class CSVLoader:
    def __init__(self, path):
        self.path = path

    def load(self):
        try:
            with open(self.path, newline='', encoding="utf-8") as f:
                return list(csv.DictReader(f))
        except Exception as e:
            raise "data didn't change good to dict"