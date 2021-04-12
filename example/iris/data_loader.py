import os
import pandas as pd
from .iris_plant import IrisPlant
from did.schema import Schema

PATH = os.path.split(os.path.abspath(__file__))[0]


class DataLoader(Schema):
    document_type = "data_loader.json"

    def __init__(self, data_location, column_names):
        self.file_location = data_location
        self.column_names = column_names

    def read(self):
        df = pd.read_csv(file_location)
        plants = []
        for index, row in df.iterrows():
            args = {}
            for col in column_names:
                args[col] = row[col]
            plants.append(IrisPlant(**args))
        return plants