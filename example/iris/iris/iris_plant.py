from did.schema import Schema

class IrisPlant(Schema):
    def __init__(
        type,
        sepal_length,
        sepal_width,
        petal_length,
        petal_width
    ):
        self.type = type
        self.sepal_length = sepal_length
        self.sepal_width = sepal_width
        self.petal_length = petal_length
        self.petal_width = petal_width

    def __str__(self):
        return "<IrisPlant(type={}, sepal_length={}, petal_length={}, petal_width={})>".format(
            self.type, self.sepal_length, self.petal_length, self.petal_width
        )
    
    def __repr__(self):
        return "<type={}, sepal_length={}, petal_length={}, petal_width={}>".format(
            self.type, self.sepal_length, self.petal_length, self.petal_width
        )