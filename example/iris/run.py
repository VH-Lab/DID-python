import os
import iris

d = iris.DataLoader(os.path.join(ex.PATH, './iris.data'), ['sepal_length','petal_length','petal_width','type'])
plants = d.read()

d.db.insert()

for plant in plants:
	plant.insert()
