import os
import did 

PATH = os.path.split(os.path.abspath(__file__))[0]

did.load_external_configuration(os.path.join(PATH, 'config.env'))
did.set_variable('DIDDOCUMENT_EX1', did.get_documentpath())

from .iris_plant import IrisPlant
from .data_loader import DataLoader