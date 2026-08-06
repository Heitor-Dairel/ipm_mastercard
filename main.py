from src.data import DB8583
from src.helpers import list_date

with DB8583() as db:
    for i in list_date():
        db.iso_db(file_date=i, file_cycle="CIC1", logging=False)
        db.iso_db(file_date=i, file_cycle="CIC2", logging=False)
        db.iso_db(file_date=i, file_cycle="CIC3", logging=False)
