from ..core import MastercardISO8583Parse
from typing import Optional
from dotenv import load_dotenv
import os


file = MastercardISO8583Parse()

arq_parse = file.parse_ipm(date_file="25/03/2026", cycle="CIC1")

if arq_parse:
    print(arq_parse[1])

load_dotenv()

host: Optional[str] = os.getenv("DB_HOST")
port: Optional[str] = os.getenv("DB_PORT")
db_name: Optional[str] = os.getenv("DB_NAME")
user: Optional[str] = os.getenv("DB_USERNAME")
password: Optional[str] = os.getenv("DB_PASSWORD")


print(host, port, db_name, user, password)
