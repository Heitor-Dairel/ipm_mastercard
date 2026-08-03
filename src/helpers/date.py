from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def list_date() -> list[str]:

    FORMAT: str = "%d/%m/%Y"

    dates: list[str] = []

    date_end: date = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    date_init: date = date_end - timedelta(days=10)

    while date_init <= date_end:
        dates.append(date_init.strftime(FORMAT))

        date_init += timedelta(days=1)

    return dates
