from typing import (
    Final,
    Dict,
    List,
    Literal,
    NamedTuple,
    Iterator,
    TypeAlias,
    Tuple,
    Optional,
)
from pathlib import Path
from datetime import datetime
from rich import print

_BASE_DIR: Final[Path] = Path(
    r"C:\Users\heitor.tavares\OneDrive - TRIVALE ADMINISTRACAO LTDA"
    r"\Operação Processadora - Arquivos CSU"
)
_FLAG_FOLDER: Final[str] = "(1)"
_FORMAT_DATE_TIME: Final[str] = "%d/%m/%Y %H:%M:%S"
_FORMAT_DATE: Final[str] = "%d/%m/%Y"


class TupleManagerFile(NamedTuple):

    file_name: str
    file_dt_time: str
    bytes_file: memoryview


FileLoadData: TypeAlias = Dict[str, Dict[str, TupleManagerFile]]
FileLoadProcessadora: TypeAlias = Dict[str, Dict[str, TupleManagerFile]]


class DateInvalidFormat(ValueError): ...


def _validate_date(date: str) -> None:

    formato: str = "%d/%m/%Y"
    msg: str = f"Formato de data está inválido '{date}'"
    try:
        datetime.strptime(date, formato)
    except ValueError as e:
        raise DateInvalidFormat(msg) from e


def _get_bytes_to_file(file_path: Path) -> memoryview:
    data_bytes: bytes = file_path.read_bytes()
    raw: memoryview = memoryview(data_bytes)

    return raw


def _format_outgoing_files(file_path: Path) -> Tuple[str, ...]:

    file_name = file_path.name
    file_name_parts: List[str] = file_path.stem.split("_")
    file_dt: str = file_name_parts[-2]
    file_time: str = file_name_parts[-1]
    f_dt_time_str: str = datetime.strptime(
        f"{file_dt} {file_time}", "%d%m%Y %H%M%S"
    ).strftime(_FORMAT_DATE_TIME)

    return file_name, f_dt_time_str


def _load_outgoing_files(
    file_date: str, cycle: Literal["CIC1", "CIC2", "CIC3"]
) -> Optional[TupleManagerFile]:

    file_date_format: str = datetime.strptime(file_date, "%d/%m/%Y").strftime("%d%m%Y")
    files: Final[Iterator[Path]] = _BASE_DIR.rglob(
        f"CSU_ACQ_MASTER_OUTGOING_{cycle}_{file_date_format}*.TXT"
    )

    file: Optional[TupleManagerFile] = None

    for arq in files:

        if _FLAG_FOLDER not in arq.parent.name:

            file_name, f_dt_time_str = _format_outgoing_files(file_path=arq)
            raw: memoryview = _get_bytes_to_file(arq)
            file = TupleManagerFile(
                file_name=file_name,
                file_dt_time=f_dt_time_str,
                bytes_file=raw,
            )

    return file


def search_arq(
    date_file: str, cycle: Literal["CIC1", "CIC2", "CIC3"]
) -> Optional[TupleManagerFile]:

    _validate_date(date=date_file)
    outgoing_file: Optional[TupleManagerFile] = _load_outgoing_files(
        file_date=date_file, cycle=cycle
    )
    return outgoing_file


if __name__ == "__main__":

    arq = search_arq(date_file="20/03/2026", cycle="CIC1")

    print(arq)
