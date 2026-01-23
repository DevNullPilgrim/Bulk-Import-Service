import csv
import io
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ErrorRow:
    row: int
    error: str
    raw: str


def build_errors_csv(rows: Iterable[ErrorRow]) -> bytes:
    out = io.StringIO(newline='')
    write = csv.writer(out)
    write.writerow(['row', 'error', 'raw'])

    for r in rows:
        write.writerow([r.row, r.error, r.raw])
    return out.getvalue().encode('utf-8')
