"""Count an uploaded file's columns and data rows without parsing it.

``POST /`` refuses a run that would be too large (``shared.problem_size``)
before storing and queueing its file, but the API carries neither pandas nor
openpyxl (``requirements/api.txt``). These counts follow what
``shared.data_loading`` will see closely enough for that check, and err on the
high side:

* CSV: the fields of the header line, and one data row per line after it.
* XLSX: the ``<dimension>`` record of the first worksheet, which Excel,
  LibreOffice and openpyxl all write. It also spans formatted-but-empty cells.

``None`` means "could not tell" (no dimension record, not a zip, any other
extension). The caller then lets the upload through, and the worker, which
checks the exact parsed matrix, is the backstop.

Regexes rather than an XML parser: only a few attributes are needed, every read
is bounded, and nothing here may expand attacker-controlled XML.
"""

import csv
import posixpath
import re
import zipfile
from io import BytesIO

# workbook.xml and its relationships part are a few KB; refuse to inflate more.
_MAX_INDEX_PART_BYTES = 2 * 1024 * 1024
# The dimension record precedes <sheetData>, so the head of the sheet is enough.
_SHEET_HEAD_BYTES = 64 * 1024

_FIRST_SHEET = re.compile(rb"<(?:\w+:)?sheet\b[^>]*?\s(?:\w+:)id=\"([^\"]+)\"")
_RELATIONSHIP = re.compile(rb"<(?:\w+:)?Relationship\b[^>]*>")
_REL_ID = re.compile(rb"\sId=\"([^\"]+)\"")
_REL_TARGET = re.compile(rb"\sTarget=\"([^\"]+)\"")
_DIMENSION = re.compile(rb"<(?:\w+:)?dimension\b[^>]*?\sref=\"(?:[A-Za-z]+\d+:)?([A-Za-z]+)(\d+)\"")


def measure(content: bytes, file_name: str) -> tuple[int, int] | None:
    """Return ``(columns, data_rows)`` for an upload, or ``None`` when unknown."""
    # Same extension rule as shared.data_loading.parse_file.
    extension = file_name.rsplit(".", 1)[-1].lower()
    try:
        if extension == "csv":
            return _measure_csv(content)
        if extension in ("xlsx", "xls"):
            return _measure_xlsx(content)
    except Exception:
        # Corrupt, encrypted or exotic files are the worker's to reject with a
        # real parse error; failing to count them here only means "unknown".
        return None
    return None


def _measure_csv(content: bytes) -> tuple[int, int] | None:
    # pandas skips blank lines, so the header is the first non-empty line.
    content = content.lstrip(b"\r\n")
    if not content:
        return None
    header = content.split(b"\n", 1)[0].decode("utf-8", errors="replace").rstrip("\r")
    columns = len(next(csv.reader([header])))
    lines = content.count(b"\n") + (0 if content.endswith(b"\n") else 1)
    return columns, lines - 1


def _measure_xlsx(content: bytes) -> tuple[int, int] | None:
    with zipfile.ZipFile(BytesIO(content)) as archive:
        sheet_part = _first_sheet_part(archive)
        with archive.open(sheet_part) as part:
            head = part.read(_SHEET_HEAD_BYTES)
    dimension = _DIMENSION.search(head)
    if dimension is None:
        return None
    last_column, last_row = dimension.groups()
    # The first row is the header; pandas reads columns from A onwards.
    return _column_number(last_column.decode()), max(int(last_row) - 1, 0)


def _first_sheet_part(archive: zipfile.ZipFile) -> str:
    """Path of the first worksheet, the one ``pd.read_excel`` reads by default."""
    sheet = _FIRST_SHEET.search(_read_index_part(archive, "xl/workbook.xml"))
    if sheet is None:
        raise KeyError("workbook lists no sheet")
    rels = _read_index_part(archive, "xl/_rels/workbook.xml.rels")
    for relationship in _RELATIONSHIP.findall(rels):
        rel_id, target = _REL_ID.search(relationship), _REL_TARGET.search(relationship)
        if rel_id and target and rel_id.group(1) == sheet.group(1):
            path = target.group(1).decode()
            if path.startswith("/"):
                return path.lstrip("/")
            return posixpath.normpath(posixpath.join("xl", path))
    raise KeyError("first sheet has no relationship")


def _read_index_part(archive: zipfile.ZipFile, name: str) -> bytes:
    if archive.getinfo(name).file_size > _MAX_INDEX_PART_BYTES:
        raise ValueError(f"{name} is implausibly large")
    return archive.read(name)


def _column_number(letters: str) -> int:
    """Spreadsheet column letters to a 1-based number: A -> 1, Z -> 26, AA -> 27."""
    number = 0
    for letter in letters.upper():
        number = number * 26 + ord(letter) - ord("A") + 1
    return number
