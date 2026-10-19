"""Counting an upload's columns and data rows before it is stored and queued.

``upload_shape`` has no pandas, so each format is also checked against what
``shared.data_loading`` (the worker's real parser) makes of the same bytes.
"""

import zipfile
from io import BytesIO

import openpyxl
import pytest

from api.simulation.upload_shape import measure
from shared.data_loading import parse_file


def _xlsx(*sheets: list[list[object]]) -> bytes:
    """A workbook with one worksheet per argument, in that order."""
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for index, rows in enumerate(sheets):
        sheet = workbook.create_sheet(f"Sheet {index}")
        for row in rows:
            sheet.append(row)
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def _rezip(
    content: bytes,
    *,
    replace: dict[str, bytes] | None = None,
    rename: dict[str, str] | None = None,
) -> bytes:
    """Copy an xlsx, swapping the content and/or the name of some parts."""
    replace, rename = replace or {}, rename or {}
    source = zipfile.ZipFile(BytesIO(content))
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as out:
        for name in source.namelist():
            data = replace.get(name, source.read(name))
            out.writestr(rename.get(name, name), data)
    return buffer.getvalue()


def _pandas_shape(content: bytes, file_name: str) -> tuple[int, int]:
    frame = parse_file(content, file_name)
    return len(frame.columns), len(frame)


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content",
    [
        b"a,b,production\n1,2,3\n4,5,6\n",
        b"a,b,production\r\n1,2,3\r\n4,5,6\r\n",
        b"a,b,production\n1,2,3\n4,5,6",
        b'"a, quoted",b,production\n1,2,3\n4,5,6\n',
        b"\n\na,b,production\n1,2,3\n4,5,6\n",
    ],
    ids=["lf", "crlf", "no-final-newline", "quoted-header", "leading-blank-lines"],
)
def test_csv_counts_what_the_worker_parses(content):
    assert measure(content, "data.csv") == (3, 2) == _pandas_shape(content, "data.csv")


def test_csv_extension_is_case_insensitive():
    assert measure(b"a,production\n1,2\n", "DATA.CSV") == (2, 1)


def test_header_only_csv_has_no_data_rows():
    assert measure(b"a,production\n", "data.csv") == (2, 0)


# ---------------------------------------------------------------------------
# XLSX
# ---------------------------------------------------------------------------


def test_xlsx_counts_what_the_worker_parses():
    content = _xlsx([["a", "b", "c", "production"], *[[1, 2, 3, 4]] * 5])

    assert measure(content, "data.xlsx") == (4, 5) == _pandas_shape(content, "data.xlsx")


def test_xlsx_reads_the_first_sheet_like_pandas():
    # pd.read_excel (and so the worker) reads the first sheet only.
    content = _xlsx(
        [["x", "y", "z"], [1, 2, 3]],
        [["a", "production"], *[[1, 2]] * 9],
    )

    assert measure(content, "data.xlsx") == (3, 1) == _pandas_shape(content, "data.xlsx")


def test_xlsx_follows_a_relative_relationship_to_the_sheet_part():
    # openpyxl writes an absolute target to "sheet1.xml"; Excel writes relative
    # ones, and nothing obliges any writer to keep that part name.
    content = _xlsx([["a", "production"], *[[1, 2]] * 7])
    rels = zipfile.ZipFile(BytesIO(content)).read("xl/_rels/workbook.xml.rels")
    moved = _rezip(
        content,
        replace={
            "xl/_rels/workbook.xml.rels": rels.replace(
                b'Target="/xl/worksheets/sheet1.xml"', b'Target="worksheets/data.xml"'
            )
        },
        rename={"xl/worksheets/sheet1.xml": "xl/worksheets/data.xml"},
    )

    assert measure(moved, "data.xlsx") == (2, 7)


def test_xlsx_without_a_dimension_record_is_unknown():
    content = _xlsx([["a", "production"], [1, 2]])
    sheet = zipfile.ZipFile(BytesIO(content)).read("xl/worksheets/sheet1.xml")
    start = sheet.index(b"<dimension")
    end = sheet.index(b"/>", start) + 2
    stripped = _rezip(content, replace={"xl/worksheets/sheet1.xml": sheet[:start] + sheet[end:]})

    assert measure(stripped, "data.xlsx") is None


# ---------------------------------------------------------------------------
# Anything else is "unknown", never an error
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("content", "file_name"),
    [
        (b"not a zip at all", "data.xlsx"),
        (b"", "data.csv"),
        (b"a,production\n1,2\n", "data.txt"),
        (b"a,production\n1,2\n", "uploaded-file"),
    ],
    ids=["corrupt-xlsx", "empty-csv", "other-extension", "no-extension"],
)
def test_unreadable_or_unsupported_uploads_are_unknown(content, file_name):
    assert measure(content, file_name) is None
