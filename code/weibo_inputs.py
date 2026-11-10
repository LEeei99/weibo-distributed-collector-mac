"""Low-memory input reader for Yuqingtong Weibo Excel exports.

Only the Python standard library is required. Text values (including dates and
counts) are preserved exactly as stored in the workbook. No source file is
modified, and worksheet XML is consumed row by row rather than loaded in full.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
import posixpath
import re
import sqlite3
import tempfile
import warnings
import xml.etree.ElementTree as ET
import zipfile


EXPORT_COLUMNS = (
    "序号", "标题/微博内容", "评论内容", "原文/评论链接", "来源网站", "日期",
    "媒体类型", "原文作者", "信息属性", "摘要", "信源地域", "信源级别",
    "精准地域", "涉及行业", "相似文章数", "用户类型", "原创/转发", "账号类型",
    "涉及词", "转发/评论作者", "转发数", "评论数", "点赞数", "粉丝数",
)
LINK_COLUMN = "原文/评论链接"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
CELL_REFERENCE = re.compile(r"^([A-Za-z]+)([1-9][0-9]*)$")


class ExportInputError(ValueError):
    """The input is not a readable Yuqingtong XLSX export."""


def _region(path: Path) -> str:
    for part in reversed(path.parts[:-1]):
        match = re.match(r"^(京津冀|长三角|粤港澳)", part)
        if match:
            return match.group(1)
    return path.parent.name


def _text(element: ET.Element | None) -> str:
    """Combine rich-text runs but exclude optional phonetic annotations."""
    if element is None:
        return ""
    parts = []
    for child in element:
        if child.tag == NS + "t":
            parts.append(child.text or "")
        elif child.tag == NS + "r":
            parts.extend(node.text or "" for node in child.findall(NS + "t"))
    return "".join(parts)


class _SharedStrings:
    """Create a disk lookup only when a shared-string cell is encountered."""

    def __init__(self, archive: zipfile.ZipFile, cache_root: str | Path | None = None):
        self.archive = archive
        self.cache_root = Path(cache_root).expanduser().resolve() if cache_root else Path(tempfile.gettempdir()) / "weibo_xlsx_sharedstrings"
        self.connection: sqlite3.Connection | None = None
        self.directory: tempfile.TemporaryDirectory | None = None

    def _build(self) -> None:
        if "xl/sharedStrings.xml" not in self.archive.namelist():
            raise ExportInputError("A shared-string cell has no sharedStrings.xml.")
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(prefix="strings-", dir=self.cache_root)
        self.connection = sqlite3.connect(str(Path(self.directory.name) / "strings.sqlite3"))
        self.connection.execute("CREATE TABLE strings (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        batch: list[tuple[int, str]] = []
        index = 0
        stack: list[ET.Element] = []
        with self.archive.open("xl/sharedStrings.xml") as stream:
            for event, element in ET.iterparse(stream, events=("start", "end")):
                if event == "start":
                    stack.append(element)
                    continue
                if element.tag == NS + "si":
                    batch.append((index, _text(element)))
                    index += 1
                    if len(batch) >= 1000:
                        self.connection.executemany("INSERT INTO strings VALUES (?, ?)", batch)
                        batch.clear()
                    if len(stack) > 1:
                        stack[-2].remove(element)
                    element.clear()
                stack.pop()
        if batch:
            self.connection.executemany("INSERT INTO strings VALUES (?, ?)", batch)
        self.connection.commit()

    def get(self, value: str) -> str:
        if self.connection is None:
            self._build()
        try:
            index = int(value)
        except (TypeError, ValueError) as exc:
            raise ExportInputError(f"Invalid shared-string index: {value!r}") from exc
        assert self.connection is not None
        found = self.connection.execute("SELECT value FROM strings WHERE id = ?", (index,)).fetchone()
        if found is None:
            raise ExportInputError(f"Shared-string index {index} is outside the string table.")
        return found[0]

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
        if self.directory is not None:
            self.directory.cleanup()


def _column_number(letters: str) -> int:
    number = 0
    for letter in letters.upper():
        number = number * 26 + ord(letter) - ord("A") + 1
    return number - 1


def _cells(row: ET.Element, strings: _SharedStrings) -> tuple[dict[int, str], list[str]]:
    cells: dict[int, str] = {}
    problems: list[str] = []
    for cell in row.findall(NS + "c"):
        reference = cell.get("r", "")
        match = CELL_REFERENCE.match(reference)
        if match is None:
            problems.append(f"Ignored cell with malformed reference {reference!r}.")
            continue
        column = _column_number(match.group(1))
        if column in cells:
            problems.append(f"Ignored duplicate cell {reference}.")
            continue
        kind = cell.get("t", "")
        raw = cell.findtext(NS + "v", default="")
        try:
            if kind == "inlineStr":
                value = _text(cell.find(NS + "is"))
            elif kind == "s":
                value = strings.get(raw)
            elif kind == "e":
                value = ""
                problems.append(f"Excel error in {reference}: {raw}")
            else:
                # Numeric cells, ISO date cells and cached formula values remain
                # strings. The reader never guesses a timezone or number type.
                value = raw
        except ExportInputError as exc:
            value = ""
            problems.append(f"{reference}: {exc}")
        cells[column] = value
    return cells, problems


def _sheets(archive: zipfile.ZipFile) -> list[tuple[str, str]]:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    targets = {
        node.get("Id"): node.get("Target", "")
        for node in relationships
        if node.get("TargetMode") != "External"
        and node.get("Type", "").endswith("/worksheet")
    }
    result = []
    for sheet in workbook.findall(NS + "sheets/" + NS + "sheet"):
        target = targets.get(sheet.get(REL_NS + "id"))
        if not target:
            continue
        name = posixpath.normpath(target.lstrip("/") if target.startswith("/") else posixpath.join("xl", target))
        if name.startswith("../") or name not in archive.namelist():
            raise ExportInputError(f"Missing or invalid worksheet target: {target!r}")
        result.append((sheet.get("name", ""), name))
    if not result:
        raise ExportInputError("Workbook has no readable worksheets.")
    return result


def _iter_export_rows(
    path: str | Path,
    max_rows: int | None = None,
    *,
    stop_excel_row: int | None = None,
    minimum_rows: int = 0,
    cache_root: str | Path | None = None,
) -> Iterator[dict]:
    source = Path(path).expanduser().resolve()
    if max_rows is not None and (not isinstance(max_rows, int) or max_rows < 0):
        raise ValueError("max_rows must be a nonnegative integer or None.")
    if max_rows == 0:
        return
    count = 0
    try:
        with zipfile.ZipFile(source) as archive:
            strings = _SharedStrings(archive, cache_root=cache_root)
            try:
                for sheet_name, member in _sheets(archive):
                    headers: dict[int, str] | None = None
                    stack: list[ET.Element] = []
                    previous_row = 0
                    with archive.open(member) as stream:
                        for event, element in ET.iterparse(stream, events=("start", "end")):
                            if event == "start":
                                stack.append(element)
                                continue
                            if element.tag != NS + "row":
                                stack.pop()
                                continue
                            try:
                                excel_row = int(element.get("r", previous_row + 1))
                                if excel_row < 1:
                                    raise ValueError
                            except (TypeError, ValueError):
                                excel_row = previous_row + 1
                            previous_row = excel_row
                            if stop_excel_row is not None and excel_row > stop_excel_row and count >= minimum_rows:
                                return
                            values, problems = _cells(element, strings)
                            if len(stack) > 1:
                                stack[-2].remove(element)
                            element.clear()
                            stack.pop()
                            if not any(value.strip() for value in values.values()):
                                if stop_excel_row is not None and excel_row >= stop_excel_row and count >= minimum_rows:
                                    return
                                continue
                            if headers is None:
                                headers = {column: value.strip().lstrip("\ufeff") for column, value in values.items() if value.strip()}
                                if LINK_COLUMN not in headers.values():
                                    raise ExportInputError(f"{source.name}, {sheet_name}: header is missing {LINK_COLUMN!r}.")
                                if len(headers) != len(set(headers.values())):
                                    raise ExportInputError(f"{source.name}, {sheet_name}: duplicate header labels.")
                                continue
                            record = {column: "" for column in EXPORT_COLUMNS}
                            record.update({label: values.get(column, "") for column, label in headers.items()})
                            record.update({
                                "_source_filepath": str(source),
                                "_source_folder": source.parent.name,
                                "_region": _region(source),
                                "_sheet": sheet_name,
                                "_excel_row": excel_row,
                            })
                            missing = [column for column in EXPORT_COLUMNS if column not in headers.values()]
                            if missing:
                                problems.append("Export headers missing: " + ", ".join(missing))
                            if problems:
                                record["_input_warnings"] = problems
                            yield record
                            count += 1
                            if max_rows is not None and count >= max_rows:
                                return
                            if stop_excel_row is not None and excel_row >= stop_excel_row and count >= minimum_rows:
                                return
            finally:
                strings.close()
    except (OSError, zipfile.BadZipFile, KeyError, ET.ParseError, RuntimeError) as exc:
        raise ExportInputError(f"Cannot read XLSX export {source}: {exc}") from exc


def export_workbooks(input_root: str | Path, workbook_paths: Iterable[str | Path] | None = None) -> list[Path]:
    """Resolve the exact input workbook list without opening or modifying it."""
    root = Path(input_root).expanduser().resolve()
    if workbook_paths is not None:
        paths = [Path(path).expanduser() for path in workbook_paths]
        paths = [path.resolve() if path.is_absolute() else (root / path).resolve() for path in paths]
    elif root.is_file():
        paths = [root]
    elif root.is_dir():
        paths = sorted(path for path in root.rglob('*.xlsx') if not path.name.startswith('~$'))
    else:
        raise ExportInputError(f'Export input is not accessible: {root}')
    paths = list(dict.fromkeys(paths))
    if not paths:
        raise ExportInputError(f'No XLSX exports found in {root}.')
    for path in paths:
        if path.suffix.lower() != '.xlsx' or not path.is_file():
            raise ExportInputError(f'Export workbook is not accessible: {path}')
    return paths


def iter_export_rows(
    input_root: str | Path,
    max_rows: int | None = None,
    *,
    workbook_paths: Iterable[str | Path] | None = None,
    cache_root: str | Path | None = None,
) -> Iterator[dict]:
    """Stream a workbook or input folder, retaining all 24 columns and provenance.

    An explicit ``workbook_paths`` whitelist is relative to ``input_root`` unless
    absolute. ``max_rows`` is an overall nonblank data-row limit across workbooks;
    it preserves the earlier single-workbook interface. No data-row list is built.
    ``cache_root`` controls the disk lookup for shared strings. The default uses
    the local system temporary folder instead of the working directory.
    Close a partly consumed iterator to release its XLSX stream immediately.
    """
    if max_rows is not None and (not isinstance(max_rows, int) or max_rows < 0):
        raise ValueError('max_rows must be a nonnegative integer or None.')
    if max_rows == 0:
        return
    seen = 0
    for path in export_workbooks(input_root, workbook_paths):
        iterator = _iter_export_rows(path, cache_root=cache_root)
        try:
            for record in iterator:
                yield record
                seen += 1
                if max_rows is not None and seen >= max_rows:
                    return
        finally:
            iterator.close()


def sample_exports(
    root: str | Path,
    per_region: int = 10,
    include_rows: Mapping[str, Iterable[int]] | None = None,
    *,
    cache_root: str | Path | None = None,
) -> list[dict]:
    """Take the first data rows of each export and optional exact Excel rows.

    ``include_rows`` keys can be a region (e.g. ``长三角``), parent-folder name,
    filename or full source path. Excel row numbers include the header: use
    ``{"长三角": [470, 2071]}`` for the pilot's additional control posts. Reading
    stops once the initial sample and requested rows have been considered. This
    is a feasibility sample, not a random or statistically representative sample.
    Invalid workbooks are reported with a warning and the others are retained.
    """
    if not isinstance(per_region, int) or per_region < 0:
        raise ValueError("per_region must be a nonnegative integer.")
    directory = Path(root).expanduser().resolve()
    if not directory.is_dir():
        raise ExportInputError(f"Export folder is not accessible: {directory}")
    files = sorted(path for path in directory.rglob("*.xlsx") if not path.name.startswith("~$"))
    if not files:
        raise ExportInputError(f"No XLSX exports found in {directory}.")
    samples: list[dict] = []
    for path in files:
        requested: set[int] = set()
        keys = {_region(path), path.parent.name, path.name, str(path), path.as_posix()}
        for key, rows in (include_rows or {}).items():
            if str(key) in keys:
                for row in rows:
                    if not isinstance(row, int) or row < 2:
                        raise ValueError("Requested Excel rows must be integers of at least 2.")
                    requested.add(row)
        if per_region == 0 and not requested:
            continue
        taken = 0
        last_requested = max(requested, default=0)
        iterator = _iter_export_rows(path, stop_excel_row=last_requested, minimum_rows=per_region, cache_root=cache_root)
        try:
            for record in iterator:
                excel_row = record["_excel_row"]
                initial = taken < per_region
                explicit = excel_row in requested
                if initial or explicit:
                    record["_sample_reason"] = [reason for reason, yes in (("first_rows", initial), ("requested_excel_row", explicit)) if yes]
                    samples.append(record)
                    if initial:
                        taken += 1
                if taken >= per_region and excel_row >= last_requested:
                    break
        except ExportInputError as exc:
            warnings.warn(str(exc), RuntimeWarning, stacklevel=2)
        finally:
            iterator.close()
    return samples


__all__ = ["EXPORT_COLUMNS", "LINK_COLUMN", "ExportInputError", "export_workbooks", "iter_export_rows", "sample_exports"]
