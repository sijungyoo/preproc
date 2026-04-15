from __future__ import annotations

import datetime
import glob
import os
from dataclasses import dataclass

import numpy as np
import pandas as pd
from PySide6 import QtCore, QtGui, QtWidgets

# ---------------------------------------------------------------------------
# Constants / Defaults
# ---------------------------------------------------------------------------
_today = datetime.date.today().strftime("%y%m%d")
DEFAULT_OUTPUT_DIR = rf"D:\Multimedia\upload\output_{_today}"

DEFAULT_VOLTAGE_COL = "VMeasCh2"   # UI label: 전압 컬럼명
DEFAULT_CURRENT_COL = "ID"         # UI label: 전류 컬럼명
DEFAULT_THRES_CUR = 1e-7
DEFAULT_MIN_INTERVAL = 1e-5

TIME_COL = "TimeOutput"
MAX_ROWS = 100

MEASURE_TYPES = ["ISPP", "Endurance", "Retention", "Custom"]
FILE_TYPES = ["xls", "nasca", "csv"]
POLARITIES = ["PGM", "ERS", "PGM/ERS", "ERS/PGM"]

_EXT_MAP = {"xls": "*.xls", "nasca": "*.xls", "csv": "*.csv"}

DEFAULT_MEASURE_CONFIG = {
    "ISPP": {
        "target_params": "V_min,V_max,V_step",
        "label_header": "Write_V",
        "condition_params": "",
        "polarity": "PGM",
    },
    "Retention": {
        "target_params": "Retention_min,Retention_max",
        "label_header": "Retention",
        "condition_params": "",
        "polarity": "PGM",
    },
    "Endurance": {
        "target_params": "Cycle",
        "label_header": "Cycle",
        "condition_params": "",
        "polarity": "PGM",
    },
}


# ---------------------------------------------------------------------------
# Small Utils
# ---------------------------------------------------------------------------

def lower_first_char(text: str) -> str:
    if not text:
        return text
    return text[:1].lower() + text[1:]


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_xls(filepath: str) -> pd.DataFrame:
    import xlrd

    wb = xlrd.open_workbook(filepath)
    sheet = wb.sheet_by_index(0)
    headers = sheet.row_values(0)
    rows = [sheet.row_values(r) for r in range(1, sheet.nrows)]
    return pd.DataFrame(rows, columns=headers)


def load_nasca(filepath: str) -> pd.DataFrame:
    import xlwings as xw

    app = xw.App(visible=False)
    try:
        wb = app.books.open(filepath)
        sheet = wb.sheets[0]
        data = sheet.used_range.value
        wb.close()
    finally:
        app.quit()
    if not data:
        return pd.DataFrame()
    headers = data[0]
    rows = data[1:]
    return pd.DataFrame(rows, columns=headers)


def load_csv(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath)


def load_file(filepath: str, file_type: str) -> pd.DataFrame:
    if file_type == "xls":
        return load_xls(filepath)
    if file_type == "nasca":
        return load_nasca(filepath)
    if file_type == "csv":
        return load_csv(filepath)
    raise ValueError(f"Unknown file type: {file_type}")


def _find_metadata_header_row(rows: list[list]) -> int:
    for i, row in enumerate(rows):
        normalized = [str(v).strip().lower() for v in row]
        if "parameter name" in normalized and "value" in normalized:
            return i
    raise ValueError("Sheet3에서 'Parameter Name' / 'Value' 헤더를 찾지 못했습니다.")


def load_metadata_from_sheet3(filepath: str, file_type: str) -> dict[str, str]:
    rows: list[list]
    if file_type == "xls":
        import xlrd

        wb = xlrd.open_workbook(filepath)
        if wb.nsheets < 3:
            raise ValueError("xls 파일에 Sheet3(3번째 시트)가 없습니다.")
        sheet = wb.sheet_by_index(2)
        rows = [sheet.row_values(r) for r in range(sheet.nrows)]
    elif file_type == "nasca":
        import xlwings as xw

        app = xw.App(visible=False)
        try:
            wb = app.books.open(filepath)
            if len(wb.sheets) < 3:
                raise ValueError("nasca 파일에 Sheet3(3번째 시트)가 없습니다.")
            data = wb.sheets[2].used_range.value
            wb.close()
        finally:
            app.quit()
        if not data:
            raise ValueError("Sheet3 데이터가 비어 있습니다.")
        rows = data if isinstance(data[0], list) else [data]
    else:
        raise ValueError("Sheet3 metadata는 xls/nasca 에서만 지원됩니다.")

    header_row = _find_metadata_header_row(rows)
    headers = [str(v).strip() for v in rows[header_row]]
    p_idx = next(i for i, h in enumerate(headers) if h.lower() == "parameter name")
    v_idx = next(i for i, h in enumerate(headers) if h.lower() == "value")

    meta: dict[str, str] = {}
    for row in rows[header_row + 1:]:
        if max(p_idx, v_idx) >= len(row):
            continue
        name = str(row[p_idx]).strip()
        if not name:
            continue
        val = str(row[v_idx]).strip()
        meta[name] = val
    if not meta:
        raise ValueError("Sheet3 metadata table에서 유효한 값을 찾지 못했습니다.")
    return meta


def scan_directory(src_dir: str, file_type: str) -> list[str]:
    pattern = os.path.join(src_dir, _EXT_MAP.get(file_type, "*.*"))
    return sorted(glob.glob(pattern))


# ---------------------------------------------------------------------------
# Core Processing
# ---------------------------------------------------------------------------

def detect_subsets(df: pd.DataFrame, min_interval: float) -> list[pd.DataFrame]:
    if TIME_COL not in df.columns:
        raise KeyError(
            f"Column '{TIME_COL}' not found in data. "
            f"Available columns: {list(df.columns)}"
        )

    df = df.copy()
    df[TIME_COL] = pd.to_numeric(df[TIME_COL], errors="coerce")

    diff = df[TIME_COL].diff()
    split_mask = (diff >= min_interval) | (diff.isna())
    split_indices = df.index[split_mask].tolist()

    subsets = []
    for k, start in enumerate(split_indices):
        end = split_indices[k + 1] if k + 1 < len(split_indices) else len(df)
        subset = df.iloc[start:end].reset_index(drop=True)
        if not subset.empty:
            subsets.append(subset)

    return subsets


def extract_parameters(
    subset_df: pd.DataFrame,
    voltage_col: str,
    current_col: str,
    thres_cur: float,
) -> dict:
    if TIME_COL not in subset_df.columns:
        raise KeyError(f"Column '{TIME_COL}' not found.")
    if voltage_col not in subset_df.columns:
        raise KeyError(f"Voltage column '{voltage_col}' not found.")
    if current_col not in subset_df.columns:
        raise KeyError(f"Current column '{current_col}' not found.")

    s = subset_df[[TIME_COL, voltage_col, current_col]].copy()
    s[TIME_COL] = pd.to_numeric(s[TIME_COL], errors="coerce")
    s[voltage_col] = pd.to_numeric(s[voltage_col], errors="coerce")
    s[current_col] = pd.to_numeric(s[current_col], errors="coerce")
    s = s.dropna(subset=[TIME_COL, voltage_col, current_col])
    s = s.sort_values(TIME_COL)
    if s.empty:
        raise ValueError("subset에 유효한 숫자 데이터가 없습니다.")

    def _first_v_at_threshold(threshold: float) -> float:
        hit = s[s[current_col] >= threshold]
        if hit.empty:
            return float("nan")
        return float(hit.iloc[0][voltage_col])

    vth = _first_v_at_threshold(thres_cur)
    vth_2 = _first_v_at_threshold(thres_cur / 10.0)
    ss = vth - vth_2 if pd.notna(vth) and pd.notna(vth_2) else float("nan")
    return {"vth": vth, "vth_2": vth_2, "ss": ss}


def _to_float(meta: dict[str, str], key: str) -> float:
    if key not in meta:
        raise KeyError(f"Sheet3 metadata에 '{key}'가 없습니다.")
    return float(str(meta[key]).replace(",", ""))


def build_measure_labels(
    measure_type: str,
    meta: dict[str, str],
    target_params: list[str],
) -> list[float]:
    if measure_type == "ISPP":
        if len(target_params) != 3:
            raise ValueError("ISPP target_params는 3개(V_min,V_max,V_step)여야 합니다.")
        v_min = _to_float(meta, target_params[0])
        v_max = _to_float(meta, target_params[1])
        v_step = _to_float(meta, target_params[2])
        if v_step == 0:
            raise ValueError("V_step은 0일 수 없습니다.")
        if v_min == v_max:
            return [v_min]

        direction = v_max - v_min
        if direction * v_step < 0:
            raise ValueError("V_min→V_max 방향과 V_step 부호가 일치해야 합니다.")

        labels = []
        cur = v_min
        guard = 0
        eps = abs(v_step) * 1e-9
        if v_step > 0:
            while cur <= v_max + eps:
                labels.append(cur)
                cur += v_step
                guard += 1
                if guard > 1_000_000:
                    raise ValueError("ISPP label 계산이 비정상적으로 길어 중단했습니다.")
            if labels and labels[-1] > v_max:
                labels[-1] = v_max
        else:
            while cur >= v_max - eps:
                labels.append(cur)
                cur += v_step
                guard += 1
                if guard > 1_000_000:
                    raise ValueError("ISPP label 계산이 비정상적으로 길어 중단했습니다.")
            if labels and labels[-1] < v_max:
                labels[-1] = v_max
        return labels

    if measure_type == "Retention":
        if len(target_params) != 2:
            raise ValueError("Retention target_params는 2개(Retention_min,Retention_max)여야 합니다.")
        r_min = _to_float(meta, target_params[0])
        r_max = _to_float(meta, target_params[1])
        if r_min <= 0 or r_max <= 0:
            raise ValueError("Retention_min/max는 양수여야 합니다.")
        labels = []
        cur = r_min
        while cur < r_max:
            labels.append(cur)
            cur *= 10
        labels.append(r_max if cur > r_max else cur)
        return labels

    if measure_type == "Endurance":
        if len(target_params) != 1:
            raise ValueError("Endurance target_params는 1개(Cycle)여야 합니다.")
        cycle = _to_float(meta, target_params[0])
        labels = [0.0, 1.0]
        cur = 10.0
        while cur < cycle:
            labels.append(cur)
            cur *= 10
        labels.append(cycle if cur > cycle else cur)
        dedup = []
        for v in labels:
            if not dedup or dedup[-1] != v:
                dedup.append(v)
        return dedup

    raise ValueError(f"지원하지 않는 measure type: {measure_type}")


def build_polarities(base_polarity: str, label_count: int) -> list[str]:
    if base_polarity in {"PGM", "ERS"}:
        return [base_polarity] * label_count
    if base_polarity in {"PGM/ERS", "ERS/PGM"}:
        a, b = base_polarity.split("/")
        out = []
        for _ in range(label_count):
            out.extend([a, b])
        return out
    raise ValueError(f"지원하지 않는 polarity: {base_polarity}")


def label_subsets(
    subsets: list[pd.DataFrame],
    measure_type: str,
    custom_labels: dict[str, list[str]] | None = None,
    label_header: str | None = None,
    labels: list[float] | None = None,
    polarity_values: list[str] | None = None,
    condition_values: dict[str, str] | None = None,
) -> list[pd.DataFrame]:
    if measure_type == "Custom" and custom_labels:
        labeled = []
        for i, subset in enumerate(subsets):
            s = subset.copy()
            for col_name, values in custom_labels.items():
                s[col_name] = values[i] if i < len(values) else ""
            labeled.append(s)
        return labeled

    if measure_type in {"ISPP", "Endurance", "Retention"}:
        if not label_header or labels is None or polarity_values is None:
            raise ValueError("measure type labeling을 위한 설정이 부족합니다.")
        if len(subsets) != len(polarity_values):
            raise ValueError(
                f"subset 개수({len(subsets)})와 polarity 할당 개수({len(polarity_values)})가 일치하지 않습니다."
            )

        labeled = []
        divisor = len(subsets) // len(labels) if labels else 1
        if divisor not in (1, 2):
            raise ValueError("subset 과 label 대응 비율이 올바르지 않습니다.")
        for i, subset in enumerate(subsets):
            s = subset.copy()
            label_idx = i // divisor
            s[label_header] = labels[label_idx]
            s["polarity"] = polarity_values[i]
            if condition_values:
                for k, v in condition_values.items():
                    s[k] = v
            labeled.append(s)
        return labeled

    return [s.copy() for s in subsets]


def keep_and_rename_columns(
    subset_df: pd.DataFrame,
    voltage_col: str,
    current_col: str,
    extra_cols: list[str],
) -> pd.DataFrame:
    cols_to_keep = []
    for c in [voltage_col, current_col] + extra_cols:
        if c in subset_df.columns and c not in cols_to_keep:
            cols_to_keep.append(c)

    df = subset_df[cols_to_keep].copy()
    rename_map = {}
    if voltage_col in df.columns:
        rename_map[voltage_col] = "voltage"
    if current_col in df.columns:
        rename_map[current_col] = "current"
    return df.rename(columns=rename_map)


def downsample(subset_df: pd.DataFrame, max_rows: int = MAX_ROWS) -> pd.DataFrame:
    if len(subset_df) <= max_rows:
        return subset_df.reset_index(drop=True)
    indices = np.linspace(0, len(subset_df) - 1, max_rows, dtype=int)
    return subset_df.iloc[indices].reset_index(drop=True)


def process_files(
    file_paths: list[str],
    file_type: str,
    output_dir: str,
    voltage_col: str,
    current_col: str,
    thres_cur: float,
    min_interval: float,
    measure_type: str,
    custom_labels: dict[str, list[str]] | None,
    measure_config: dict | None,
    on_progress=None,
    on_message=None,
) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    saved_paths: list[str] = []

    total = len(file_paths)
    for idx, filepath in enumerate(file_paths):
        fname = os.path.basename(filepath)
        if on_progress:
            on_progress(idx / max(total, 1), f"Processing {fname}…")
        try:
            df = load_file(filepath, file_type)

            subsets = detect_subsets(df, min_interval)
            if not subsets:
                if on_message:
                    on_message("warning", f"{fname}: subset이 감지되지 않았습니다 — 건너뜁니다.")
                continue

            param_dicts = [extract_parameters(s, voltage_col, current_col, thres_cur) for s in subsets]

            param_cols = []
            for i, params in enumerate(param_dicts):
                for k, v in params.items():
                    subsets[i][k] = v
                    if k not in param_cols:
                        param_cols.append(k)

            label_cols = []
            if measure_type == "Custom":
                subsets = label_subsets(subsets, measure_type, custom_labels)
                label_cols = list(custom_labels.keys()) if custom_labels else []
            else:
                if file_type not in {"xls", "nasca"}:
                    raise ValueError("ISPP/Retention/Endurance labeling은 xls/nasca 에서만 지원됩니다.")
                if not measure_config:
                    raise ValueError("Measure 설정값이 없습니다.")
                meta = load_metadata_from_sheet3(filepath, file_type)
                target_params = [p.strip() for p in measure_config.get("target_params", "").split(",") if p.strip()]
                label_header = lower_first_char(measure_config.get("label_header", "").strip())
                polarity = measure_config.get("polarity", "PGM").strip()
                labels = build_measure_labels(measure_type, meta, target_params)
                polarity_values = build_polarities(polarity, len(labels))
                if len(subsets) != len(polarity_values):
                    raise ValueError(
                        f"subset 개수({len(subsets)})와 label x polarity 개수({len(polarity_values)})가 일치하지 않습니다."
                    )
                condition_params = [p.strip() for p in measure_config.get("condition_params", "").split(",") if p.strip()]
                condition_values = {k: meta.get(k, "") for k in condition_params}
                subsets = label_subsets(
                    subsets,
                    measure_type,
                    custom_labels=None,
                    label_header=label_header,
                    labels=labels,
                    polarity_values=polarity_values,
                    condition_values=condition_values,
                )
                label_cols = [label_header, "polarity"] + list(condition_values.keys())

            extra_cols = param_cols + label_cols
            processed_subsets = []
            for subset in subsets:
                subset = keep_and_rename_columns(subset, voltage_col, current_col, extra_cols)
                subset = downsample(subset, MAX_ROWS)
                processed_subsets.append(subset)

            result_df = pd.concat(processed_subsets, ignore_index=True)
            stem = os.path.splitext(fname)[0]
            out_path = os.path.join(output_dir, f"{stem}_processed.csv")
            result_df.to_csv(out_path, index=False)
            saved_paths.append(out_path)

        except Exception as e:
            if on_message:
                on_message("error", f"{fname} 처리 중 오류: {e}")

    if on_progress:
        on_progress(1.0, "완료")
    return saved_paths


# ---------------------------------------------------------------------------
# Qt UI
# ---------------------------------------------------------------------------

class MeasureConfigDialog(QtWidgets.QDialog):
    def __init__(self, measure_type: str, current_config: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"{measure_type} 설정")
        self.result: dict | None = None

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        self.target_params_edit = QtWidgets.QLineEdit(current_config.get("target_params", ""))
        self.label_header_edit = QtWidgets.QLineEdit(current_config.get("label_header", ""))
        self.condition_params_edit = QtWidgets.QLineEdit(current_config.get("condition_params", ""))
        self.polarity_box = QtWidgets.QComboBox()
        self.polarity_box.addItems(POLARITIES)
        self.polarity_box.setCurrentText(current_config.get("polarity", "PGM"))

        form.addRow("Target Parameter Name(s)", self.target_params_edit)
        form.addRow("Subset Label Column Header", self.label_header_edit)
        form.addRow("Append Condition Parameter Name(s)", self.condition_params_edit)
        form.addRow("Polarity", self.polarity_box)
        layout.addLayout(form)

        btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        btns.accepted.connect(self._save)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _save(self):
        label_header = self.label_header_edit.text().strip()
        target_params = self.target_params_edit.text().strip()
        if not label_header or not target_params:
            QtWidgets.QMessageBox.critical(self, "입력 오류", "target params와 label header는 필수입니다.")
            return
        self.result = {
            "target_params": target_params,
            "label_header": label_header,
            "condition_params": self.condition_params_edit.text().strip(),
            "polarity": self.polarity_box.currentText().strip(),
        }
        self.accept()


class CustomLabelDialog(QtWidgets.QDialog):
    def __init__(self, subset_count: int, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Custom Labeling")
        self.resize(900, 520)
        self.subset_count = subset_count
        self.result: dict[str, list[str]] | None = None

        layout = QtWidgets.QVBoxLayout(self)

        top = QtWidgets.QHBoxLayout()
        top.addWidget(QtWidgets.QLabel(f"감지된 subset 수: {subset_count}"))
        top.addSpacing(16)
        top.addWidget(QtWidgets.QLabel("라벨 컬럼 수"))
        self.col_count_spin = QtWidgets.QSpinBox()
        self.col_count_spin.setMinimum(1)
        self.col_count_spin.setValue(1)
        top.addWidget(self.col_count_spin)
        apply_btn = QtWidgets.QPushButton("적용")
        apply_btn.clicked.connect(self._rebuild_table)
        top.addWidget(apply_btn)
        top.addStretch(1)
        hint = QtWidgets.QLabel("엑셀처럼 범위 선택 후 Ctrl+C / Ctrl+V 지원")
        hint.setStyleSheet("color:#666;")
        top.addWidget(hint)
        layout.addLayout(top)

        self.header_edits_container = QtWidgets.QWidget()
        self.header_edits_layout = QtWidgets.QHBoxLayout(self.header_edits_container)
        self.header_edits_layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.header_edits_container)

        self.table = QtWidgets.QTableWidget(self.subset_count, 1)
        self.table.setHorizontalHeaderLabels(["label_1"])
        self.table.setVerticalHeaderLabels([f"S{i + 1}" for i in range(self.subset_count)])
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.ContiguousSelection)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectItems)
        self.table.setEditTriggers(
            QtWidgets.QAbstractItemView.DoubleClicked
            | QtWidgets.QAbstractItemView.EditKeyPressed
            | QtWidgets.QAbstractItemView.AnyKeyPressed
        )
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setDefaultSectionSize(24)
        layout.addWidget(self.table)

        self._ensure_table_items()
        self._rebuild_header_editors(1)

        btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        btns.accepted.connect(self._save)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _ensure_table_items(self):
        for r in range(self.table.rowCount()):
            for c in range(self.table.columnCount()):
                if self.table.item(r, c) is None:
                    self.table.setItem(r, c, QtWidgets.QTableWidgetItem(""))

    def keyPressEvent(self, event: QtGui.QKeyEvent):
        if event.matches(QtGui.QKeySequence.Copy):
            self._copy_selection()
            return
        if event.matches(QtGui.QKeySequence.Paste):
            self._paste_selection()
            return
        super().keyPressEvent(event)

    def _copy_selection(self):
        ranges = self.table.selectedRanges()
        if not ranges:
            return
        rg = ranges[0]
        lines = []
        for r in range(rg.topRow(), rg.bottomRow() + 1):
            cols = []
            for c in range(rg.leftColumn(), rg.rightColumn() + 1):
                item = self.table.item(r, c)
                cols.append(item.text() if item else "")
            lines.append("\t".join(cols))
        QtWidgets.QApplication.clipboard().setText("\n".join(lines))

    def _paste_selection(self):
        text = QtWidgets.QApplication.clipboard().text()
        if not text:
            return
        start = self.table.currentIndex()
        start_row = start.row() if start.isValid() else 0
        start_col = start.column() if start.isValid() else 0

        rows = [line.split("\t") for line in text.splitlines()]
        for dr, row in enumerate(rows):
            rr = start_row + dr
            if rr >= self.table.rowCount():
                break
            for dc, value in enumerate(row):
                cc = start_col + dc
                if cc >= self.table.columnCount():
                    break
                item = self.table.item(rr, cc)
                if item is None:
                    item = QtWidgets.QTableWidgetItem("")
                    self.table.setItem(rr, cc, item)
                item.setText(value)

    def _rebuild_header_editors(self, n_cols: int):
        while self.header_edits_layout.count():
            w = self.header_edits_layout.takeAt(0).widget()
            if w:
                w.deleteLater()

        self.header_edits: list[QtWidgets.QLineEdit] = []
        for i in range(n_cols):
            box = QtWidgets.QVBoxLayout()
            container = QtWidgets.QWidget()
            container.setLayout(box)
            label = QtWidgets.QLabel(f"컬럼 {i + 1} 이름")
            edit = QtWidgets.QLineEdit(f"label_{i + 1}")
            edit.textChanged.connect(self._sync_table_headers)
            self.header_edits.append(edit)
            box.addWidget(label)
            box.addWidget(edit)
            self.header_edits_layout.addWidget(container)
        self.header_edits_layout.addStretch(1)
        self._sync_table_headers()

    def _sync_table_headers(self):
        headers = [e.text().strip() or f"label_{i + 1}" for i, e in enumerate(self.header_edits)]
        self.table.setHorizontalHeaderLabels(headers)

    def _rebuild_table(self):
        n_cols = self.col_count_spin.value()
        old_rows = self.table.rowCount()
        old_cols = self.table.columnCount()

        snapshot = [["" for _ in range(old_cols)] for _ in range(old_rows)]
        for r in range(old_rows):
            for c in range(old_cols):
                item = self.table.item(r, c)
                snapshot[r][c] = item.text() if item else ""

        self.table.setColumnCount(n_cols)
        self._ensure_table_items()

        for r in range(min(old_rows, self.table.rowCount())):
            for c in range(min(old_cols, n_cols)):
                self.table.item(r, c).setText(snapshot[r][c])

        self._rebuild_header_editors(n_cols)

    def _save(self):
        headers = [e.text().strip() for e in self.header_edits]
        if any(not h for h in headers):
            QtWidgets.QMessageBox.critical(self, "입력 오류", "컬럼 이름은 비워둘 수 없습니다.")
            return
        if len(set(headers)) != len(headers):
            QtWidgets.QMessageBox.critical(self, "입력 오류", "중복 컬럼 이름이 있습니다.")
            return

        result: dict[str, list[str]] = {h: [] for h in headers}
        for c, name in enumerate(headers):
            for r in range(self.table.rowCount()):
                item = self.table.item(r, c)
                result[name].append(item.text() if item else "")

        self.result = result
        self.accept()


@dataclass
class ProcessParams:
    file_paths: list[str]
    file_type: str
    output_dir: str
    voltage_col: str
    current_col: str
    thres_cur: float
    min_interval: float
    measure_type: str
    custom_labels: dict[str, list[str]] | None
    measure_config: dict | None


class ProcessWorker(QtCore.QObject):
    progress = QtCore.Signal(float, str)
    message = QtCore.Signal(str, str)
    finished = QtCore.Signal(list)

    def __init__(self, params: ProcessParams):
        super().__init__()
        self.params = params

    @QtCore.Slot()
    def run(self):
        saved = process_files(
            file_paths=self.params.file_paths,
            file_type=self.params.file_type,
            output_dir=self.params.output_dir,
            voltage_col=self.params.voltage_col,
            current_col=self.params.current_col,
            thres_cur=self.params.thres_cur,
            min_interval=self.params.min_interval,
            measure_type=self.params.measure_type,
            custom_labels=self.params.custom_labels,
            measure_config=self.params.measure_config,
            on_progress=lambda p, t: self.progress.emit(p, t),
            on_message=lambda lv, m: self.message.emit(lv, m),
        )
        self.finished.emit(saved)


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("File Preprocessor (PySide6)")
        self.resize(1040, 780)

        self.selected_files: list[str] = []
        self.custom_labels: dict[str, list[str]] | None = None
        self.measure_configs = {k: v.copy() for k, v in DEFAULT_MEASURE_CONFIG.items()}

        self._build_ui()

    def _build_ui(self):
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        root = QtWidgets.QVBoxLayout(central)

        sec1 = QtWidgets.QGroupBox("1. 파일 선택")
        sec1_l = QtWidgets.QVBoxLayout(sec1)
        row = QtWidgets.QHBoxLayout()
        row.addWidget(QtWidgets.QLabel("File type"))
        self.file_type_box = QtWidgets.QComboBox()
        self.file_type_box.addItems(FILE_TYPES)
        row.addWidget(self.file_type_box)
        row.addSpacing(12)
        row.addWidget(QtWidgets.QLabel("입력 폴더"))
        self.input_dir_edit = QtWidgets.QLineEdit(os.getcwd())
        row.addWidget(self.input_dir_edit, 1)
        input_browse_btn = QtWidgets.QPushButton("찾기")
        input_browse_btn.clicked.connect(self.browse_input)
        row.addWidget(input_browse_btn)
        scan_btn = QtWidgets.QPushButton("폴더 스캔")
        scan_btn.clicked.connect(self.scan_input_directory)
        row.addWidget(scan_btn)
        pick_btn = QtWidgets.QPushButton("파일 선택(다중)")
        pick_btn.clicked.connect(self.select_files)
        row.addWidget(pick_btn)
        row.addStretch(1)
        sec1_l.addLayout(row)

        self.file_list = QtWidgets.QListWidget()
        self.file_list.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        sec1_l.addWidget(self.file_list)
        root.addWidget(sec1)

        sec2 = QtWidgets.QGroupBox("2. 출력 폴더")
        sec2_l = QtWidgets.QHBoxLayout(sec2)
        self.output_dir_edit = QtWidgets.QLineEdit(DEFAULT_OUTPUT_DIR)
        sec2_l.addWidget(self.output_dir_edit)
        browse_btn = QtWidgets.QPushButton("찾기")
        browse_btn.clicked.connect(self.browse_output)
        sec2_l.addWidget(browse_btn)
        root.addWidget(sec2)

        sec3 = QtWidgets.QGroupBox("3. 처리 파라미터")
        form = QtWidgets.QGridLayout(sec3)
        self.voltage_col_edit = QtWidgets.QLineEdit(DEFAULT_VOLTAGE_COL)
        self.current_col_edit = QtWidgets.QLineEdit(DEFAULT_CURRENT_COL)
        self.thres_cur_edit = QtWidgets.QLineEdit(f"{DEFAULT_THRES_CUR:.2e}")
        self.min_interval_edit = QtWidgets.QLineEdit(f"{DEFAULT_MIN_INTERVAL:.2e}")

        form.addWidget(QtWidgets.QLabel("전압 컬럼명"), 0, 0)
        form.addWidget(self.voltage_col_edit, 0, 1)
        form.addWidget(QtWidgets.QLabel("전류 컬럼명"), 0, 2)
        form.addWidget(self.current_col_edit, 0, 3)
        form.addWidget(QtWidgets.QLabel("Vth 임계전류값"), 1, 0)
        form.addWidget(self.thres_cur_edit, 1, 1)
        form.addWidget(QtWidgets.QLabel("Curve 분리 최소 간격"), 1, 2)
        form.addWidget(self.min_interval_edit, 1, 3)
        root.addWidget(sec3)

        sec4 = QtWidgets.QGroupBox("4. Measure Type")
        sec4_l = QtWidgets.QHBoxLayout(sec4)
        self.measure_type_box = QtWidgets.QComboBox()
        self.measure_type_box.addItems(MEASURE_TYPES)
        self.measure_type_box.currentTextChanged.connect(self.on_measure_type_changed)
        sec4_l.addWidget(self.measure_type_box)
        self.custom_btn = QtWidgets.QPushButton("Custom Label 설정")
        self.custom_btn.clicked.connect(self.configure_custom_labels)
        self.custom_btn.setEnabled(False)
        sec4_l.addWidget(self.custom_btn)
        self.measure_btn = QtWidgets.QPushButton("Measure 설정")
        self.measure_btn.clicked.connect(self.configure_measure_settings)
        sec4_l.addWidget(self.measure_btn)
        sec4_l.addStretch(1)
        root.addWidget(sec4)

        run_row = QtWidgets.QHBoxLayout()
        self.process_btn = QtWidgets.QPushButton("Process")
        self.process_btn.clicked.connect(self.process)
        run_row.addWidget(self.process_btn)
        self.progress = QtWidgets.QProgressBar()
        self.progress.setRange(0, 100)
        run_row.addWidget(self.progress)
        root.addLayout(run_row)

        self.status_label = QtWidgets.QLabel("대기 중")
        root.addWidget(self.status_label)

        log_group = QtWidgets.QGroupBox("로그")
        log_l = QtWidgets.QVBoxLayout(log_group)
        self.log_text = QtWidgets.QPlainTextEdit()
        self.log_text.setReadOnly(True)
        log_l.addWidget(self.log_text)
        root.addWidget(log_group, 1)

    def append_log(self, msg: str):
        self.log_text.appendPlainText(msg)

    def browse_output(self):
        path = QtWidgets.QFileDialog.getExistingDirectory(self, "출력 폴더 선택", self.output_dir_edit.text() or "/")
        if path:
            self.output_dir_edit.setText(path)

    def browse_input(self):
        path = QtWidgets.QFileDialog.getExistingDirectory(self, "입력 폴더 선택", self.input_dir_edit.text() or "/")
        if path:
            self.input_dir_edit.setText(path)

    def scan_input_directory(self):
        src_dir = self.input_dir_edit.text().strip()
        if not src_dir or not os.path.isdir(src_dir):
            QtWidgets.QMessageBox.warning(self, "안내", "유효한 입력 폴더를 지정해 주세요.")
            return

        file_type = self.file_type_box.currentText().strip()
        files = scan_directory(src_dir, file_type)
        if not files:
            QtWidgets.QMessageBox.information(self, "안내", f"{file_type} 파일을 찾지 못했습니다.")
            return

        self.selected_files = files
        self.file_list.clear()
        for path in self.selected_files:
            item = QtWidgets.QListWidgetItem(os.path.basename(path))
            item.setData(QtCore.Qt.UserRole, path)
            self.file_list.addItem(item)
            item.setSelected(True)

        self.status_label.setText(f"{len(self.selected_files)}개 파일 선택됨")
        self.append_log(f"[INFO] 폴더 스캔 완료: {src_dir} ({len(self.selected_files)}개)")

    def select_files(self):
        file_type = self.file_type_box.currentText().strip()
        pattern = _EXT_MAP.get(file_type, "*.*")
        files, _ = QtWidgets.QFileDialog.getOpenFileNames(
            self,
            "처리할 파일 선택 (다중 선택 가능)",
            self.input_dir_edit.text().strip() or "",
            f"{file_type} files ({pattern});;All files (*.*)",
        )
        if not files:
            return

        self.selected_files = list(files)
        self.input_dir_edit.setText(os.path.dirname(self.selected_files[0]))
        self.file_list.clear()
        for path in self.selected_files:
            item = QtWidgets.QListWidgetItem(os.path.basename(path))
            item.setData(QtCore.Qt.UserRole, path)
            self.file_list.addItem(item)
            item.setSelected(True)

        self.status_label.setText(f"{len(self.selected_files)}개 파일 선택됨")
        self.append_log(f"[INFO] 파일 선택 완료: {len(self.selected_files)}개")

    def selected_files_paths(self) -> list[str]:
        return [item.data(QtCore.Qt.UserRole) for item in self.file_list.selectedItems()]

    def detect_subset_count(self, selected_files: list[str], file_type: str, min_interval: float) -> int:
        df = load_file(selected_files[0], file_type)
        subsets = detect_subsets(df, min_interval)
        return len(subsets)

    def on_measure_type_changed(self, value: str):
        is_custom = value == "Custom"
        self.custom_btn.setEnabled(is_custom)
        self.measure_btn.setEnabled(not is_custom)
        if not is_custom:
            self.custom_labels = None

    def configure_custom_labels(self):
        selected = self.selected_files_paths()
        if not selected:
            QtWidgets.QMessageBox.warning(self, "안내", "먼저 파일을 1개 이상 선택하세요.")
            return

        try:
            min_interval = float(self.min_interval_edit.text())
            subset_count = self.detect_subset_count(selected, self.file_type_box.currentText(), min_interval)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "오류", f"subset check 실패: {e}")
            return

        if subset_count <= 0:
            QtWidgets.QMessageBox.warning(self, "안내", "subset이 감지되지 않았습니다.")
            return

        dialog = CustomLabelDialog(subset_count, self)
        if dialog.exec() == QtWidgets.QDialog.Accepted and dialog.result is not None:
            self.custom_labels = dialog.result
            self.append_log(f"[INFO] Custom label 설정 완료: {list(self.custom_labels.keys())}")

    def configure_measure_settings(self):
        measure_type = self.measure_type_box.currentText()
        if measure_type == "Custom":
            QtWidgets.QMessageBox.information(self, "안내", "Custom은 Measure 설정 대신 Custom Label 설정을 사용합니다.")
            return
        dialog = MeasureConfigDialog(measure_type, self.measure_configs.get(measure_type, {}).copy(), self)
        if dialog.exec() == QtWidgets.QDialog.Accepted and dialog.result is not None:
            self.measure_configs[measure_type] = dialog.result
            self.append_log(f"[INFO] {measure_type} 설정 업데이트 완료")

    def process(self):
        selected = self.selected_files_paths()
        if not selected:
            QtWidgets.QMessageBox.critical(self, "오류", "처리할 파일을 선택해 주세요.")
            return

        measure_type = self.measure_type_box.currentText()
        if measure_type == "Custom" and not self.custom_labels:
            QtWidgets.QMessageBox.warning(self, "안내", "Custom Label 설정을 먼저 진행하세요.")
            return

        try:
            thres_cur = float(self.thres_cur_edit.text())
            min_interval = float(self.min_interval_edit.text())
        except ValueError:
            QtWidgets.QMessageBox.critical(self, "오류", "숫자 입력값(thres_cur, min_interval)을 확인해 주세요.")
            return

        params = ProcessParams(
            file_paths=selected,
            file_type=self.file_type_box.currentText(),
            output_dir=self.output_dir_edit.text().strip(),
            voltage_col=self.voltage_col_edit.text().strip(),
            current_col=self.current_col_edit.text().strip(),
            thres_cur=thres_cur,
            min_interval=min_interval,
            measure_type=measure_type,
            custom_labels=self.custom_labels.copy() if self.custom_labels else None,
            measure_config=self.measure_configs.get(measure_type, {}).copy(),
        )

        self.process_btn.setEnabled(False)
        self.progress.setValue(0)
        self.status_label.setText("처리 시작…")

        self.thread = QtCore.QThread(self)
        self.worker = ProcessWorker(params)
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self.update_progress)
        self.worker.message.connect(self.handle_message)
        self.worker.finished.connect(self.finish_process)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.start()

    @QtCore.Slot(float, str)
    def update_progress(self, progress: float, text: str):
        self.progress.setValue(max(0, min(100, int(progress * 100))))
        self.status_label.setText(text)

    @QtCore.Slot(str, str)
    def handle_message(self, level: str, msg: str):
        self.append_log(f"[{level.upper()}] {msg}")

    @QtCore.Slot(list)
    def finish_process(self, saved: list[str]):
        self.process_btn.setEnabled(True)
        if saved:
            self.status_label.setText(f"완료: {len(saved)}개 파일 저장")
            self.append_log("[INFO] 저장 완료 파일:")
            for p in saved:
                self.append_log(f"  - {p}")
            QtWidgets.QMessageBox.information(self, "완료", f"{len(saved)}개 파일 저장 완료")
        else:
            self.status_label.setText("완료: 저장된 파일 없음")
            QtWidgets.QMessageBox.warning(self, "완료", "저장된 파일이 없습니다. 로그를 확인하세요.")


def main():
    app = QtWidgets.QApplication([])
    win = MainWindow()
    win.show()
    app.exec()


if __name__ == "__main__":
    main()
