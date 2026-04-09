from __future__ import annotations

import datetime
import glob
import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants / Defaults
# ---------------------------------------------------------------------------
_today = datetime.date.today().strftime("%y%m%d")
DEFAULT_OUTPUT_DIR = rf"D:\Multimedia\upload\output_{_today}"

DEFAULT_VOLTAGE_COL = "VMeasCh2"   # UI label: 전압 컬럼명 (cur_col)
DEFAULT_CURRENT_COL = "ID"         # UI label: 전류 컬럼명 (vol_col)
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
# Data Loading
# ---------------------------------------------------------------------------

def load_xls(filepath: str) -> pd.DataFrame:
    """Load a legacy .xls file from a filesystem path using xlrd."""
    import xlrd

    wb = xlrd.open_workbook(filepath)
    sheet = wb.sheet_by_index(0)
    headers = sheet.row_values(0)
    rows = [sheet.row_values(r) for r in range(1, sheet.nrows)]
    return pd.DataFrame(rows, columns=headers)


def load_nasca(filepath: str) -> pd.DataFrame:
    """Load a DRM-protected file via xlwings (requires Windows + Excel installed)."""
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
    """Load a CSV file from a filesystem path."""
    return pd.read_csv(filepath)


def load_file(filepath: str, file_type: str) -> pd.DataFrame:
    """Dispatch to the appropriate loader based on file_type."""
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
    """Extract metadata table from Sheet3 where columns are Parameter Name and Value."""
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
    """Return sorted list of full file paths in src_dir matching file_type."""
    pattern = os.path.join(src_dir, _EXT_MAP.get(file_type, "*.*"))
    return sorted(glob.glob(pattern))


# ---------------------------------------------------------------------------
# Core Processing
# ---------------------------------------------------------------------------

def detect_subsets(df: pd.DataFrame, min_interval: float) -> list[pd.DataFrame]:
    """
    Split df into subsets wherever consecutive TimeOutput values differ by
    >= min_interval.

    Returns a list of DataFrames (one per subset), all with reset indices.
    """
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
    """Extract vth, vth_2, ss from a subset."""
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
        if v_step <= 0:
            raise ValueError("V_step은 양수여야 합니다.")
        labels = []
        cur = v_min
        guard = 0
        while cur <= v_max + (abs(v_step) * 1e-9):
            labels.append(cur)
            cur += v_step
            guard += 1
            if guard > 1_000_000:
                raise ValueError("ISPP label 계산이 비정상적으로 길어 중단했습니다.")
        if labels and labels[-1] > v_max:
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
        # 중복 제거(예: cycle=1)
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
    """
    Add labeling columns to each subset DataFrame.

    For ISPP / Endurance / Retention:
        TODO: implement measure-type-specific labeling logic.

    For Custom:
        custom_labels = {col_name: [value_for_subset_0, value_for_subset_1, …]}
        Each column is added to the corresponding subset.
    """
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
    """
    Keep only the voltage, current, and any extra columns (params + labels).
    Rename voltage_col → 'voltage', current_col → 'current'.
    """
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
    """Evenly downsample a subset to at most max_rows rows."""
    if len(subset_df) <= max_rows:
        return subset_df.reset_index(drop=True)
    indices = np.linspace(0, len(subset_df) - 1, max_rows, dtype=int)
    return subset_df.iloc[indices].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

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
    """
    Full pipeline: load → detect subsets → extract params → label →
    trim/rename columns → downsample → save CSV.

    Returns a list of saved file paths.
    """
    del thres_cur  # reserved for future parameter extraction
    os.makedirs(output_dir, exist_ok=True)
    saved_paths: list[str] = []

    total = len(file_paths)
    for idx, filepath in enumerate(file_paths):
        fname = os.path.basename(filepath)
        if on_progress:
            on_progress(idx / max(total, 1), f"Processing {fname}…")
        try:
            # 1. Load
            df = load_file(filepath, file_type)

            # 2. Detect subsets
            subsets = detect_subsets(df, min_interval)
            if not subsets:
                if on_message:
                    on_message("warning", f"{fname}: subset이 감지되지 않았습니다 — 건너뜁니다.")
                continue

            # 3. Extract parameters per subset
            param_dicts = []
            for subset in subsets:
                param_dicts.append(
                    extract_parameters(subset, voltage_col, current_col, thres_cur)
                )

            param_cols = []
            for i, params in enumerate(param_dicts):
                for k, v in params.items():
                    subsets[i][k] = v
                    if k not in param_cols:
                        param_cols.append(k)

            # 4. Label subsets
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
                label_header = measure_config.get("label_header", "").strip()
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

            # 5. Trim, rename, and downsample each subset
            extra_cols = param_cols + label_cols
            processed_subsets = []
            for subset in subsets:
                subset = keep_and_rename_columns(subset, voltage_col, current_col, extra_cols)
                subset = downsample(subset, MAX_ROWS)
                processed_subsets.append(subset)

            # 6. Concatenate and save
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
# Desktop UI
# ---------------------------------------------------------------------------


class CustomLabelDialog(tk.Toplevel):
    def __init__(self, master, subset_count: int):
        super().__init__(master)
        self.title("Custom Labeling")
        self.subset_count = subset_count
        self.result: dict[str, list[str]] | None = None
        self.column_count_var = tk.StringVar(value="1")
        self.entries: list[tuple[tk.Entry, list[tk.Entry]]] = []

        self._build()
        self.transient(master)
        self.grab_set()

    def _build(self):
        ttk.Label(self, text=f"감지된 subset 수: {self.subset_count}").grid(row=0, column=0, columnspan=4, sticky="w", padx=10, pady=(10, 6))
        ttk.Label(self, text="라벨 컬럼 수").grid(row=1, column=0, sticky="w", padx=10)
        ttk.Entry(self, textvariable=self.column_count_var, width=8).grid(row=1, column=1, sticky="w")
        ttk.Button(self, text="생성", command=self._render_inputs).grid(row=1, column=2, padx=6)

        self.form_frame = ttk.Frame(self)
        self.form_frame.grid(row=2, column=0, columnspan=4, sticky="nsew", padx=10, pady=10)

        btns = ttk.Frame(self)
        btns.grid(row=3, column=0, columnspan=4, sticky="e", padx=10, pady=(0, 10))
        ttk.Button(btns, text="취소", command=self.destroy).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="확인", command=self._save).pack(side="right")

        self._render_inputs()

    def _render_inputs(self):
        for widget in self.form_frame.winfo_children():
            widget.destroy()
        self.entries.clear()

        try:
            n_cols = max(1, int(self.column_count_var.get()))
        except ValueError:
            n_cols = 1
            self.column_count_var.set("1")

        for c in range(n_cols):
            ttk.Label(self.form_frame, text=f"컬럼 {c + 1} 이름").grid(row=c * 2, column=0, sticky="w", pady=(4, 2))
            name_entry = ttk.Entry(self.form_frame, width=20)
            name_entry.insert(0, f"label_{c + 1}")
            name_entry.grid(row=c * 2, column=1, sticky="w", pady=(4, 2))

            val_entries = []
            val_row = c * 2 + 1
            for j in range(self.subset_count):
                ttk.Label(self.form_frame, text=f"S{j + 1}").grid(row=val_row, column=j * 2, sticky="e", padx=(0, 2), pady=(0, 6))
                entry = ttk.Entry(self.form_frame, width=10)
                entry.grid(row=val_row, column=j * 2 + 1, sticky="w", padx=(0, 8), pady=(0, 6))
                val_entries.append(entry)

            self.entries.append((name_entry, val_entries))

    def _save(self):
        result = {}
        for name_entry, val_entries in self.entries:
            col_name = name_entry.get().strip()
            if not col_name:
                messagebox.showerror("입력 오류", "컬럼 이름은 비워둘 수 없습니다.", parent=self)
                return
            result[col_name] = [v.get() for v in val_entries]
        self.result = result
        self.destroy()


class MeasureConfigDialog(tk.Toplevel):
    def __init__(self, master, measure_type: str, current_config: dict):
        super().__init__(master)
        self.title(f"{measure_type} 설정")
        self.measure_type = measure_type
        self.result: dict | None = None

        self.target_params_var = tk.StringVar(value=current_config.get("target_params", ""))
        self.label_header_var = tk.StringVar(value=current_config.get("label_header", ""))
        self.condition_params_var = tk.StringVar(value=current_config.get("condition_params", ""))
        self.polarity_var = tk.StringVar(value=current_config.get("polarity", "PGM"))

        self._build()
        self.transient(master)
        self.grab_set()

    def _build(self):
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Target Parameter Name(s) (콤마 구분)").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.target_params_var, width=50).grid(row=1, column=0, sticky="we", pady=(2, 8))

        ttk.Label(frm, text="Subset Label Column Header").grid(row=2, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.label_header_var, width=50).grid(row=3, column=0, sticky="we", pady=(2, 8))

        ttk.Label(frm, text="Append Condition Parameter Name(s) (콤마 구분)").grid(row=4, column=0, sticky="w")
        ttk.Entry(frm, textvariable=self.condition_params_var, width=50).grid(row=5, column=0, sticky="we", pady=(2, 8))

        ttk.Label(frm, text="Polarity").grid(row=6, column=0, sticky="w")
        ttk.Combobox(frm, textvariable=self.polarity_var, values=POLARITIES, state="readonly", width=20).grid(row=7, column=0, sticky="w", pady=(2, 8))

        btns = ttk.Frame(frm)
        btns.grid(row=8, column=0, sticky="e")
        ttk.Button(btns, text="취소", command=self.destroy).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="확인", command=self._save).pack(side="right")

    def _save(self):
        label_header = self.label_header_var.get().strip()
        target_params = self.target_params_var.get().strip()
        if not label_header or not target_params:
            messagebox.showerror("입력 오류", "target params와 label header는 필수입니다.", parent=self)
            return
        self.result = {
            "target_params": target_params,
            "label_header": label_header,
            "condition_params": self.condition_params_var.get().strip(),
            "polarity": self.polarity_var.get().strip(),
        }
        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("File Preprocessor (Tk)")
        self.geometry("980x700")

        self.selected_files: list[str] = []
        self.custom_labels: dict[str, list[str]] | None = None
        self.measure_configs = {
            k: v.copy() for k, v in DEFAULT_MEASURE_CONFIG.items()
        }

        self.output_dir_var = tk.StringVar(value=DEFAULT_OUTPUT_DIR)
        self.file_type_var = tk.StringVar(value=FILE_TYPES[0])
        self.measure_type_var = tk.StringVar(value=MEASURE_TYPES[0])
        self.voltage_col_var = tk.StringVar(value=DEFAULT_VOLTAGE_COL)
        self.current_col_var = tk.StringVar(value=DEFAULT_CURRENT_COL)
        self.thres_cur_var = tk.StringVar(value=f"{DEFAULT_THRES_CUR:.2e}")
        self.min_interval_var = tk.StringVar(value=f"{DEFAULT_MIN_INTERVAL:.2e}")
        self.status_var = tk.StringVar(value="대기 중")

        self._build_ui()

    def _build_ui(self):
        root = ttk.Frame(self, padding=12)
        root.pack(fill="both", expand=True)

        # 1. 파일 선택
        sec1 = ttk.LabelFrame(root, text="1. 파일 선택", padding=10)
        sec1.pack(fill="x")

        ttk.Label(sec1, text="File type").grid(row=0, column=0, sticky="w")
        file_type_box = ttk.Combobox(sec1, textvariable=self.file_type_var, values=FILE_TYPES, state="readonly", width=10)
        file_type_box.grid(row=0, column=1, sticky="w")

        ttk.Button(sec1, text="파일 선택(다중)", command=self.select_files).grid(row=0, column=2, padx=(8, 0))

        self.file_listbox = tk.Listbox(sec1, selectmode="extended", height=8)
        self.file_listbox.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=(8, 0))

        # 2. 출력 폴더
        sec2 = ttk.LabelFrame(root, text="2. 출력 폴더", padding=10)
        sec2.pack(fill="x", pady=(10, 0))
        ttk.Entry(sec2, textvariable=self.output_dir_var, width=80).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(sec2, text="찾기", command=self._browse_output).grid(row=0, column=1)

        # 3. 처리 파라미터
        sec3 = ttk.LabelFrame(root, text="3. 처리 파라미터", padding=10)
        sec3.pack(fill="x", pady=(10, 0))

        ttk.Label(sec3, text="전압 컬럼명").grid(row=0, column=0, sticky="w")
        ttk.Entry(sec3, textvariable=self.voltage_col_var, width=24).grid(row=0, column=1, sticky="w", padx=(6, 14))

        ttk.Label(sec3, text="전류 컬럼명").grid(row=0, column=2, sticky="w")
        ttk.Entry(sec3, textvariable=self.current_col_var, width=24).grid(row=0, column=3, sticky="w", padx=(6, 0))

        ttk.Label(sec3, text="Vth 임계전류값").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(sec3, textvariable=self.thres_cur_var, width=24).grid(row=1, column=1, sticky="w", padx=(6, 14), pady=(8, 0))

        ttk.Label(sec3, text="Curve 분리 최소 간격").grid(row=1, column=2, sticky="w", pady=(8, 0))
        ttk.Entry(sec3, textvariable=self.min_interval_var, width=24).grid(row=1, column=3, sticky="w", padx=(6, 0), pady=(8, 0))

        # 4. Measure Type
        sec4 = ttk.LabelFrame(root, text="4. Measure Type", padding=10)
        sec4.pack(fill="x", pady=(10, 0))

        measure_box = ttk.Combobox(sec4, textvariable=self.measure_type_var, values=MEASURE_TYPES, state="readonly", width=16)
        measure_box.grid(row=0, column=0, sticky="w")
        measure_box.bind("<<ComboboxSelected>>", lambda _: self._on_measure_type_change())

        self.custom_btn = ttk.Button(sec4, text="Custom Label 설정", command=self.configure_custom_labels, state="disabled")
        self.custom_btn.grid(row=0, column=1, padx=(8, 0))
        self.measure_btn = ttk.Button(sec4, text="Measure 설정", command=self.configure_measure_settings, state="normal")
        self.measure_btn.grid(row=0, column=2, padx=(8, 0))

        # 실행
        sec5 = ttk.Frame(root)
        sec5.pack(fill="x", pady=(12, 0))
        self.process_btn = ttk.Button(sec5, text="Process", command=self.process)
        self.process_btn.pack(side="left")

        self.progress = ttk.Progressbar(sec5, mode="determinate", maximum=100)
        self.progress.pack(side="left", fill="x", expand=True, padx=(10, 0))

        ttk.Label(root, textvariable=self.status_var).pack(anchor="w", pady=(8, 0))

        log_frame = ttk.LabelFrame(root, text="로그", padding=8)
        log_frame.pack(fill="both", expand=True, pady=(10, 0))
        self.log_text = tk.Text(log_frame, height=10)
        self.log_text.pack(fill="both", expand=True)

    def _browse_output(self):
        path = filedialog.askdirectory(initialdir=self.output_dir_var.get() or "/")
        if path:
            self.output_dir_var.set(path)

    def _on_measure_type_change(self):
        is_custom = self.measure_type_var.get() == "Custom"
        self.custom_btn.configure(state="normal" if is_custom else "disabled")
        self.measure_btn.configure(state="disabled" if is_custom else "normal")
        if not is_custom:
            self.custom_labels = None

    def _append_log(self, msg: str):
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")

    def select_files(self):
        file_type = self.file_type_var.get().strip()
        pattern = _EXT_MAP.get(file_type, "*.*")
        selected = filedialog.askopenfilenames(
            title="처리할 파일 선택 (다중 선택 가능)",
            filetypes=[(f"{file_type} files", pattern), ("All files", "*.*")],
        )
        if not selected:
            return

        self.selected_files = list(selected)
        self.file_listbox.delete(0, "end")
        for path in self.selected_files:
            self.file_listbox.insert("end", path)

        for i in range(len(self.selected_files)):
            self.file_listbox.selection_set(i)

        self.status_var.set(f"{len(self.selected_files)}개 파일 선택됨")
        self._append_log(f"[INFO] 파일 선택 완료: {len(self.selected_files)}개")

    def _selected_files(self) -> list[str]:
        indices = self.file_listbox.curselection()
        return [self.file_listbox.get(i) for i in indices]

    def _detect_subset_count(self, selected_files: list[str], file_type: str, min_interval: float) -> int:
        df = load_file(selected_files[0], file_type)
        subsets = detect_subsets(df, min_interval)
        return len(subsets)

    def configure_custom_labels(self):
        selected = self._selected_files()
        if not selected:
            messagebox.showwarning("안내", "먼저 파일을 1개 이상 선택하세요.")
            return

        try:
            min_interval = float(self.min_interval_var.get())
            subset_count = self._detect_subset_count(selected, self.file_type_var.get(), min_interval)
        except Exception as e:
            messagebox.showerror("오류", f"subset check 실패: {e}")
            return

        if subset_count <= 0:
            messagebox.showwarning("안내", "subset이 감지되지 않았습니다.")
            return

        dialog = CustomLabelDialog(self, subset_count=subset_count)
        self.wait_window(dialog)
        if dialog.result is not None:
            self.custom_labels = dialog.result
            self._append_log(f"[INFO] Custom label 설정 완료: {list(self.custom_labels.keys())}")

    def configure_measure_settings(self):
        measure_type = self.measure_type_var.get()
        if measure_type == "Custom":
            messagebox.showinfo("안내", "Custom은 Measure 설정 대신 Custom Label 설정을 사용합니다.")
            return
        current = self.measure_configs.get(measure_type, {}).copy()
        dialog = MeasureConfigDialog(self, measure_type, current)
        self.wait_window(dialog)
        if dialog.result is not None:
            self.measure_configs[measure_type] = dialog.result
            self._append_log(f"[INFO] {measure_type} 설정 업데이트 완료")

    def process(self):
        selected_files = self._selected_files()
        if not selected_files:
            messagebox.showerror("오류", "처리할 파일을 선택해 주세요.")
            return

        if self.measure_type_var.get() == "Custom" and not self.custom_labels:
            messagebox.showwarning("안내", "Custom Label 설정을 먼저 진행하세요.")
            return

        try:
            thres_cur = float(self.thres_cur_var.get())
            min_interval = float(self.min_interval_var.get())
        except ValueError:
            messagebox.showerror("오류", "숫자 입력값(thres_cur, min_interval)을 확인해 주세요.")
            return

        self.process_btn.configure(state="disabled")
        self.progress["value"] = 0
        self.status_var.set("처리 시작…")

        threading.Thread(
            target=self._worker_process,
            args=(
                list(selected_files),
                self.file_type_var.get(),
                self.output_dir_var.get().strip(),
                self.voltage_col_var.get().strip(),
                self.current_col_var.get().strip(),
                float(thres_cur),
                float(min_interval),
                self.measure_type_var.get(),
                self.custom_labels.copy() if self.custom_labels else None,
                self.measure_configs.get(self.measure_type_var.get(), {}).copy(),
            ),
            daemon=True,
        ).start()

    def _worker_process(
        self,
        file_paths: list[str],
        file_type: str,
        output_dir: str,
        voltage_col: str,
        current_col: str,
        thres_cur_value: float,
        min_interval_value: float,
        measure_type: str,
        custom_labels: dict[str, list[str]] | None,
        measure_config: dict | None,
    ):
        def on_progress(progress: float, text: str):
            self.after(0, lambda p=progress, t=text: self._update_progress(p, t))

        def on_message(level: str, msg: str):
            self.after(0, lambda lv=level, m=msg: self._handle_message(lv, m))

        saved = process_files(
            file_paths=file_paths,
            file_type=file_type,
            output_dir=output_dir,
            voltage_col=voltage_col,
            current_col=current_col,
            thres_cur=thres_cur_value,
            min_interval=min_interval_value,
            measure_type=measure_type,
            custom_labels=custom_labels,
            measure_config=measure_config,
            on_progress=on_progress,
            on_message=on_message,
        )
        self.after(0, lambda: self._finish_process(saved))

    def _update_progress(self, progress: float, text: str):
        self.progress["value"] = max(0, min(100, progress * 100))
        self.status_var.set(text)

    def _handle_message(self, level: str, msg: str):
        prefix = level.upper()
        self._append_log(f"[{prefix}] {msg}")

    def _finish_process(self, saved: list[str]):
        self.process_btn.configure(state="normal")
        if saved:
            self.status_var.set(f"완료: {len(saved)}개 파일 저장")
            self._append_log("[INFO] 저장 완료 파일:")
            for path in saved:
                self._append_log(f"  - {path}")
            messagebox.showinfo("완료", f"{len(saved)}개 파일 저장 완료")
        else:
            self.status_var.set("완료: 저장된 파일 없음")
            messagebox.showwarning("완료", "저장된 파일이 없습니다. 로그를 확인하세요.")


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
