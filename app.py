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

_EXT_MAP = {"xls": "*.xls", "nasca": "*.xls", "csv": "*.csv"}


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


def extract_parameters(subset_df: pd.DataFrame) -> dict:
    """
    Extract parameters from a single subset.

    TODO: implement parameter extraction logic (e.g. Vth, Ion, Ioff, …).
    """
    return {}


def label_subsets(
    subsets: list[pd.DataFrame],
    measure_type: str,
    custom_labels: dict[str, list[str]] | None = None,
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

    # TODO: implement ISPP / Endurance / Retention labeling
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
            param_dicts = [extract_parameters(s) for s in subsets]

            param_cols = []
            for i, params in enumerate(param_dicts):
                for k, v in params.items():
                    subsets[i][k] = v
                    if k not in param_cols:
                        param_cols.append(k)

            # 4. Label subsets
            subsets = label_subsets(subsets, measure_type, custom_labels)

            label_cols = (
                list(custom_labels.keys())
                if measure_type == "Custom" and custom_labels
                else []
            )

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


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("File Preprocessor (Tk)")
        self.geometry("980x700")

        self.selected_files: list[str] = []
        self.custom_labels: dict[str, list[str]] | None = None

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

        def on_progress(progress: float, text: str):
            self.after(0, lambda: self._update_progress(progress, text))

        def on_message(level: str, msg: str):
            self.after(0, lambda: self._handle_message(level, msg))

        def worker():
            saved = process_files(
                file_paths=selected_files,
                file_type=self.file_type_var.get(),
                output_dir=self.output_dir_var.get().strip(),
                voltage_col=self.voltage_col_var.get().strip(),
                current_col=self.current_col_var.get().strip(),
                thres_cur=thres_cur,
                min_interval=min_interval,
                measure_type=self.measure_type_var.get(),
                custom_labels=self.custom_labels,
                on_progress=on_progress,
                on_message=on_message,
            )
            self.after(0, lambda: self._finish_process(saved))

        threading.Thread(target=worker, daemon=True).start()

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
