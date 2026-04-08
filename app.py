from __future__ import annotations

import datetime
import glob
import os
import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants / Defaults
# ---------------------------------------------------------------------------
_today = datetime.date.today().strftime("%y%m%d")
DEFAULT_SRC_DIR    = r"D:\\"
DEFAULT_OUTPUT_DIR = rf"D:\Multimedia\output_{_today}"

DEFAULT_VOLTAGE_COL  = "VMeasCh2"
DEFAULT_CURRENT_COL  = "ID"
DEFAULT_THRES_CUR    = 1e-7
DEFAULT_MIN_INTERVAL = 1e-5

TIME_COL = "TimeOutput"
MAX_ROWS = 100

MEASURE_TYPES = ["ISPP", "Endurance", "Retention", "Custom"]
FILE_TYPES    = ["xls", "nasca", "csv"]
_EXT_MAP      = {"xls": "*.xls", "nasca": "*.xls", "csv": "*.csv"}


# ---------------------------------------------------------------------------
# Data Loading (filesystem path-based)
# ---------------------------------------------------------------------------

def load_xls(filepath: str) -> pd.DataFrame:
    import xlrd
    wb = xlrd.open_workbook(filepath)
    sheet = wb.sheet_by_index(0)
    headers = sheet.row_values(0)
    rows = [sheet.row_values(r) for r in range(1, sheet.nrows)]
    return pd.DataFrame(rows, columns=headers)


def load_nasca(filepath: str) -> pd.DataFrame:
    """DRM-protected file via xlwings (Windows + Excel required)."""
    import xlwings as xw
    xw_app = xw.App(visible=False)
    try:
        wb = xw_app.books.open(filepath)
        sheet = wb.sheets[0]
        data = sheet.used_range.value
        wb.close()
    finally:
        xw_app.quit()
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data[1:], columns=data[0])


def load_csv(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath)


def load_file(filepath: str, file_type: str) -> pd.DataFrame:
    if file_type == "xls":
        return load_xls(filepath)
    elif file_type == "nasca":
        return load_nasca(filepath)
    elif file_type == "csv":
        return load_csv(filepath)
    raise ValueError(f"Unknown file type: {file_type}")


def scan_directory(src_dir: str, file_type: str) -> list:
    pattern = os.path.join(src_dir, _EXT_MAP.get(file_type, "*.*"))
    return sorted(glob.glob(pattern))


# ---------------------------------------------------------------------------
# Core Processing
# ---------------------------------------------------------------------------

def detect_subsets(df: pd.DataFrame, min_interval: float) -> list:
    if TIME_COL not in df.columns:
        raise KeyError(
            f"'{TIME_COL}' 컬럼이 없습니다. "
            f"사용 가능한 컬럼: {list(df.columns)}"
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
    # TODO: implement parameter extraction logic (e.g. Vth, Ion, Ioff, …)
    return {}


def label_subsets(
    subsets: list,
    measure_type: str,
    custom_labels: dict | None = None,
) -> list:
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
    extra_cols: list,
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


def run_pipeline(
    file_paths: list,
    file_type: str,
    output_dir: str,
    voltage_col: str,
    current_col: str,
    thres_cur: float,
    min_interval: float,
    measure_type: str,
    custom_labels: dict | None,
    log_cb,
    progress_cb,
) -> list:
    """
    Full pipeline. log_cb / progress_cb are called from a background thread.
    Returns list of saved CSV paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    saved_paths = []
    total = len(file_paths)

    for idx, filepath in enumerate(file_paths):
        fname = os.path.basename(filepath)
        progress_cb(idx / total)
        log_cb(f"[{idx + 1}/{total}] 처리 중: {fname}")
        try:
            df = load_file(filepath, file_type)

            subsets = detect_subsets(df, min_interval)
            if not subsets:
                log_cb(f"  ⚠ subset 없음 — 건너뜀")
                continue

            param_dicts = [extract_parameters(s) for s in subsets]
            param_cols: list = []
            for i, (s, params) in enumerate(zip(subsets, param_dicts)):
                for k, v in params.items():
                    subsets[i][k] = v
                    if k not in param_cols:
                        param_cols.append(k)

            subsets = label_subsets(subsets, measure_type, custom_labels)
            label_cols = (
                list(custom_labels.keys())
                if measure_type == "Custom" and custom_labels
                else []
            )

            processed = []
            for s in subsets:
                s = keep_and_rename_columns(
                    s, voltage_col, current_col, param_cols + label_cols
                )
                s = downsample(s, MAX_ROWS)
                processed.append(s)

            result_df = pd.concat(processed, ignore_index=True)
            stem = os.path.splitext(fname)[0]
            out_path = os.path.join(output_dir, f"{stem}_processed.csv")
            result_df.to_csv(out_path, index=False)
            saved_paths.append(out_path)
            log_cb(f"  ✔ 저장 완료: {out_path}")

        except Exception as e:
            log_cb(f"  ✘ 오류: {e}")

    progress_cb(1.0)
    return saved_paths


# ---------------------------------------------------------------------------
# Tkinter Application
# ---------------------------------------------------------------------------

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("File Preprocessor")
        self.geometry("900x960")
        self.minsize(700, 600)
        self.resizable(True, True)

        # Apply platform theme
        style = ttk.Style(self)
        for theme in ("vista", "xpnative", "winnative", "clam"):
            if theme in style.theme_names():
                style.theme_use(theme)
                break

        # Internal state
        self._q: queue.Queue = queue.Queue()
        self._scanned_files: list = []        # full paths from last scan
        self._label_rows: list = []            # [(header_var, [value_var, …])]
        self.subset_count: int = 0

        # tk variables
        self.src_dir_var      = tk.StringVar(value=DEFAULT_SRC_DIR)
        self.output_dir_var   = tk.StringVar(value=DEFAULT_OUTPUT_DIR)
        self.file_type_var    = tk.StringVar(value="xls")
        self.voltage_col_var  = tk.StringVar(value=DEFAULT_VOLTAGE_COL)
        self.current_col_var  = tk.StringVar(value=DEFAULT_CURRENT_COL)
        self.thres_cur_var    = tk.StringVar(value=str(DEFAULT_THRES_CUR))
        self.min_interval_var = tk.StringVar(value=str(DEFAULT_MIN_INTERVAL))
        self.measure_type_var = tk.StringVar(value=MEASURE_TYPES[0])

        self._build_ui()

    # ── Scrollable container ─────────────────────────────────────────────────

    def _build_ui(self):
        outer = ttk.Frame(self)
        outer.pack(fill="both", expand=True)

        self._canvas = tk.Canvas(outer, borderwidth=0, highlightthickness=0)
        vscroll = ttk.Scrollbar(outer, orient="vertical", command=self._canvas.yview)
        self._canvas.configure(yscrollcommand=vscroll.set)
        vscroll.pack(side="right", fill="y")
        self._canvas.pack(side="left", fill="both", expand=True)

        self._inner = ttk.Frame(self._canvas, padding=4)
        self._win_id = self._canvas.create_window(
            (0, 0), window=self._inner, anchor="nw"
        )
        self._inner.bind("<Configure>",
            lambda e: self._canvas.configure(scrollregion=self._canvas.bbox("all")))
        self._canvas.bind("<Configure>",
            lambda e: self._canvas.itemconfig(self._win_id, width=e.width))
        self._canvas.bind_all("<MouseWheel>",
            lambda e: self._canvas.yview_scroll(int(-1 * (e.delta / 120)), "units"))

        pad = dict(padx=10, pady=5)

        self._build_section_files(pad)
        self._build_section_output(pad)
        self._build_section_params(pad)
        self._build_section_measure(pad)
        self._build_section_custom()   # hidden until Custom is selected
        self._build_section_process(pad)

    # ── Section 1: 파일 선택 ─────────────────────────────────────────────────

    def _build_section_files(self, pad):
        frm = ttk.LabelFrame(self._inner, text="1. 파일 선택", padding=8)
        frm.pack(fill="x", **pad)

        # Source directory row
        r = ttk.Frame(frm)
        r.pack(fill="x", pady=2)
        ttk.Label(r, text="소스 폴더:", width=12).pack(side="left")
        ttk.Entry(r, textvariable=self.src_dir_var, width=55).pack(side="left", padx=4)
        ttk.Button(r, text="찾아보기", command=self._browse_src).pack(side="left")

        # File type radio buttons
        r2 = ttk.Frame(frm)
        r2.pack(fill="x", pady=4)
        ttk.Label(r2, text="File type:", width=12).pack(side="left")
        for ft in FILE_TYPES:
            ttk.Radiobutton(
                r2, text=ft, variable=self.file_type_var, value=ft
            ).pack(side="left", padx=8)

        ttk.Button(frm, text="  Scan  ", command=self._scan).pack(anchor="w", pady=2)

        # File listbox
        ttk.Label(frm, text="파일 목록 (Ctrl+클릭으로 다중 선택):").pack(anchor="w", pady=(6, 0))
        lf = ttk.Frame(frm)
        lf.pack(fill="x", pady=2)

        self._file_listbox = tk.Listbox(
            lf, selectmode=tk.MULTIPLE, height=7, font=("Consolas", 9),
            activestyle="dotbox"
        )
        sb_y = ttk.Scrollbar(lf, orient="vertical",   command=self._file_listbox.yview)
        sb_x = ttk.Scrollbar(lf, orient="horizontal", command=self._file_listbox.xview)
        self._file_listbox.configure(
            yscrollcommand=sb_y.set, xscrollcommand=sb_x.set
        )
        sb_y.pack(side="right", fill="y")
        sb_x.pack(side="bottom", fill="x")
        self._file_listbox.pack(side="left", fill="both", expand=True)

        r3 = ttk.Frame(frm)
        r3.pack(anchor="w", pady=2)
        ttk.Button(r3, text="전체 선택",  command=self._select_all).pack(side="left", padx=2)
        ttk.Button(r3, text="선택 해제",  command=self._deselect_all).pack(side="left", padx=2)

    # ── Section 2: 출력 폴더 ─────────────────────────────────────────────────

    def _build_section_output(self, pad):
        frm = ttk.LabelFrame(self._inner, text="2. 출력 폴더", padding=8)
        frm.pack(fill="x", **pad)

        r = ttk.Frame(frm)
        r.pack(fill="x", pady=2)
        ttk.Label(r, text="출력 폴더:", width=12).pack(side="left")
        ttk.Entry(r, textvariable=self.output_dir_var, width=55).pack(side="left", padx=4)
        ttk.Button(r, text="찾아보기", command=self._browse_output).pack(side="left")

    # ── Section 3: 처리 파라미터 ─────────────────────────────────────────────

    def _build_section_params(self, pad):
        frm = ttk.LabelFrame(self._inner, text="3. 처리 파라미터", padding=8)
        frm.pack(fill="x", **pad)

        g = ttk.Frame(frm)
        g.pack(fill="x")
        lw = 24

        ttk.Label(g, text="전압 컬럼명 (cur_col):", width=lw).grid(
            row=0, column=0, sticky="w", pady=3)
        ttk.Entry(g, textvariable=self.voltage_col_var, width=18).grid(
            row=0, column=1, padx=6)

        ttk.Label(g, text="전류 컬럼명 (vol_col):", width=lw).grid(
            row=0, column=2, sticky="w", padx=16)
        ttk.Entry(g, textvariable=self.current_col_var, width=18).grid(
            row=0, column=3, padx=6)

        ttk.Label(g, text="Vth 임계전류값 (thres_cur):", width=lw).grid(
            row=1, column=0, sticky="w", pady=3)
        ttk.Entry(g, textvariable=self.thres_cur_var, width=18).grid(
            row=1, column=1, padx=6)

        ttk.Label(g, text="Curve 분리 최소 간격:", width=lw).grid(
            row=1, column=2, sticky="w", padx=16)
        ttk.Entry(g, textvariable=self.min_interval_var, width=18).grid(
            row=1, column=3, padx=6)

    # ── Section 4: Measure Type ───────────────────────────────────────────────

    def _build_section_measure(self, pad):
        frm = ttk.LabelFrame(self._inner, text="4. Measure Type", padding=8)
        frm.pack(fill="x", **pad)

        r = ttk.Frame(frm)
        r.pack(fill="x")
        ttk.Label(r, text="Measure Type:").pack(side="left")
        cb = ttk.Combobox(
            r, textvariable=self.measure_type_var,
            values=MEASURE_TYPES, state="readonly", width=18
        )
        cb.pack(side="left", padx=8)
        cb.bind("<<ComboboxSelected>>", self._on_measure_type_change)

    # ── Section 5: Custom Labeling (조건부 표시) ──────────────────────────────

    def _build_section_custom(self):
        self._custom_frm = ttk.LabelFrame(
            self._inner, text="5. Custom Labeling", padding=8
        )
        # packed only when Custom is selected

        r = ttk.Frame(self._custom_frm)
        r.pack(fill="x", pady=2)
        ttk.Button(r, text="subset check", command=self._subset_check).pack(side="left")
        self._subset_count_lbl = ttk.Label(r, text="", foreground="blue")
        self._subset_count_lbl.pack(side="left", padx=10)

        r2 = ttk.Frame(self._custom_frm)
        r2.pack(fill="x", pady=2)
        ttk.Button(r2, text="+ Add label column", command=self._add_label_col).pack(
            side="left", padx=2)
        ttk.Button(r2, text="- Remove last", command=self._remove_label_col).pack(
            side="left", padx=2)

        self._label_container = ttk.Frame(self._custom_frm)
        self._label_container.pack(fill="x", pady=4)

    # ── Section 6: Process + Progress + Log ──────────────────────────────────

    def _build_section_process(self, pad):
        frm = ttk.LabelFrame(self._inner, text="6. 처리", padding=8)
        frm.pack(fill="x", **pad)

        self._process_btn = ttk.Button(
            frm, text="      Process      ", command=self._run_process
        )
        self._process_btn.pack(pady=6)

        self._progress = ttk.Progressbar(frm, mode="determinate", length=800)
        self._progress.pack(fill="x", pady=4)

        ttk.Label(frm, text="로그:").pack(anchor="w")
        self._log_area = scrolledtext.ScrolledText(
            frm, height=10, font=("Consolas", 9), state="disabled", wrap="none"
        )
        self._log_area.pack(fill="x", pady=4)

    # ── Event handlers ────────────────────────────────────────────────────────

    def _browse_src(self):
        d = filedialog.askdirectory(initialdir=self.src_dir_var.get() or "/")
        if d:
            self.src_dir_var.set(d)

    def _browse_output(self):
        d = filedialog.askdirectory(initialdir=self.output_dir_var.get() or "/")
        if d:
            self.output_dir_var.set(d)

    def _scan(self):
        src = self.src_dir_var.get()
        if not os.path.isdir(src):
            messagebox.showerror("오류", f"폴더를 찾을 수 없습니다:\n{src}")
            return
        self._scanned_files = scan_directory(src, self.file_type_var.get())
        self._file_listbox.delete(0, tk.END)
        for f in self._scanned_files:
            self._file_listbox.insert(tk.END, os.path.basename(f))
        if self._scanned_files:
            self._file_listbox.select_set(0, tk.END)
            self._log(f"Scan 완료: {len(self._scanned_files)}개 파일 발견.")
        else:
            self._log("해당 확장자 파일을 찾지 못했습니다.")

    def _select_all(self):
        self._file_listbox.select_set(0, tk.END)

    def _deselect_all(self):
        self._file_listbox.selection_clear(0, tk.END)

    def _get_selected_files(self) -> list:
        return [self._scanned_files[i] for i in self._file_listbox.curselection()]

    def _on_measure_type_change(self, _event=None):
        if self.measure_type_var.get() == "Custom":
            self._custom_frm.pack(fill="x", padx=10, pady=5, before=self._inner.winfo_children()[-1])
        else:
            self._custom_frm.pack_forget()

    # ── Custom labeling ───────────────────────────────────────────────────────

    def _subset_check(self):
        files = self._get_selected_files()
        if not files:
            messagebox.showwarning("알림", "파일을 먼저 스캔하고 선택해 주세요.")
            return
        try:
            min_iv = float(self.min_interval_var.get())
            df = load_file(files[0], self.file_type_var.get())
            subsets = detect_subsets(df, min_iv)
            self.subset_count = len(subsets)
            self._subset_count_lbl.config(text=f"{self.subset_count}개 subset 감지됨")
            self._refresh_label_rows()
        except Exception as e:
            messagebox.showerror("오류", f"Subset check 실패:\n{e}")

    def _add_label_col(self):
        if self.subset_count == 0:
            messagebox.showinfo("알림", "먼저 'subset check'를 실행하세요.")
            return
        hv = tk.StringVar(value=f"label_{len(self._label_rows) + 1}")
        vvs = [tk.StringVar() for _ in range(self.subset_count)]
        self._label_rows.append((hv, vvs))
        self._render_label_row(len(self._label_rows) - 1, hv, vvs)

    def _remove_label_col(self):
        if not self._label_rows:
            return
        self._label_rows.pop()
        for w in self._label_container.winfo_children():
            w.destroy()
        for i, (hv, vvs) in enumerate(self._label_rows):
            self._render_label_row(i, hv, vvs)

    def _render_label_row(self, idx: int, header_var: tk.StringVar, value_vars: list):
        rf = ttk.LabelFrame(self._label_container, text=f"Label column {idx + 1}", padding=4)
        rf.pack(fill="x", pady=3)

        hr = ttk.Frame(rf)
        hr.pack(fill="x", pady=2)
        ttk.Label(hr, text="Column name:").pack(side="left")
        ttk.Entry(hr, textvariable=header_var, width=22).pack(side="left", padx=4)

        cols_per_row = 4
        for i in range(0, len(value_vars), cols_per_row):
            vr = ttk.Frame(rf)
            vr.pack(fill="x", pady=1)
            for j in range(i, min(i + cols_per_row, len(value_vars))):
                ttk.Label(vr, text=f"Subset {j + 1}:").pack(side="left", padx=(8, 0))
                ttk.Entry(vr, textvariable=value_vars[j], width=12).pack(
                    side="left", padx=(2, 6))

    def _refresh_label_rows(self):
        for w in self._label_container.winfo_children():
            w.destroy()
        refreshed = []
        for hv, old_vvs in self._label_rows:
            new_vvs = [tk.StringVar() for _ in range(self.subset_count)]
            for i, v in enumerate(old_vvs):
                if i < len(new_vvs):
                    new_vvs[i].set(v.get())
            refreshed.append((hv, new_vvs))
        self._label_rows = refreshed
        for i, (hv, vvs) in enumerate(self._label_rows):
            self._render_label_row(i, hv, vvs)

    def _get_custom_labels(self) -> dict | None:
        if not self._label_rows:
            return None
        return {hv.get(): [v.get() for v in vvs] for hv, vvs in self._label_rows}

    # ── Processing ────────────────────────────────────────────────────────────

    def _log(self, msg: str):
        self._log_area.configure(state="normal")
        self._log_area.insert(tk.END, msg + "\n")
        self._log_area.see(tk.END)
        self._log_area.configure(state="disabled")

    def _run_process(self):
        files = self._get_selected_files()
        if not files:
            messagebox.showerror("오류", "처리할 파일을 선택해 주세요.")
            return
        try:
            thres_cur    = float(self.thres_cur_var.get())
            min_interval = float(self.min_interval_var.get())
        except ValueError:
            messagebox.showerror("오류", "임계값과 최소 간격은 숫자로 입력하세요.")
            return

        custom_labels = (
            self._get_custom_labels()
            if self.measure_type_var.get() == "Custom"
            else None
        )

        self._process_btn.configure(state="disabled")
        self._progress["value"] = 0
        self._log("─" * 60)
        self._log(f"처리 시작: {len(files)}개 파일")

        t = threading.Thread(
            target=self._process_thread,
            args=(
                files,
                self.file_type_var.get(),
                self.output_dir_var.get(),
                self.voltage_col_var.get(),
                self.current_col_var.get(),
                thres_cur,
                min_interval,
                self.measure_type_var.get(),
                custom_labels,
            ),
            daemon=True,
        )
        t.start()
        self.after(100, self._poll_queue)

    def _process_thread(self, *args):
        try:
            saved = run_pipeline(
                *args,
                log_cb=lambda msg: self._q.put(("log", msg)),
                progress_cb=lambda frac: self._q.put(("progress", frac)),
            )
            self._q.put(("done", saved))
        except Exception as e:
            self._q.put(("error", str(e)))

    def _poll_queue(self):
        try:
            while True:
                kind, data = self._q.get_nowait()
                if kind == "log":
                    self._log(data)
                elif kind == "progress":
                    self._progress["value"] = data * 100
                elif kind == "done":
                    self._log(f"\n완료: {len(data)}개 파일 저장됨.")
                    for p in data:
                        self._log(f"  → {p}")
                    self._process_btn.configure(state="normal")
                    return
                elif kind == "error":
                    self._log(f"오류: {data}")
                    self._process_btn.configure(state="normal")
                    return
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = App()
    app.mainloop()
