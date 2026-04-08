from __future__ import annotations

import datetime
import glob
import os

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants / Defaults
# ---------------------------------------------------------------------------
_today = datetime.date.today().strftime("%y%m%d")
DEFAULT_SRC_DIR = r"D:\\"
DEFAULT_OUTPUT_DIR = rf"D:\Multimedia\output_{_today}"

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
    elif file_type == "nasca":
        return load_nasca(filepath)
    elif file_type == "csv":
        return load_csv(filepath)
    else:
        raise ValueError(f"Unknown file type: {file_type}")


def scan_directory(src_dir: str, file_type: str) -> list:
    """Return sorted list of full file paths in src_dir matching file_type."""
    pattern = os.path.join(src_dir, _EXT_MAP.get(file_type, "*.*"))
    return sorted(glob.glob(pattern))


# ---------------------------------------------------------------------------
# Core Processing
# ---------------------------------------------------------------------------

def detect_subsets(df: pd.DataFrame, min_interval: float) -> list:
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
    subsets: list,
    measure_type: str,
    custom_labels: dict | None = None,
) -> list:
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
    extra_cols: list,
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
    file_paths: list,
    file_type: str,
    output_dir: str,
    voltage_col: str,
    current_col: str,
    thres_cur: float,
    min_interval: float,
    measure_type: str,
    custom_labels: dict | None,
) -> list:
    """
    Full pipeline: load → detect subsets → extract params → label →
    trim/rename columns → downsample → save CSV.

    Returns a list of saved file paths.
    """
    os.makedirs(output_dir, exist_ok=True)
    saved_paths = []

    progress = st.progress(0, text="Starting…")
    total = len(file_paths)

    for idx, filepath in enumerate(file_paths):
        fname = os.path.basename(filepath)
        progress.progress(idx / total, text=f"Processing {fname}…")
        try:
            # 1. Load
            df = load_file(filepath, file_type)

            # 2. Detect subsets
            subsets = detect_subsets(df, min_interval)
            if not subsets:
                st.warning(f"{fname}: subset이 감지되지 않았습니다 — 건너뜁니다.")
                continue

            # 3. Extract parameters per subset
            param_dicts = [extract_parameters(s) for s in subsets]

            param_cols = []
            for i, (s, params) in enumerate(zip(subsets, param_dicts)):
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
            for s in subsets:
                s = keep_and_rename_columns(s, voltage_col, current_col, extra_cols)
                s = downsample(s, MAX_ROWS)
                processed_subsets.append(s)

            # 6. Concatenate and save
            result_df = pd.concat(processed_subsets, ignore_index=True)
            stem = os.path.splitext(fname)[0]
            out_path = os.path.join(output_dir, f"{stem}_processed.csv")
            result_df.to_csv(out_path, index=False)
            saved_paths.append(out_path)

        except Exception as e:
            st.error(f"{fname} 처리 중 오류: {e}")

    progress.progress(1.0, text="완료.")
    return saved_paths


# ---------------------------------------------------------------------------
# UI Helpers
# ---------------------------------------------------------------------------

def _init_session_state():
    defaults = {
        "subset_count": 0,
        "label_headers": [],
        "scanned_files": [],
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


def render_custom_label_ui(
    file_paths: list,
    file_type: str,
    min_interval: float,
) -> dict | None:
    """
    Custom Measure Type 전용 UI:
      - "subset check" 버튼
      - 라벨 컬럼 추가 / 제거 버튼
      - 컬럼별 헤더 입력 + subset 수만큼 값 입력

    Returns {col_name: [val_per_subset]} or None.
    """
    st.subheader("Custom Labeling")

    # ── Subset check ─────────────────────────────────────────────────────────
    if st.button("subset check"):
        if not file_paths:
            st.warning("먼저 파일을 스캔하고 선택해 주세요.")
        else:
            try:
                df = load_file(file_paths[0], file_type)
                subsets = detect_subsets(df, min_interval)
                st.session_state.subset_count = len(subsets)
            except Exception as e:
                st.error(f"Subset check 실패: {e}")
            st.rerun()

    n = st.session_state.subset_count
    if n > 0:
        st.info(f"{n}개 subset 감지됨.")

        # ── Add / Remove label columns ────────────────────────────────────────
        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            if st.button("+ Add label column"):
                st.session_state.label_headers.append(
                    f"label_{len(st.session_state.label_headers) + 1}"
                )
                st.rerun()
        with btn_col2:
            if st.button("- Remove last") and st.session_state.label_headers:
                st.session_state.label_headers.pop()
                st.rerun()

        # ── Per-column inputs ─────────────────────────────────────────────────
        for i in range(len(st.session_state.label_headers)):
            st.markdown(f"**Label column {i + 1}**")
            st.text_input(
                "Column name",
                value=st.session_state.label_headers[i],
                key=f"header_name_{i}",
            )

            cols_per_row = 4
            for row_start in range(0, n, cols_per_row):
                row_cols = st.columns(min(cols_per_row, n - row_start))
                for j_offset, col in enumerate(row_cols):
                    j = row_start + j_offset
                    col.text_input(f"Subset {j + 1}", key=f"label_val_{i}_{j}")

            st.divider()

        # ── Collect values ────────────────────────────────────────────────────
        result = {}
        for i in range(len(st.session_state.label_headers)):
            col_name = st.session_state.get(
                f"header_name_{i}", st.session_state.label_headers[i]
            )
            values = [
                st.session_state.get(f"label_val_{i}_{j}", "") for j in range(n)
            ]
            result[col_name] = values
        return result if result else None

    return None


# ---------------------------------------------------------------------------
# Main Layout
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(page_title="File Preprocessor", layout="wide")
    _init_session_state()

    st.title("File Preprocessor")

    # ── 1. 파일 선택 (경로 기반) ──────────────────────────────────────────────
    st.subheader("1. 파일 선택")

    scan_col1, scan_col2 = st.columns([3, 1])
    with scan_col1:
        src_dir = st.text_input("소스 폴더 경로", value=DEFAULT_SRC_DIR)
    with scan_col2:
        # File type을 여기서 먼저 결정해야 Scan에 쓸 수 있음 — 임시로 session state 활용
        pass  # file_type은 아래 Section 3에서 결정

    # File type (Section 3보다 먼저 선언해야 Scan 버튼에서 사용 가능)
    # → 레이아웃 편의상 Section 3보다 위로 올림
    file_type = st.radio("File type", FILE_TYPES, horizontal=True)
    if file_type == "nasca":
        st.info("nasca 모드: xlwings(Windows + Excel 필요)를 사용합니다.")

    if st.button("Scan"):
        if not os.path.isdir(src_dir):
            st.error(f"폴더를 찾을 수 없습니다: {src_dir}")
        else:
            found = scan_directory(src_dir, file_type)
            st.session_state.scanned_files = found
            if not found:
                st.warning(f"{src_dir} 에서 {file_type} 파일을 찾지 못했습니다.")
            st.rerun()

    scanned = st.session_state.scanned_files
    if scanned:
        st.info(f"{len(scanned)}개 파일 발견.")
        selected_files = st.multiselect(
            "처리할 파일 선택",
            options=scanned,
            default=scanned,
            format_func=os.path.basename,
        )
    else:
        st.caption("폴더 경로를 입력하고 Scan 버튼을 눌러 파일 목록을 불러오세요.")
        selected_files = []

    # ── 2. 출력 폴더 ──────────────────────────────────────────────────────────
    st.subheader("2. 출력 폴더")
    output_dir = st.text_input("Output folder", value=DEFAULT_OUTPUT_DIR)

    # ── 3. 처리 파라미터 ──────────────────────────────────────────────────────
    st.subheader("3. 처리 파라미터")
    param_col2, param_col3 = st.columns(2)

    with param_col2:
        voltage_col = st.text_input("전압 컬럼명 (cur_col)", value=DEFAULT_VOLTAGE_COL)
        current_col = st.text_input("전류 컬럼명 (vol_col)", value=DEFAULT_CURRENT_COL)

    with param_col3:
        thres_cur = st.number_input(
            "Vth 임계전류값 (thres_cur)",
            value=DEFAULT_THRES_CUR,
            format="%.2e",
            step=1e-8,
        )
        min_interval = st.number_input(
            "Curve 분리 최소 간격 (min_interval)",
            value=DEFAULT_MIN_INTERVAL,
            format="%.2e",
            step=1e-6,
        )

    # ── 4. Measure Type ───────────────────────────────────────────────────────
    st.subheader("4. Measure Type")
    measure_type = st.selectbox("Measure Type", MEASURE_TYPES)

    # ── Custom UI (조건부) ────────────────────────────────────────────────────
    custom_labels = None
    if measure_type == "Custom":
        st.divider()
        custom_labels = render_custom_label_ui(selected_files, file_type, min_interval)

    # ── Process ───────────────────────────────────────────────────────────────
    st.divider()
    if st.button("Process", type="primary"):
        if not selected_files:
            st.error("처리할 파일을 선택해 주세요. (Scan 후 파일을 선택하세요)")
        else:
            with st.spinner("파일 처리 중…"):
                saved = process_files(
                    file_paths=selected_files,
                    file_type=file_type,
                    output_dir=output_dir,
                    voltage_col=voltage_col,
                    current_col=current_col,
                    thres_cur=thres_cur,
                    min_interval=min_interval,
                    measure_type=measure_type,
                    custom_labels=custom_labels,
                )
            if saved:
                st.success(
                    f"{len(saved)}개 파일 저장 완료:\n"
                    + "\n".join(f"- `{p}`" for p in saved)
                )
            else:
                st.warning("저장된 파일이 없습니다. 위의 오류 메시지를 확인하세요.")


main()
