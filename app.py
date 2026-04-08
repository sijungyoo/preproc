import os
import datetime
import io

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants / Defaults
# ---------------------------------------------------------------------------
DEFAULT_INIT_DIR = r"D:\\"
_today = datetime.date.today().strftime("%y%m%d")
DEFAULT_OUTPUT_DIR = rf"D:\Multimedia\output_{_today}"

DEFAULT_VOLTAGE_COL = "VMeasCh2"   # UI label: 전압 컬럼명 (cur_col)
DEFAULT_CURRENT_COL = "ID"         # UI label: 전류 컬럼명 (vol_col)
DEFAULT_THRES_CUR = 1e-7
DEFAULT_MIN_INTERVAL = 1e-5

TIME_COL = "TimeOutput"
MAX_ROWS = 100

MEASURE_TYPES = ["ISPP", "Endurance", "Retention", "Custom"]
FILE_TYPES = ["xls", "nasca", "csv"]


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_xls(file_obj) -> pd.DataFrame:
    """Load a legacy .xls file from an in-memory buffer using xlrd."""
    import xlrd
    data = file_obj.read()
    wb = xlrd.open_workbook(file_contents=data)
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


def load_csv(file_obj) -> pd.DataFrame:
    """Load a CSV file."""
    return pd.read_csv(file_obj)


def load_file(uploaded_file, file_type: str, init_dir: str) -> pd.DataFrame:
    """Dispatch to the appropriate loader based on file_type."""
    if file_type == "xls":
        return load_xls(uploaded_file)
    elif file_type == "nasca":
        filepath = os.path.join(init_dir, uploaded_file.name)
        return load_nasca(filepath)
    elif file_type == "csv":
        return load_csv(uploaded_file)
    else:
        raise ValueError(f"Unknown file type: {file_type}")


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
    # Ensure the time column is numeric
    df[TIME_COL] = pd.to_numeric(df[TIME_COL], errors="coerce")

    diff = df[TIME_COL].diff()
    # Mark the first row as a split point too so we always start a new subset
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
                if i < len(values):
                    s[col_name] = values[i]
                else:
                    s[col_name] = ""
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
    df = df.rename(columns=rename_map)
    return df


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
    uploaded_files,
    file_type: str,
    init_dir: str,
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
    total = len(uploaded_files)

    for idx, uf in enumerate(uploaded_files):
        progress.progress((idx) / total, text=f"Processing {uf.name}…")
        try:
            # 1. Load
            df = load_file(uf, file_type, init_dir)

            # 2. Detect subsets
            subsets = detect_subsets(df, min_interval)
            if not subsets:
                st.warning(f"{uf.name}: no subsets detected — skipping.")
                continue

            # 3. Extract parameters per subset
            param_dicts = [extract_parameters(s) for s in subsets]

            # Attach extracted params as columns (currently all empty)
            param_cols = []
            for i, (s, params) in enumerate(zip(subsets, param_dicts)):
                for k, v in params.items():
                    subsets[i][k] = v
                    if k not in param_cols:
                        param_cols.append(k)

            # 4. Label subsets
            subsets = label_subsets(subsets, measure_type, custom_labels)

            # Determine label columns added by labeling
            label_cols = list(custom_labels.keys()) if (
                measure_type == "Custom" and custom_labels
            ) else []

            # 5. Trim, rename, and downsample each subset
            extra_cols = param_cols + label_cols
            processed_subsets = []
            for s in subsets:
                s = keep_and_rename_columns(s, voltage_col, current_col, extra_cols)
                s = downsample(s, MAX_ROWS)
                processed_subsets.append(s)

            # 6. Concatenate and save
            result_df = pd.concat(processed_subsets, ignore_index=True)
            stem = os.path.splitext(uf.name)[0]
            out_path = os.path.join(output_dir, f"{stem}_processed.csv")
            result_df.to_csv(out_path, index=False)
            saved_paths.append(out_path)

        except Exception as e:
            st.error(f"Error processing {uf.name}: {e}")

    progress.progress(1.0, text="Done.")
    return saved_paths


# ---------------------------------------------------------------------------
# UI Helpers
# ---------------------------------------------------------------------------

def _init_session_state():
    defaults = {
        "subset_count": 0,
        "label_headers": [],
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


def render_custom_label_ui(
    uploaded_files,
    file_type: str,
    init_dir: str,
    min_interval: float,
) -> dict | None:
    """
    Render the Custom measure-type UI:
      - "subset check" button
      - Add / Remove label column buttons
      - Per-column: editable header + one value input per subset

    Returns a dict {col_name: [val_per_subset]} or None if count is 0.
    """
    st.subheader("Custom Labeling")

    # ── Subset check ────────────────────────────────────────────────────────
    if st.button("subset check"):
        if not uploaded_files:
            st.warning("Upload at least one file before running subset check.")
        else:
            try:
                df = load_file(uploaded_files[0], file_type, init_dir)
                subsets = detect_subsets(df, min_interval)
                st.session_state.subset_count = len(subsets)
                # Reset label value keys so stale data doesn't carry over
            except Exception as e:
                st.error(f"Subset check failed: {e}")
            st.rerun()

    n = st.session_state.subset_count
    if n > 0:
        st.info(f"{n} subset(s) detected.")

        # ── Add / Remove label columns ───────────────────────────────────────
        btn_col1, btn_col2 = st.columns([1, 1])
        with btn_col1:
            if st.button("+ Add label column"):
                new_name = f"label_{len(st.session_state.label_headers) + 1}"
                st.session_state.label_headers.append(new_name)
                st.rerun()
        with btn_col2:
            if st.button("- Remove last") and st.session_state.label_headers:
                st.session_state.label_headers.pop()
                st.rerun()

        # ── Per-column inputs ────────────────────────────────────────────────
        for i in range(len(st.session_state.label_headers)):
            st.markdown(f"**Label column {i + 1}**")
            header_key = f"header_name_{i}"
            st.text_input(
                "Column name",
                value=st.session_state.label_headers[i],
                key=header_key,
            )

            # Lay out value inputs in rows of up to 4
            cols_per_row = 4
            for row_start in range(0, n, cols_per_row):
                row_cols = st.columns(min(cols_per_row, n - row_start))
                for j_offset, col in enumerate(row_cols):
                    j = row_start + j_offset
                    col.text_input(
                        f"Subset {j + 1}",
                        key=f"label_val_{i}_{j}",
                    )

            st.divider()

        # ── Collect current values ───────────────────────────────────────────
        result = {}
        for i in range(len(st.session_state.label_headers)):
            col_name = st.session_state.get(
                f"header_name_{i}",
                st.session_state.label_headers[i],
            )
            values = [
                st.session_state.get(f"label_val_{i}_{j}", "")
                for j in range(n)
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

    # ── File upload ──────────────────────────────────────────────────────────
    st.subheader("1. Upload Files")
    uploaded_files = st.file_uploader(
        "Select files to process",
        accept_multiple_files=True,
        type=["xls", "xlsx", "csv"],
    )

    # ── Directories ──────────────────────────────────────────────────────────
    st.subheader("2. Directories")
    dir_col1, dir_col2 = st.columns(2)
    with dir_col1:
        init_dir = st.text_input("Initial directory", value=DEFAULT_INIT_DIR)
    with dir_col2:
        output_dir = st.text_input("Output folder", value=DEFAULT_OUTPUT_DIR)

    # ── Processing parameters ────────────────────────────────────────────────
    st.subheader("3. Processing Parameters")
    param_col1, param_col2, param_col3 = st.columns(3)

    with param_col1:
        file_type = st.radio("File type", FILE_TYPES)
        if file_type == "nasca":
            st.info(
                "nasca mode uses xlwings (Windows + Excel required). "
                "Files must already exist at the Initial directory path."
            )

    with param_col2:
        voltage_col = st.text_input(
            "전압 컬럼명 (cur_col)", value=DEFAULT_VOLTAGE_COL
        )
        current_col = st.text_input(
            "전류 컬럼명 (vol_col)", value=DEFAULT_CURRENT_COL
        )

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

    # ── Measure Type ─────────────────────────────────────────────────────────
    st.subheader("4. Measure Type")
    measure_type = st.selectbox("Measure Type", MEASURE_TYPES)

    # ── Custom UI (conditional) ──────────────────────────────────────────────
    custom_labels = None
    if measure_type == "Custom":
        st.divider()
        custom_labels = render_custom_label_ui(
            uploaded_files, file_type, init_dir, min_interval
        )

    # ── Process ──────────────────────────────────────────────────────────────
    st.divider()
    if st.button("Process", type="primary"):
        if not uploaded_files:
            st.error("Please upload at least one file before processing.")
        else:
            with st.spinner("Processing files…"):
                saved = process_files(
                    uploaded_files=uploaded_files,
                    file_type=file_type,
                    init_dir=init_dir,
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
                    f"Saved {len(saved)} file(s):\n"
                    + "\n".join(f"- `{p}`" for p in saved)
                )
            else:
                st.warning("No files were saved. Check errors above.")


main()
