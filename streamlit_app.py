import streamlit as st
import hashlib
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time
from datetime import datetime, timedelta
import pandas as pd
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
import pytz

# --- Cấu hình logging ---
logging.basicConfig(filename='app.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Đặt cấu hình trang đầu tiên ---
st.set_page_config(page_title="Quản lý nhập liệu - Agribank", page_icon="💻", layout="wide")

# --- CSS để thiết kế giao diện hiện đại, tông đỏ Agribank ---
st.markdown("""
    <style>
    /* Sidebar nền */
    .css-1d391kg {
        background-color: #F5F5F5;
    }
    /* Nút sidebar */
    .stButton>button {
        width: 100%;
        background-color: #A91B2A;
        color: white;
        border-radius: 8px;
        padding: 10px;
        font-size: 16px;
        font-weight: 500;
        border: none;
        margin-bottom: 5px;
        transition: background-color 0.3s;
    }
    .stButton>button:hover {
        background-color: #8B1623;
        color: white;
    }
    /* Trường bắt buộc bôi đỏ */
    .required-label {
        color: red;
        font-weight: bold;
    }
    /* Logo và chữ chi nhánh */
    .sidebar .sidebar-content {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        height: 100%;
    }
    .sidebar-logo {
        display: block;
        margin: 0 auto;
        width: 200px;
    }
    .branch-text {
        text-align: center;
        font-size: 14px;
        font-weight: bold;
        color: #333333;
        margin-top: 10px;
    }
    /* Tối ưu hiển thị bảng trên mobile */
    .ag-root-wrapper {
        max-height: 70vh !important;
        overflow-x: auto !important;
        overflow-y: auto !important;
    }
    @media (max-width: 600px) {
        .ag-root-wrapper {
            max-height: 50vh !important;
        }
    }
    /* Đảm bảo nội dung cột không bị cắt */
    .ag-cell {
        white-space: normal !important;
        word-wrap: break-word !important;
        max-height: none !important;
        line-height: 1.5 !important;
        padding: 5px !important;
    }
    .ag-header-cell {
        white-space: normal !important;
        word-wrap: break-word !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- Kết nối Google Sheets ---
@st.cache_resource
def connect_to_gsheets():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        sheet_id = os.getenv("SHEET_ID")
        
        if not creds_json or not sheet_id:
            st.error("Thiếu biến môi trường GOOGLE_CREDENTIALS_JSON hoặc SHEET_ID")
            return None
        
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open_by_key(sheet_id)
    except Exception as e:
        st.error(f"Lỗi kết nối Google Sheets: {e}")
        logger.error(f"Lỗi kết nối Google Sheets: {e}")
        return None

# --- Lấy định dạng cột từ hàng thứ 2 với xử lý ngày ---
def get_column_formats_from_row2(sh, sheet_name):
    try:
        worksheet = sh.worksheet(sheet_name)
        headers = worksheet.row_values(1)  # Hàng tiêu đề
        row2_values = worksheet.row_values(2)  # Hàng thứ 2
        formats = {}
        for header, value in zip(headers, row2_values):
            header_clean = header.rstrip('*')
            if not value:  # Nếu giá trị rỗng, mặc định là text
                formats[header_clean] = 'text'
            else:
                try:
                    # Kiểm tra xem có phải ngày không
                    datetime.strptime(value, '%d/%m/%Y')
                    formats[header_clean] = 'date'
                except ValueError:
                    try:
                        float(value)  # Kiểm tra xem có phải số không
                        if value.startswith('0') and len(value) > 1:  # Nếu bắt đầu bằng 0, xem như text (SĐT, CCCD)
                            formats[header_clean] = 'text'
                        else:
                            formats[header_clean] = 'number'
                    except ValueError:
                        formats[header_clean] = 'text'
        return formats
    except Exception as e:
        st.error(f"Lỗi khi lấy định dạng cột từ hàng thứ 2: {e}")
        logger.error(f"Lỗi khi lấy định dạng cột từ hàng thứ 2: {e}")
        return {}

# --- Làm sạch dữ liệu DataFrame mà không ép kiểu, chuyển đổi ngày ---
def clean_dataframe(df, headers, column_formats):
    """Làm sạch DataFrame, giữ nguyên định dạng gốc và chuyển đổi ngày sang DD/MM/YYYY."""
    for col in df.columns:
        try:
            # Chỉ làm sạch ký tự không in được, không ép kiểu
            original_values = df[col].copy()
            df[col] = df[col].apply(lambda x: ''.join(c for c in str(x) if c.isprintable() or ord(c) > 31) if pd.notna(x) else '')
            # Chuyển đổi ngày nếu cột là date
            if column_formats.get(col, 'text') == 'date':
                df[col] = df[col].apply(
                    lambda x: datetime.strptime(x, '%m/%d/%Y').strftime('%d/%m/%Y') if pd.notna(x) and re.match(r'^\d{2}/\d{2}/\d{4}$', str(x)) else x
                )
            # Log các ký tự bị loại bỏ
            for idx, (orig, cleaned) in enumerate(zip(original_values, df[col])):
                if orig != cleaned and pd.notna(orig):
                    diff_chars = ''.join(c for c in str(orig) if c not in cleaned and ord(c) <= 31)
                    if diff_chars:
                        logger.warning(f"Row {idx}, Column {col}: Removed non-printable chars: {repr(diff_chars)}")
            logger.info(f"DataFrame column {col} after cleaning: {df[col].head().to_list()}")
        except Exception as e:
            logger.error(f"Lỗi khi làm sạch cột {col}: {e}")
    logger.info(f"DataFrame cleaned: {df.head().to_dict()}")
    return df

# --- Validate chuỗi nhập liệu ---
def validate_input(value, field_name):
    """Kiểm tra chuỗi nhập liệu, linh hoạt hơn."""
    if not value:
        return False, f"Trường {field_name} không được để trống."
    cleaned_value = ''.join(c for c in str(value) if c.isprintable() or ord(c) > 31)
    if value.strip() == '':
        return False, f"Trường {field_name} chỉ chứa khoảng trắng."
    return True, cleaned_value

# --- Đọc cấu hình từ sheet Config ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(gspread.exceptions.APIError)
)
def get_sheet_config(sh):
    cache_key = "sheet_config"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            worksheet = sh.worksheet("Config")
            data = worksheet.get_all_records(value_render_option='UNFORMATTED_VALUE')
            if not data:
                st.error("Sheet Config trống. Vui lòng thêm dữ liệu với các cột: Sheetname, Tìm kiếm, Nhập, Xem đã nhập.")
                return []
            st.session_state[cache_key] = data
            st.session_state[f"{cache_key}_timestamp"] = time.time()
        except gspread.exceptions.WorksheetNotFound:
            st.error("Không tìm thấy sheet 'Config'. Vui lòng tạo sheet 'Config' với các cột: Sheetname, Tìm kiếm, Nhập, Xem đã nhập.")
            return []
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
            raise
        except Exception as e:
            st.error(f"Lỗi khi đọc sheet Config: {e}")
            logger.error(f"Lỗi khi đọc sheet Config: {e}")
            return []
    return st.session_state[cache_key]

# --- Lấy danh sách sheet nhập liệu từ Config ---
def get_input_sheets(sh):
    cache_key = "input_sheets"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            config = get_sheet_config(sh)
            if not config:
                return []
            sheets = [row['Sheetname'] for row in config if row.get('Nhập') == 1]
            existing_sheets = [ws.title for ws in sh.worksheets()]
            valid_sheets = [s for s in sheets if s in existing_sheets]
            if not valid_sheets:
                st.warning("Không tìm thấy sheet nhập liệu nào hợp lệ theo cấu hình Config.")
            st.session_state[cache_key] = valid_sheets
            st.session_state[f"{cache_key}_timestamp"] = time.time()
        except Exception as e:
            st.error(f"Lỗi khi lấy danh sách sheet nhập liệu: {e}")
            logger.error(f"Lỗi khi lấy danh sách sheet nhập liệu: {e}")
            return []
    return st.session_state[cache_key]

# --- Lấy danh sách sheet tra cứu từ Config ---
def get_lookup_sheets(sh):
    cache_key = "lookup_sheets"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            config = get_sheet_config(sh)
            if not config:
                return []
            sheets = [row['Sheetname'] for row in config if row.get('Tìm kiếm') == 1]
            existing_sheets = [ws.title for ws in sh.worksheets()]
            valid_sheets = [s for s in sheets if s in existing_sheets]
            if not valid_sheets:
                st.warning("Không tìm thấy sheet tra cứu nào hợp lệ theo cấu hình Config.")
            st.session_state[cache_key] = valid_sheets
            st.session_state[f"{cache_key}_timestamp"] = time.time()
        except Exception as e:
            st.error(f"Lỗi khi lấy danh sách sheet tra cứu: {e}")
            logger.error(f"Lỗi khi lấy danh sách sheet tra cứu: {e}")
            return []
    return st.session_state[cache_key]

# --- Lấy danh sách sheet xem đã nhập từ Config ---
def get_view_sheets(sh):
    cache_key = "view_sheets"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            config = get_sheet_config(sh)
            if not config:
                return []
            sheets = [row['Sheetname'] for row in config if row.get('Xem đã nhập') == 1]
            existing_sheets = [ws.title for ws in sh.worksheets()]
            valid_sheets = [s for s in sheets if s in existing_sheets]
            if not valid_sheets:
                st.warning("Không tìm thấy sheet xem dữ liệu nào hợp lệ theo cấu hình Config.")
            st.session_state[cache_key] = valid_sheets
            st.session_state[f"{cache_key}_timestamp"] = time.time()
        except Exception as e:
            st.error(f"Lỗi khi lấy danh sách sheet xem đã nhập: {e}")
            logger.error(f"Lỗi khi lấy danh sách sheet xem đã nhập: {e}")
            return []
    return st.session_state[cache_key]

# --- Kiểm tra xem chuỗi có mã hóa SHA256 chưa ---
def is_hashed(pw):
    return isinstance(pw, str) and len(pw) == 64 and re.fullmatch(r'[0-9a-fA-F]+', pw)

# --- Hàm mã hóa mật khẩu ---
def hash_password(password):
    return hashlib.sha256(str(password).encode('utf-8')).hexdigest() if password else ''

# --- Kiểm tra độ mạnh mật khẩu ---
def is_strong_password(password):
    if len(str(password)) < 8:
        return False, "Mật khẩu phải dài ít nhất 8 ký tự."
    if not re.search(r'[A-Z]', str(password)):
        return False, "Mật khẩu phải chứa ít nhất một chữ cái in hoa."
    if not re.search(r'[0-9]', str(password)):
        return False, "Mật khẩu phải chứa ít nhất một số."
    if not re.search(r'[!@#$%^&*(),.?":{}|<>]', str(password)):
        return False, "Mật khẩu phải chứa ít nhất một ký tự đặc biệt."
    return True, ""

# --- Lấy danh sách người dùng từ sheet "User" ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(gspread.exceptions.APIError)
)
def get_users(sh):
    try:
        worksheet = sh.worksheet("User")
        data = worksheet.get_all_records(value_render_option='UNFORMATTED_VALUE')
        return data
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
        raise
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu người dùng: {e}")
        logger.error(f"Lỗi khi lấy dữ liệu người dùng: {e}")
        return []

# --- Xác thực người dùng ---
def check_login(sh, username, password):
    if not username or not password:
        st.error("Tên đăng nhập hoặc mật khẩu không được để trống.")
        return None, False
    users = get_users(sh)
    hashed_input = hash_password(password)
    for user in users:
        stored_password = str(user.get('Password', ''))
        if user.get('Username') == username:
            if stored_password == password:
                return user.get('Role', 'User'), True
            if stored_password == hashed_input:
                return user.get('Role', 'User'), False
    return None, False

# --- Đổi mật khẩu ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(gspread.exceptions.APIError)
)
def change_password(sh, username, old_pw, new_pw):
    try:
        worksheet = sh.worksheet("User")
        data = worksheet.get_all_records(value_render_option='UNFORMATTED_VALUE')
        hashed_old = hash_password(old_pw)
        hashed_new = hash_password(new_pw)

        for idx, user in enumerate(data):
            stored_password = str(user.get('Password', ''))
            if user.get('Username') == username and (stored_password == old_pw or stored_password == hashed_old):
                worksheet.update_cell(idx + 2, 2, hashed_new)
                return True
        return False
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
        raise
    except Exception as e:
        st.error(f"Lỗi khi đổi mật khẩu: {e}")
        logger.error(f"Lỗi khi đổi mật khẩu: {e}")
        return False

# --- Lấy tiêu đề cột từ sheet, tách cột bắt buộc (*) ---
def get_columns(sh, sheet_name):
    cache_key = f"columns_{sheet_name}"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            worksheet = sh.worksheet(sheet_name)
            headers = worksheet.row_values(1)
            required_columns = [h for h in headers if h.endswith('*')]
            optional_columns = [h for h in headers if not h.endswith('*') and h not in ["Nguoi_nhap", "Thoi_gian_nhap"]]
            st.session_state[cache_key] = (required_columns, optional_columns)
            st.session_state[f"{cache_key}_timestamp"] = time.time()
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
            raise
        except Exception as e:
            st.error(f"Lỗi khi lấy tiêu đề cột: {e}")
            logger.error(f"Lỗi khi lấy tiêu đề cột: {e}")
            return [], []
    return st.session_state[cache_key]

# --- Kiểm tra và thêm cột Nguoi_nhap, Thoi_gian_nhap nếu chưa có ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(gspread.exceptions.APIError)
)
def ensure_columns(sh, sheet_name):
    try:
        worksheet = sh.worksheet(sheet_name)
        headers = worksheet.row_values(1)
        if "Nguoi_nhap" not in headers:
            headers.append("Nguoi_nhap")
            worksheet.update_cell(1, len(headers), "Nguoi_nhap")
        if "Thoi_gian_nhap" not in headers:
            headers.append("Thoi_gian_nhap")
            worksheet.update_cell(1, len(headers), "Thoi_gian_nhap")
        return headers
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
        raise
    except Exception as e:
        st.error(f"Lỗi khi kiểm tra/thêm cột: {e}")
        logger.error(f"Lỗi khi kiểm tra/thêm cột: {e}")
        return []

# --- Thêm dữ liệu vào sheet ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(gspread.exceptions.APIError)
)
def add_data_to_sheet(sh, sheet_name, data, username):
    try:
        worksheet = sh.worksheet(sheet_name)
        headers = ensure_columns(sh, sheet_name)
        row_data = [data.get(header.rstrip('*'), '') for header in headers[:-2]]
        row_data.append(username)
        # Đặt múi giờ Việt Nam (UTC+7)
        vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        current_time = datetime.now(vn_timezone).strftime("%d/%m/%Y %H:%M:%S")
        row_data.append(current_time)
        worksheet.append_row(row_data)
        # Xóa cache liên quan
        for key in list(st.session_state.keys()):
            if key.startswith(f"{sheet_name}_"):
                del st.session_state[key]
        return True
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
            logger.error(f"API Error 429: Quá nhiều yêu cầu khi thêm dữ liệu vào {sheet_name}")
        raise
    except Exception as e:
        st.error(f"Lỗi khi nhập liệu: {str(e)}")
        logger.error(f"Lỗi khi nhập liệu vào {sheet_name}: {str(e)}")
        return False

# --- Cập nhật bản ghi trong sheet ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(gspread.exceptions.APIError)
)
def update_data_in_sheet(sh, sheet_name, row_idx, data, username):
    try:
        worksheet = sh.worksheet(sheet_name)
        headers = ensure_columns(sh, sheet_name)
        row_data = [data.get(header.rstrip('*'), '') for header in headers[:-2]]
        row_data.append(username)
        # Đặt múi giờ Việt Nam (UTC+7)
        vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        current_time = datetime.now(vn_timezone).strftime("%d/%m/%Y %H:%M:%S")
        row_data.append(current_time)
        worksheet.update(f"A{row_idx + 2}:{chr(65 + len(headers) - 1)}{row_idx + 2}", [row_data])
        # Xóa cache liên quan
        for key in list(st.session_state.keys()):
            if key.startswith(f"{sheet_name}_"):
                del st.session_state[key]
        return True
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
            logger.error(f"API Error 429: Quá nhiều yêu cầu khi cập nhật dữ liệu tại {sheet_name}, row {row_idx}")
        raise
    except Exception as e:
        st.error(f"Lỗi khi cập nhật dữ liệu: {str(e)}")
        logger.error(f"Lỗi khi cập nhật dữ liệu tại {sheet_name}, row {row_idx}: {str(e)}")
        return False

# --- Lấy dữ liệu đã nhập, hỗ trợ admin thấy tất cả ---
def get_user_data(sh, sheet_name, username, role, start_date=None, end_date=None, keyword=None):
    try:
        cache_key = f"{sheet_name}_{username}_{role}_{start_date}_{end_date}_{keyword}"
        worksheet = sh.worksheet(sheet_name)
        row_count = len(worksheet.get_all_records())
        cached_row_count = st.session_state.get(f"{cache_key}_row_count", 0)

        if cache_key not in st.session_state or row_count > cached_row_count:
            @retry(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=10),
                retry=retry_if_exception_type(gspread.exceptions.APIError)
            )
            def fetch_data():
                data = worksheet.get_all_records(value_render_option='UNFORMATTED_VALUE')
                headers = worksheet.row_values(1)
                return headers, data

            headers, data = fetch_data()
            filtered_data = []
            for idx, row in enumerate(data):
                if role.lower() == 'admin' or row.get("Nguoi_nhap") == username:
                    if start_date and end_date:
                        try:
                            entry_time = datetime.strptime(row.get("Thoi_gian_nhap", ""), "%d/%m/%Y %H:%M:%S")
                            if not (start_date <= entry_time.date() <= end_date):
                                continue
                        except ValueError:
                            continue
                    if keyword:
                        keyword = keyword.lower()
                        if not any(keyword in str(value).lower() for value in row.values()):
                            continue
                    filtered_data.append((idx, row))
            st.session_state[cache_key] = (headers, filtered_data)
            st.session_state[f"{cache_key}_row_count"] = row_count
        return st.session_state[cache_key]
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
        raise
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu đã nhập: {e}")
        logger.error(f"Lỗi khi lấy dữ liệu đã nhập: {e}")
        return [], []

# --- Tìm kiếm trong sheet ---
def search_in_sheet(sh, sheet_name, keyword, column=None):
    cache_key = f"search_{sheet_name}_{keyword}_{column}"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            worksheet = sh.worksheet(sheet_name)
            # Lấy dữ liệu với giá trị thô
            data = worksheet.get_all_records(value_render_option='UNFORMATTED_VALUE')
            headers = worksheet.row_values(1)
            if not keyword:
                st.session_state[cache_key] = (headers, data)
            else:
                keyword = keyword.lower()
                if column == "Tất cả":
                    filtered_data = [row for row in data if any(keyword in str(value).lower() for value in row.values())]
                else:
                    clean_column = column.rstrip('*')
                    filtered_data = [row for row in data if keyword in str(row.get(clean_column, '')).lower()]
                st.session_state[cache_key] = (headers, filtered_data)
            st.session_state[f"{cache_key}_timestamp"] = time.time()
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
            raise
        except Exception as e:
            st.error(f"Lỗi khi tìm kiếm dữ liệu: {e}")
            logger.error(f"Lỗi khi tìm kiếm dữ liệu: {e}")
            return [], []
    return st.session_state[cache_key]

# --- Giao diện chính ---
def main():
    if 'login' not in st.session_state:
        st.session_state.login = False
    if 'username' not in st.session_state:
        st.session_state.username = ''
    if 'role' not in st.session_state:
        st.session_state.role = ''
    if 'login_attempts' not in st.session_state:
        st.session_state.login_attempts = 0
    if 'lockout_time' not in st.session_state:
        st.session_state.lockout_time = 0
    if 'show_change_password' not in st.session_state:
        st.session_state.show_change_password = False
    if 'force_change_password' not in st.session_state:
        st.session_state.force_change_password = False
    if 'selected_function' not in st.session_state:
        st.session_state.selected_function = "Nhập liệu"

    sh = connect_to_gsheets()
    if not sh:
        return

    if st.session_state.lockout_time > time.time():
        st.error(f"Tài khoản bị khóa. Vui lòng thử lại sau {int(st.session_state.lockout_time - time.time())} giây.")
        return

    st.sidebar.image("https://ruybangphuonghoang.com/wp-content/uploads/2024/10/logo-agribank-scaled.jpg", use_container_width=False, output_format="auto", caption="", width=200, clamp=False)
    st.sidebar.markdown('<div class="branch-text">Chi nhánh tỉnh Quảng Trị</div>', unsafe_allow_html=True)
    st.sidebar.markdown("---")
    
    st.sidebar.title("Điều hướng")
    functions = ["Nhập liệu", "Xem và sửa dữ liệu", "Tìm kiếm", "Đổi mật khẩu", "Đăng xuất"]
    for func in functions:
        if st.sidebar.button(func, key=f"nav_{func}"):
            st.session_state.selected_function = func
    if st.sidebar.button("Hiển thị tất cả", key="show_all"):
        st.session_state.selected_function = "all"

    st.title("Ứng dụng quản lý nhập liệu - Agribank")

    if not st.session_state.login:
        st.subheader("🔐 Đăng nhập")
        with st.form("login_form"):
            username = st.text_input("Tên đăng nhập", max_chars=50, key="login_username")
            password = st.text_input("Mật khẩu", type="password", max_chars=50, key="login_password")
            submit = st.form_submit_button("Đăng nhập")

            if submit:
                if st.session_state.login_attempts >= 5:
                    st.session_state.lockout_time = time.time() + 300
                    st.error("Quá nhiều lần thử đăng nhập. Tài khoản bị khóa trong 5 phút.")
                    return

                role, force_change_password = check_login(sh, username.strip(), password.strip())
                if role:
                    st.session_state.login = True
                    st.session_state.username = username.strip()
                    st.session_state.role = role
                    st.session_state.login_attempts = 0
                    st.session_state.show_change_password = force_change_password
                    st.session_state.force_change_password = force_change_password
                    st.success(f"Đăng nhập thành công với quyền: {role}")
                    if force_change_password:
                        st.warning("Mật khẩu của bạn chưa được mã hóa. Vui lòng đổi mật khẩu ngay!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.session_state.login_attempts += 1
                    st.error(f"Sai tên đăng nhập hoặc mật khẩu. Còn {5 - st.session_state.login_attempts} lần thử.")
    else:
        st.write(f"👋 Xin chào **{st.session_state.username}**! Quyền: **{st.session_state.role}**")

        if st.session_state.selected_function == "Đăng xuất":
            st.session_state.login = False
            st.session_state.username = ''
            st.session_state.role = ''
            st.session_state.login_attempts = 0
            st.session_state.show_change_password = False
            st.session_state.force_change_password = False
            st.session_state.selected_function = "Nhập liệu"
            st.success("Đã đăng xuất!")
            time.sleep(1)
            st.rerun()

        if st.session_state.selected_function in ["all", "Đổi mật khẩu"] or st.session_state.force_change_password:
            st.subheader("🔒 Đổi mật khẩu")
            if st.session_state.show_change_password or not st.session_state.force_change_password:
                with st.form("change_password_form"):
                    old_password = st.text_input("Mật khẩu cũ", type="password", max_chars=50, key="old_password")
                    new_password = st.text_input("Mật khẩu mới", type="password", max_chars=50, key="new_password")
                    new_password2 = st.text_input("Nhập lại mật khẩu mới", type="password", max_chars=50, key="new_password2")
                    submit_change = st.form_submit_button("Cập nhật mật khẩu")

                    if submit_change:
                        if not old_password or not new_password or not new_password2:
                            st.error("Vui lòng nhập đầy đủ các trường.")
                        elif new_password != new_password2:
                            st.error("Mật khẩu mới không khớp.")
                        else:
                            is_valid, msg = is_strong_password(new_password)
                            if not is_valid:
                                st.error(msg)
                            else:
                                if change_password(sh, st.session_state.username, old_password, new_password):
                                    st.success("🎉 Đổi mật khẩu thành công! Vui lòng đăng nhập lại.")
                                    st.session_state.login = False
                                    st.session_state.username = ''
                                    st.session_state.role = ''
                                    st.session_state.login_attempts = 0
                                    st.session_state.show_change_password = False
                                    st.session_state.force_change_password = False
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Mật khẩu cũ không chính xác.")

        if st.session_state.selected_function in ["all", "Nhập liệu"] and not st.session_state.force_change_password:
            st.subheader("📝 Nhập liệu")
            input_sheets = get_input_sheets(sh)
            if not input_sheets:
                st.error("Không tìm thấy sheet nhập liệu hợp lệ.")
            else:
                selected_sheet = st.selectbox("Chọn sheet để nhập liệu", input_sheets, key="input_sheet")
                required_columns, optional_columns = get_columns(sh, selected_sheet)
                column_formats = get_column_formats_from_row2(sh, selected_sheet)
                if required_columns or optional_columns:
                    with st.form(f"input_form_{selected_sheet}"):
                        form_data = {}
                        for header in required_columns:
                            clean_header = header.rstrip('*')
                            st.markdown(f'<span class="required-label">{clean_header} (bắt buộc)</span>', unsafe_allow_html=True)
                            format_type = column_formats.get(clean_header, 'text')
                            if format_type == 'date':
                                form_data[clean_header] = st.date_input(
                                    label=clean_header,
                                    label_visibility="collapsed",
                                    key=f"{selected_sheet}_{clean_header}_input",
                                    value=None,
                                    format="DD/MM/YYYY"
                                )
                            else:
                                help_text = "Chỉ nhập số" if format_type == 'number' else None
                                form_data[clean_header] = st.text_input(
                                    label=clean_header,
                                    label_visibility="collapsed",
                                    key=f"{selected_sheet}_{clean_header}_input",
                                    help=help_text
                                )
                        for header in optional_columns:
                            clean_header = header.rstrip('*')
                            format_type = column_formats.get(clean_header, 'text')
                            if format_type == 'date':
                                form_data[clean_header] = st.date_input(
                                    label=clean_header,
                                    label_visibility="collapsed",
                                    key=f"{selected_sheet}_{clean_header}_input",
                                    value=None,
                                    format="DD/MM/YYYY"
                                )
                            else:
                                help_text = "Chỉ nhập số" if format_type == 'number' else None
                                placeholder = f"{clean_header} (tùy chọn, chỉ nhập số)" if format_type == 'number' else f"{clean_header} (tùy chọn)"
                                form_data[clean_header] = st.text_input(
                                    label=clean_header,
                                    label_visibility="collapsed",
                                    key=f"{selected_sheet}_{clean_header}_input",
                                    placeholder=placeholder,
                                    help=help_text
                                )
                        submit_data = st.form_submit_button("Gửi")

                        if submit_data:
                            missing_required = []
                            validated_data = {}
                            # Validate các trường bắt buộc
                            for header in required_columns:
                                clean_header = header.rstrip('*')
                                format_type = column_formats.get(clean_header, 'text')
                                value = form_data.get(clean_header, '')
                                if format_type == 'date':
                                    if value is None:
                                        st.error(f"Trường {clean_header} không được để trống.")
                                        missing_required.append(clean_header)
                                    else:
                                        validated_data[clean_header] = value.strftime("%d/%m/%Y")
                                elif format_type == 'number':
                                    if not value:
                                        st.error(f"Trường {clean_header} không được để trống.")
                                        missing_required.append(clean_header)
                                    elif not re.match(r'^\d+$', str(value)):
                                        st.error(f"Trường {clean_header} chỉ được nhập số.")
                                        missing_required.append(clean_header)
                                    else:
                                        validated_data[clean_header] = value
                                else:
                                    is_valid, result = validate_input(value, clean_header)
                                    if not is_valid:
                                        st.error(result)
                                        missing_required.append(clean_header)
                                    else:
                                        validated_data[clean_header] = result
                            # Nếu có trường bắt buộc bị thiếu, dừng lại và không lưu
                            if missing_required:
                                st.error(f"Vui lòng nhập các trường bắt buộc: {', '.join(missing_required)}")
                                return  # Dừng xử lý, không lưu dữ liệu
                            # Không validate các trường không bắt buộc, chỉ lấy giá trị
                            for header in optional_columns:
                                clean_header = header.rstrip('*')
                                format_type = column_formats.get(clean_header, 'text')
                                value = form_data.get(clean_header, '')
                                if format_type == 'date':
                                    validated_data[clean_header] = value.strftime("%d/%m/%Y") if value else ''
                                elif format_type == 'number':
                                    if value and not re.match(r'^\d+$', str(value)):
                                        st.error(f"Trường {clean_header} chỉ được nhập số.")
                                        return
                                    validated_data[clean_header] = value if value else ''
                                else:
                                    validated_data[clean_header] = value if value else ''
                            # Lưu dữ liệu nếu không có lỗi
                            if add_data_to_sheet(sh, selected_sheet, validated_data, st.session_state.username):
                                st.success("🎉 Dữ liệu đã được nhập thành công!")
                            else:
                                st.error("Lỗi khi nhập dữ liệu. Vui lòng kiểm tra log và thử lại.")

        if st.session_state.selected_function in ["all", "Xem và sửa dữ liệu"] and not st.session_state.force_change_password:
            st.subheader("📊 Xem và sửa dữ liệu đã nhập")
            view_sheets = get_view_sheets(sh)
            if not view_sheets:
                st.error("Không tìm thấy sheet xem dữ liệu hợp lệ.")
            else:
                selected_view_sheet = st.selectbox("Chọn sheet để xem", view_sheets, key="view_sheet")
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input("Từ ngày", value=datetime.now().date() - timedelta(days=90), key="start_date")
                with col2:
                    end_date = st.date_input("Đến ngày", value=datetime.now().date(), key="end_date")
                search_keyword = st.text_input("Tìm kiếm bản ghi", key="view_search_keyword")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Áp dụng bộ lọc", key="apply_filter"):
                        st.session_state.filter_applied = True
                with col2:
                    if st.button("Làm mới", key="refresh_data"):
                        for key in list(st.session_state.keys()):
                            if key.startswith(f"{selected_view_sheet}_"):
                                del st.session_state[key]
                        st.session_state.filter_applied = True

                if 'filter_applied' in st.session_state and st.session_state.filter_applied:
                    headers, user_data = get_user_data(
                        sh, selected_view_sheet, st.session_state.username, st.session_state.role, start_date, end_date, search_keyword
                    )
                    column_formats = get_column_formats_from_row2(sh, selected_view_sheet)
                    if headers and user_data:
                        df = pd.DataFrame([row for _, row in user_data])
                        df.insert(0, 'row_idx', [row_idx for row_idx, _ in user_data])
                        df['sheet'] = selected_view_sheet

                        df = clean_dataframe(df, headers, column_formats)

                        # Tạo grid với giữ nguyên định dạng
                        gb = GridOptionsBuilder.from_dataframe(df)
                        for col in df.columns:
                            if col not in ['row_idx', 'sheet']:
                                gb.configure_column(
                                    col,
                                    minWidth=200,
                                    autoSize=True,
                                    wrapText=True,
                                    autoHeight=True,
                                    editable=True
                                )
                            else:
                                gb.configure_column(col, hide=True)
                        gb.configure_grid_options(
                            domLayout='autoHeight',
                            suppressHorizontalScroll=False,
                            suppressColumnVirtualisation=False,
                            suppressCellFormat=True,  # Giữ nguyên định dạng từ Sheet
                            autoSizeColumnsMode='fitCellContents',
                            enableRangeSelection=True,
                            rowSelection='multiple',
                            enableCellTextSelection=True
                        )
                        grid_response = AgGrid(
                            df,
                            gridOptions=gb.build(),
                            update_mode=GridUpdateMode.VALUE_CHANGED,
                            data_return_mode=DataReturnMode.AS_INPUT,
                            height=400 if len(df) < 10 else 600,
                            fit_columns_on_grid_load=True,
                            allow_unsafe_jscode=True,
                            custom_css={"#gridToolBar": {"display": "none"}},
                        )

                        # Lấy dữ liệu đã chỉnh sửa
                        updated_df = pd.DataFrame(grid_response['data'])
                        if not updated_df.empty and not updated_df.equals(df):
                            for idx, row in updated_df.iterrows():
                                row_idx = row['row_idx']
                                if pd.isna(row_idx) or not str(row_idx).isdigit():
                                    continue  # Bỏ qua hàng không hợp lệ
                                original_row = df[df['row_idx'] == row_idx].iloc[0] if row_idx in df['row_idx'].values else None
                                if original_row is not None and not row.drop(['row_idx', 'sheet']).equals(original_row.drop(['row_idx', 'sheet'])):
                                    sheet_name = row['sheet']
                                    updated_data = row.drop(['row_idx', 'sheet']).to_dict()
                                    # Validate dữ liệu trước khi cập nhật
                                    missing_required = []
                                    validated_data = {}
                                    required_columns, _ = get_columns(sh, sheet_name)
                                    for header in required_columns:
                                        clean_header = header.rstrip('*')
                                        is_valid, result = validate_input(updated_data.get(clean_header, ''), clean_header)
                                        if not is_valid:
                                            st.error(result)
                                            return
                                        validated_data[clean_header] = result
                                        if not updated_data.get(clean_header):
                                            missing_required.append(clean_header)
                                    for header in updated_data:
                                        if header not in validated_data:
                                            _, result = validate_input(updated_data.get(header, ''), header)
                                            validated_data[header] = result
                                    if missing_required:
                                        st.error(f"Vui lòng nhập các trường bắt buộc: {', '.join(missing_required)}")
                                        return
                                    else:
                                        if update_data_in_sheet(sh, sheet_name, int(row_idx), validated_data, st.session_state.username):
                                            st.success(f"🎉 Bản ghi #{int(row_idx) + 2} đã được cập nhật thành công!", icon="✅")
                                        else:
                                            st.error("Lỗi khi cập nhật dữ liệu. Vui lòng kiểm tra log và thử lại.")
                                            return
                    else:
                        st.info("Không có dữ liệu nào được nhập trong khoảng thời gian hoặc từ khóa này.")

        if st.session_state.selected_function in ["all", "Tìm kiếm"] and not st.session_state.force_change_password:
            st.subheader("🔍 Tìm kiếm")
            lookup_sheets = get_lookup_sheets(sh)
            if not lookup_sheets:
                st.error("Không tìm thấy sheet tra cứu hợp lệ.")
            else:
                selected_lookup_sheet = st.selectbox("Chọn sheet để tìm kiếm", lookup_sheets, key="lookup_sheet")
                headers = [h.rstrip('*') for h in get_columns(sh, selected_lookup_sheet)[0]] + get_columns(sh, selected_lookup_sheet)[1]
                search_column = st.selectbox("Chọn cột để tìm kiếm", ["Tất cả"] + headers, key="search_column")
                keyword = st.text_input("Nhập từ khóa tìm kiếm", key="search_keyword")
                if st.button("Tìm kiếm", key="search_button"):
                    headers, search_results = search_in_sheet(sh, selected_lookup_sheet, keyword, search_column)
                    column_formats = get_column_formats_from_row2(sh, selected_lookup_sheet)
                    if headers and search_results:
                        df = pd.DataFrame(search_results)
                        df = clean_dataframe(df, headers, column_formats)
                        st.dataframe(df)
                    else:
                        st.info("Không tìm thấy kết quả nào khớp với từ khóa.")

if __name__ == "__main__":
    main()
