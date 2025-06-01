import streamlit as st
import hashlib
import re
import os
import json
import time
from datetime import datetime, timedelta
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
import pytz
import gspread
from google.oauth2.service_account import Credentials
import polars as pl
import pandas as pd

# --- Cấu hình logging ---
logging.basicConfig(filename='app.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Đặt cấu hình trang đầu tiên ---
st.set_page_config(page_title="Quản lý nhập liệu - Agribank", page_icon="💻", layout="wide")

# --- CSS để thiết kế giao diện hiện đại, tông đỏ Agribank ---
st.markdown("""
    <style>
    .css-1d391kg { background-color: #F5F5F5; }
    .stButton>button {
        width: 100%; background-color: #A91B2A; color: white; border-radius: 8px; padding: 10px;
        font-size: 16px; font-weight: 500; border: none; margin-bottom: 5px; transition: background-color 0.3s;
    }
    .stButton>button:hover { background-color: #8B1623; color: white; }
    .required-label { color: red; font-weight: bold; }
    .sidebar .sidebar-content { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; }
    .sidebar-logo { display: block; margin: 0 auto; width: 200px; }
    .branch-text { text-align: center; font-size: 14px; font-weight: bold; color: #333333; margin-top: 10px; }
    .ag-root-wrapper { max-height: 70vh !important; overflow-x: auto !important; overflow-y: auto !important; }
    @media (max-width: 600px) { .ag-root-wrapper { max-height: 50vh !important; } }
    .ag-cell { white-space: normal !important; word-wrap: break-word !important; max-height: none !important; line-height: 1.5 !important; padding: 5px !important; }
    .ag-header-cell { white-space: normal !important; word-wrap: break-word !important; }
    </style>
""", unsafe_allow_html=True)

# --- Kết nối Google Sheets ---
@st.cache_resource
def connect_to_gsheets():
    try:
        creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        sheet_id = os.getenv("SHEET_ID")
        st.write("GOOGLE_CREDENTIALS_JSON length:", len(creds_json) if creds_json else 0)  # Debug
        st.write("SHEET_ID:", sheet_id)  # Debug
        if not creds_json or not sheet_id:
            st.error("Thiếu biến môi trường GOOGLE_CREDENTIALS_JSON hoặc SHEET_ID")
            return None
        
        creds_dict = json.loads(creds_json)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        gc = gspread.authorize(creds)
        spread = gc.open_by_key(sheet_id)
        return spread
    except Exception as e:
        st.error(f"Lỗi kết nối Google Sheets: {e}")
        logger.error(f"Lỗi kết nối Google Sheets: {e}")
        return None

# --- Lấy định dạng cột từ hàng thứ 2 với xử lý ngày và số 0 ---
def get_column_formats_from_row2(spread, sheet_name):
    try:
        worksheet = spread.worksheet(sheet_name)
        data = worksheet.get_all_values()
        headers = data[0]
        row2_values = data[1] if len(data) > 1 else ['' for _ in headers]
        formats = {}
        for header, value in zip(headers, row2_values):
            header_clean = str(header).rstrip('*')
            if not value:
                formats[header_clean] = 'text'
            else:
                try:
                    datetime.strptime(str(value), '%d/%m/%Y')
                    formats[header_clean] = 'date'
                except ValueError:
                    try:
                        float_value = float(value)
                        if str(value).startswith('0') and len(str(value)) > 1:
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

# --- Làm sạch dữ liệu DataFrame với Polars, giữ số 0 và định dạng ngày ---
def clean_dataframe(df, headers, column_formats):
    for col in df.columns:
        if column_formats.get(col, 'text') == 'date':
            df = df.with_columns(
                pl.col(col).cast(pl.Utf8).apply(
                    lambda x: datetime.strptime(x, '%m/%d/%Y').strftime('%d/%m/%Y') 
                    if pd.notna(x) and re.match(r'^\d{2}/\d{2}/\d{4}$', str(x)) else x
                ).alias(col)
            )
        elif column_formats.get(col, 'text') in ['text', 'number']:
            df = df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
    return df

# --- Validate chuỗi nhập liệu ---
def validate_input(value, field_name):
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
    retry=retry_if_exception_type(Exception)
)
def get_sheet_config(spread):
    cache_key = "sheet_config"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            worksheet = spread.worksheet("Config")
            data = worksheet.get_all_values()
            if not data or len(data) <= 1:
                st.error("Sheet Config trống. Vui lòng thêm dữ liệu với các cột: Sheetname, Tìm kiếm, Nhập, Xem đã nhập.")
                return []
            headers = data[0]
            rows = data[1:]
            config = [dict(zip(headers, row)) for row in rows]
            st.session_state[cache_key] = config
            st.session_state[f"{cache_key}_timestamp"] = time.time()
        except Exception as e:
            st.error(f"Lỗi khi đọc sheet Config: {e}")
            logger.error(f"Lỗi khi đọc sheet Config: {e}")
            return []
    return st.session_state[cache_key]

# --- Lấy danh sách sheet nhập liệu từ Config ---
def get_input_sheets(spread):
    cache_key = "input_sheets"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            config = get_sheet_config(spread)
            if not config:
                return []
            sheets = [row['Sheetname'] for row in config if row.get('Nhập') == '1']
            existing_sheets = [s.title for s in spread.worksheets()]
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
def get_lookup_sheets(spread):
    cache_key = "lookup_sheets"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            config = get_sheet_config(spread)
            if not config:
                return []
            sheets = [row['Sheetname'] for row in config if row.get('Tìm kiếm') == '1']
            existing_sheets = [s.title for s in spread.worksheets()]
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
def get_view_sheets(spread):
    cache_key = "view_sheets"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            config = get_sheet_config(spread)
            if not config:
                return []
            sheets = [row['Sheetname'] for row in config if row.get('Xem đã nhập') == '1']
            existing_sheets = [s.title for s in spread.worksheets()]
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
    retry=retry_if_exception_type(Exception)
)
def get_users(spread):
    try:
        worksheet = spread.worksheet("User")
        data = worksheet.get_all_values()
        if not data or len(data) <= 1:
            return []
        headers = data[0]
        rows = data[1:]
        return [dict(zip(headers, row)) for row in rows]
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu người dùng: {e}")
        logger.error(f"Lỗi khi lấy dữ liệu người dùng: {e}")
        return []

# --- Xác thực người dùng ---
def check_login(spread, username, password):
    if not username or not password:
        st.error("Tên đăng nhập hoặc mật khẩu không được để trống.")
        return None, False
    users = get_users(spread)
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
    retry=retry_if_exception_type(Exception)
)
def change_password(spread, username, old_pw, new_pw):
    try:
        worksheet = spread.worksheet("User")
        data = worksheet.get_all_values()
        if not data or len(data) <= 1:
            return False
        headers = data[0]
        rows = data[1:]
        hashed_old = hash_password(old_pw)
        hashed_new = hash_password(new_pw)
        for idx, row in enumerate(rows):
            if row[headers.index('Username')] == username and (str(row[headers.index('Password')]) == old_pw or str(row[headers.index('Password')]) == hashed_old):
                rows[idx][headers.index('Password')] = hashed_new
                worksheet.update('A2', [headers] + rows)
                return True
        return False
    except Exception as e:
        st.error(f"Lỗi khi đổi mật khẩu: {e}")
        logger.error(f"Lỗi khi đổi mật khẩu: {e}")
        return False

# --- Lấy tiêu đề cột từ sheet, tách cột bắt buộc (*) ---
def get_columns(spread, sheet_name):
    cache_key = f"columns_{sheet_name}"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            worksheet = spread.worksheet(sheet_name)
            headers = worksheet.row_values(1)
            required_columns = [h for h in headers if str(h).endswith('*')]
            optional_columns = [h for h in headers if not str(h).endswith('*') and h not in ["Nguoi_nhap", "Thoi_gian_nhap"]]
            st.session_state[cache_key] = (required_columns, optional_columns)
            st.session_state[f"{cache_key}_timestamp"] = time.time()
        except Exception as e:
            st.error(f"Lỗi khi lấy tiêu đề cột: {e}")
            logger.error(f"Lỗi khi lấy tiêu đề cột: {e}")
            return [], []
    return st.session_state[cache_key]

# --- Kiểm tra và thêm cột Nguoi_nhap, Thoi_gian_nhap nếu chưa có ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception)
)
def ensure_columns(spread, sheet_name):
    try:
        worksheet = spread.worksheet(sheet_name)
        headers = worksheet.row_values(1)
        if "Nguoi_nhap" not in headers:
            headers.append("Nguoi_nhap")
            worksheet.append_row(["Nguoi_nhap"])
        if "Thoi_gian_nhap" not in headers:
            headers.append("Thoi_gian_nhap")
            worksheet.append_row(["Thoi_gian_nhap"])
        return headers
    except Exception as e:
        st.error(f"Lỗi khi kiểm tra/thêm cột: {e}")
        logger.error(f"Lỗi khi kiểm tra/thêm cột: {e}")
        return []

# --- Thêm dữ liệu vào sheet ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception)
)
def add_data_to_sheet(spread, sheet_name, data, username):
    try:
        worksheet = spread.worksheet(sheet_name)
        headers = ensure_columns(spread, sheet_name)
        row_data = [data.get(str(header).rstrip('*'), '') for header in headers if header not in ["Nguoi_nhap", "Thoi_gian_nhap"]]
        row_data.append(username)
        vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        row_data.append(datetime.now(vn_timezone).strftime("%d/%m/%Y %H:%M:%S"))
        worksheet.append_row(row_data)
        for key in list(st.session_state.keys()):
            if key.startswith(f"{sheet_name}_"):
                del st.session_state[key]
        return True
    except Exception as e:
        st.error(f"Lỗi khi nhập liệu: {str(e)}")
        logger.error(f"Lỗi khi nhập liệu vào {sheet_name}: {str(e)}")
        return False

# --- Cập nhật bản ghi trong sheet ---
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception)
)
def update_data_in_sheet(spread, sheet_name, row_idx, data, username):
    try:
        worksheet = spread.worksheet(sheet_name)
        headers = ensure_columns(spread, sheet_name)
        row_data = [data.get(str(header).rstrip('*'), '') for header in headers if header not in ["Nguoi_nhap", "Thoi_gian_nhap"]]
        row_data.append(username)
        vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        row_data.append(datetime.now(vn_timezone).strftime("%d/%m/%Y %H:%M:%S"))
        worksheet.update_cell(row_idx + 2, 1, row_data[0])  # Cập nhật từ cột 1
        for i, value in enumerate(row_data[1:], start=2):
            worksheet.update_cell(row_idx + 2, i, value)
        for key in list(st.session_state.keys()):
            if key.startswith(f"{sheet_name}_"):
                del st.session_state[key]
        return True
    except Exception as e:
        st.error(f"Lỗi khi cập nhật dữ liệu: {str(e)}")
        logger.error(f"Lỗi khi cập nhật dữ liệu tại {sheet_name}, row {row_idx}: {str(e)}")
        return False

# --- Lấy dữ liệu đã nhập, hỗ trợ admin thấy tất cả ---
def get_user_data(spread, sheet_name, username, role, start_date=None, end_date=None, keyword=None):
    try:
        cache_key = f"{sheet_name}_{username}_{role}_{start_date}_{end_date}_{keyword}"
        worksheet = spread.worksheet(sheet_name)
        data = worksheet.get_all_values()
        if not data or len(data) <= 1:
            return [], []
        headers = data[0]
        rows = data[1:]
        df = pl.DataFrame(rows, schema=headers)
        row_count = len(rows)
        cached_row_count = st.session_state.get(f"{cache_key}_row_count", 0)

        if cache_key not in st.session_state or row_count > cached_row_count:
            column_formats = get_column_formats_from_row2(spread, sheet_name)
            df = clean_dataframe(df, headers, column_formats)
            filtered_data = df.filter(
                (pl.col("Nguoi_nhap") == username) | (role.lower() == 'admin')
            )
            if start_date and end_date:
                filtered_data = filtered_data.filter(
                    pl.col("Thoi_gian_nhap").str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S").dt.date().is_between(start_date, end_date)
                )
            if keyword:
                keyword = keyword.lower()
                filtered_data = filtered_data.filter(
                    pl.concat_str([pl.col(c) for c in filtered_data.columns]).str.to_lowercase().str.contains(keyword)
                )
            st.session_state[cache_key] = (headers, list(zip(range(len(filtered_data)), filtered_data.to_dicts())))
            st.session_state[f"{cache_key}_row_count"] = row_count
        return st.session_state[cache_key]
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu đã nhập: {e}")
        logger.error(f"Lỗi khi lấy dữ liệu đã nhập: {e}")
        return [], []

# --- Tìm kiếm trong sheet ---
def search_in_sheet(spread, sheet_name, keyword, column=None):
    cache_key = f"search_{sheet_name}_{keyword}_{column}"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            worksheet = spread.worksheet(sheet_name)
            data = worksheet.get_all_values()
            if not data or len(data) <= 1:
                st.session_state[cache_key] = ([], [])
                return st.session_state[cache_key]
            headers = data[0]
            rows = data[1:]
            df = pl.DataFrame(rows, schema=headers)
            column_formats = get_column_formats_from_row2(spread, sheet_name)
            df = clean_dataframe(df, headers, column_formats)
            if not keyword:
                st.session_state[cache_key] = (headers, df.to_dicts())
            else:
                keyword = keyword.lower()
                if column == "Tất cả":
                    filtered_data = df.filter(
                        pl.concat_str([pl.col(c) for c in df.columns]).str.to_lowercase().str.contains(keyword)
                    )
                else:
                    clean_column = str(column).rstrip('*')
                    filtered_data = df.filter(
                        pl.col(clean_column).str.to_lowercase().str.contains(keyword)
                    )
                st.session_state[cache_key] = (headers, filtered_data.to_dicts())
            st.session_state[f"{cache_key}_timestamp"] = time.time()
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

    spread = connect_to_gsheets()
    if not spread:
        return

    if st.session_state.lockout_time > time.time():
        st.error(f"Tài khoản bị khóa. Vui lòng thử lại sau {int(st.session_state.lockout_time - time.time())} giây.")
        return

    st.sidebar.image("https://ruybangphuonghoang.com/wp-content/uploads/2024/10/logo-agribank-scaled.jpg", use_container_width=False, output_format="auto", caption="", width=200)
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

                role, force_change_password = check_login(spread, username.strip(), password.strip())
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
                                if change_password(spread, st.session_state.username, old_password, new_password):
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
            input_sheets = get_input_sheets(spread)
            if not input_sheets:
                st.error("Không tìm thấy sheet nhập liệu hợp lệ.")
            else:
                selected_sheet = st.selectbox("Chọn sheet để nhập liệu", input_sheets, key="input_sheet")
                required_columns, optional_columns = get_columns(spread, selected_sheet)
                column_formats = get_column_formats_from_row2(spread, selected_sheet)
                if required_columns or optional_columns:
                    with st.form(f"input_form_{selected_sheet}"):
                        form_data = {}
                        for header in required_columns:
                            clean_header = str(header).rstrip('*')
                            st.markdown(f'<span class="required-label">{clean_header} (bắt buộc)</span>', unsafe_allow_html=True)
                            format_type = column_formats.get(clean_header, 'text')
                            if format_type == 'date':
                                form_data[clean_header] = st.date_input(
                                    label=clean_header, label_visibility="collapsed", key=f"{selected_sheet}_{clean_header}_input",
                                    value=None, format="DD/MM/YYYY"
                                )
                            else:
                                help_text = "Chỉ nhập số" if format_type == 'number' else None
                                form_data[clean_header] = st.text_input(
                                    label=clean_header, label_visibility="collapsed", key=f"{selected_sheet}_{clean_header}_input",
                                    help=help_text
                                )
                        for header in optional_columns:
                            clean_header = str(header).rstrip('*')
                            format_type = column_formats.get(clean_header, 'text')
                            if format_type == 'date':
                                form_data[clean_header] = st.date_input(
                                    label=clean_header, label_visibility="collapsed", key=f"{selected_sheet}_{clean_header}_input",
                                    value=None, format="DD/MM/YYYY"
                                )
                            else:
                                help_text = "Chỉ nhập số" if format_type == 'number' else None
                                placeholder = f"{clean_header} (tùy chọn, chỉ nhập số)" if format_type == 'number' else f"{clean_header} (tùy chọn)"
                                form_data[clean_header] = st.text_input(
                                    label=clean_header, label_visibility="collapsed", key=f"{selected_sheet}_{clean_header}_input",
                                    placeholder=placeholder, help=help_text
                                )
                        submit_data = st.form_submit_button("Gửi")

                        if submit_data:
                            missing_required = []
                            validated_data = {}
                            for header in required_columns:
                                clean_header = str(header).rstrip('*')
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
                            if missing_required:
                                st.error(f"Vui lòng nhập các trường bắt buộc: {', '.join(missing_required)}")
                                return
                            for header in optional_columns:
                                clean_header = str(header).rstrip('*')
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
                            if add_data_to_sheet(spread, selected_sheet, validated_data, st.session_state.username):
                                st.success("🎉 Dữ liệu đã được nhập thành công!")
                            else:
                                st.error("Lỗi khi nhập dữ liệu. Vui lòng kiểm tra log và thử lại.")

        if st.session_state.selected_function in ["all", "Xem và sửa dữ liệu"] and not st.session_state.force_change_password:
            st.subheader("📊 Xem và sửa dữ liệu đã nhập")
            view_sheets = get_view_sheets(spread)
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
                        spread, selected_view_sheet, st.session_state.username, st.session_state.role, start_date, end_date, search_keyword
                    )
                    if headers and user_data:
                        df = pl.DataFrame([row for _, row in user_data])
                        df = df.with_columns(pl.lit(range(len(df))).alias('row_idx'))
                        df = df.with_columns(pl.lit(selected_view_sheet).alias('sheet'))

                        # Tạo grid với giữ nguyên định dạng
                        gb = GridOptionsBuilder.from_dataframe(df.to_pandas())
                        for col in df.columns:
                            if col not in ['row_idx', 'sheet']:
                                gb.configure_column(col, minWidth=200, autoSize=True, wrapText=True, autoHeight=True, editable=True)
                            else:
                                gb.configure_column(col, hide=True)
                        gb.configure_grid_options(
                            domLayout='autoHeight', suppressHorizontalScroll=False, suppressColumnVirtualisation=False,
                            suppressCellFormat=True, autoSizeColumnsMode='fitCellContents', enableRangeSelection=True,
                            rowSelection='multiple', enableCellTextSelection=True
                        )
                        grid_response = AgGrid(
                            df.to_pandas(), gridOptions=gb.build(), update_mode=GridUpdateMode.VALUE_CHANGED,
                            data_return_mode=DataReturnMode.AS_INPUT, height=400 if len(df) < 10 else 600,
                            fit_columns_on_grid_load=True, allow_unsafe_jscode=True, custom_css={"#gridToolBar": {"display": "none"}}
                        )

                        updated_df = pl.from_pandas(pd.DataFrame(grid_response['data']))
                        if not updated_df.frame_equal(df):
                            for idx, row in enumerate(updated_df.to_dicts()):
                                row_idx = row['row_idx']
                                if pd.isna(row_idx) or not str(row_idx).isdigit():
                                    continue
                                original_row = df.filter(pl.col('row_idx') == row_idx).row(0) if row_idx in df['row_idx'].to_list() else None
                                if original_row and not all(row.get(k, '') == v for k, v in original_row.items() if k not in ['row_idx', 'sheet']):
                                    sheet_name = row['sheet']
                                    updated_data = {k: v for k, v in row.items() if k not in ['row_idx', 'sheet']}
                                    missing_required = []
                                    validated_data = {}
                                    required_columns, _ = get_columns(spread, sheet_name)
                                    for header in required_columns:
                                        clean_header = str(header).rstrip('*')
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
                                    if update_data_in_sheet(spread, sheet_name, int(row_idx), validated_data, st.session_state.username):
                                        st.success(f"🎉 Bản ghi #{int(row_idx) + 2} đã được cập nhật thành công!", icon="✅")
                                    else:
                                        st.error("Lỗi khi cập nhật dữ liệu. Vui lòng kiểm tra log và thử lại.")
                    else:
                        st.info("Không có dữ liệu nào được nhập trong khoảng thời gian hoặc từ khóa này.")

        if st.session_state.selected_function in ["all", "Tìm kiếm"] and not st.session_state.force_change_password:
            st.subheader("🔍 Tìm kiếm")
            lookup_sheets = get_lookup_sheets(spread)
            if not lookup_sheets:
                st.error("Không tìm thấy sheet tra cứu hợp lệ.")
            else:
                selected_lookup_sheet = st.selectbox("Chọn sheet để tìm kiếm", lookup_sheets, key="lookup_sheet")
                required_columns, optional_columns = get_columns(spread, selected_lookup_sheet)
                headers = [str(h).rstrip('*') for h in required_columns] + [str(h) for h in optional_columns]
                search_column = st.selectbox("Chọn cột để tìm kiếm", ["Tất cả"] + headers, key="search_column")
                keyword = st.text_input("Nhập từ khóa tìm kiếm", key="search_keyword")
                if st.button("Tìm kiếm", key="search_button"):
                    headers, search_results = search_in_sheet(spread, selected_lookup_sheet, keyword, search_column)
                    if headers and search_results:
                        st.dataframe(pl.DataFrame(search_results).to_pandas())
                    else:
                        st.info("Không tìm thấy kết quả nào khớp với từ khóa.")

if __name__ == "__main__":
    main()
