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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# --- Đặt cấu hình trang đầu tiên ---
st.set_page_config(page_title="Quản lý nhập liệu - Agribank", page_icon="💻")

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
    .sidebar-logo {
        display: block;
        margin: 0 auto;
        width: 120px;
    }
    .branch-text {
        text-align: center;
        font-size: 14px;
        font-weight: bold;
        color: #333333;
        margin-top: 10px;
    }
    /* Nút sửa trong ag-Grid */
    .edit-button {
        background-color: #A91B2A;
        color: white;
        border: none;
        border-radius: 5px;
        padding: 5px 10px;
        font-size: 14px;
        cursor: pointer;
    }
    .edit-button:hover {
        background-color: #8B1623;
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
        return None

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
            data = worksheet.get_all_records()
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
        data = worksheet.get_all_records()
        return data
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
        raise
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu người dùng: {e}")
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
                return user.get('Role', 'User'), True  # True: bắt buộc đổi mật khẩu
            if stored_password == hashed_input:
                return user.get('Role', 'User'), False  # False: không cần đổi
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
        data = worksheet.get_all_records()
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
        row_data.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        worksheet.append_row(row_data)
        # Xóa cache cho sheet này
        for key in list(st.session_state.keys()):
            if key.startswith(f"{sheet_name}_"):
                del st.session_state[key]
        return True
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
        raise
    except Exception as e:
        st.error(f"Lỗi khi nhập liệu: {e}")
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
        row_data.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        worksheet.update(f"A{row_idx + 2}:{chr(65 + len(headers) - 1)}{row_idx + 2}", [row_data])
        # Xóa cache cho sheet này
        for key in list(st.session_state.keys()):
            if key.startswith(f"{sheet_name}_"):
                del st.session_state[key]
        return True
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            st.warning("Hệ thống đang bận, vui lòng thử lại sau ít giây.")
        raise
    except Exception as e:
        st.error(f"Lỗi khi cập nhật dữ liệu: {e}")
        return False

# --- Lấy dữ liệu đã nhập, hỗ trợ admin thấy tất cả ---
def get_user_data(sh, sheet_name, username, role, start_date=None, end_date=None, keyword=None):
    try:
        cache_key = f"{sheet_name}_{username}_{role}_{start_date}_{end_date}_{keyword}"
        # Kiểm tra số hàng trong sheet để bỏ cache nếu có dữ liệu mới
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
                data = worksheet.get_all_records()
                headers = worksheet.row_values(1)
                return headers, data

            headers, data = fetch_data()
            filtered_data = []
            for idx, row in enumerate(data):
                if role.lower() == 'admin' or row.get("Nguoi_nhap") == username:
                    if start_date and end_date:
                        try:
                            entry_time = datetime.strptime(row.get("Thoi_gian_nhap", ""), "%Y-%m-%d %H:%M:%S")
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
        return [], []

# --- Tìm kiếm trong sheet ---
def search_in_sheet(sh, sheet_name, keyword, column=None):
    cache_key = f"search_{sheet_name}_{keyword}_{column}"
    if cache_key not in st.session_state or st.session_state.get(f"{cache_key}_timestamp", 0) < time.time() - 60:
        try:
            worksheet = sh.worksheet(sheet_name)
            data = worksheet.get_all_records()
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
            return [], []
    return st.session_state[cache_key]

# --- Giao diện chính ---
def main():
    # Khởi tạo session state
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
    if 'edit_mode' not in st.session_state:
        st.session_state.edit_mode = False
    if 'edit_row_idx' not in st.session_state:
        st.session_state.edit_row_idx = None
    if 'edit_sheet' not in st.session_state:
        st.session_state.edit_sheet = None
    if 'selected_function' not in st.session_state:
        st.session_state.selected_function = "Nhập liệu"

    # Kết nối Google Sheets
    sh = connect_to_gsheets()
    if not sh:
        return

    # Kiểm tra khóa tài khoản
    if st.session_state.lockout_time > time.time():
        st.error(f"Tài khoản bị khóa. Vui lòng thử lại sau {int(st.session_state.lockout_time - time.time())} giây.")
        return

    # Sidebar: Logo, chữ chi nhánh, và menu điều hướng
    st.sidebar.image("https://ruybangphuonghoang.com/wp-content/uploads/2024/10/logo-agribank-scaled.jpg", use_container_width=False, output_format="auto", caption="", width=120)
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
        # Giao diện đăng nhập
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
        # Giao diện sau khi đăng nhập
        st.write(f"👋 Xin chào **{st.session_state.username}**! Quyền: **{st.session_state.role}**")

        if st.session_state.selected_function == "Đăng xuất":
            st.session_state.login = False
            st.session_state.username = ''
            st.session_state.role = ''
            st.session_state.login_attempts = 0
            st.session_state.show_change_password = False
            st.session_state.force_change_password = False
            st.session_state.edit_mode = False
            st.session_state.edit_row_idx = None
            st.session_state.edit_sheet = None
            st.session_state.selected_function = "Nhập liệu"
            st.success("Đã đăng xuất!")
            time.sleep(1)
            st.rerun()

        if st.session_state.selected_function in ["all", "Đổi mật khẩu"] or st.session_state.force_change_password:
            # Đổi mật khẩu
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
                                    st.session_state.edit_mode = False
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error("Mật khẩu cũ không chính xác.")

        if st.session_state.selected_function in ["all", "Nhập liệu"] and not st.session_state.force_change_password:
            # Nhập liệu
            st.subheader("📝 Nhập liệu")
            input_sheets = get_input_sheets(sh)
            if not input_sheets:
                st.error("Không tìm thấy sheet nhập liệu hợp lệ.")
            else:
                selected_sheet = st.selectbox("Chọn sheet để nhập liệu", input_sheets, key="input_sheet")
                required_columns, optional_columns = get_columns(sh, selected_sheet)
                if required_columns or optional_columns:
                    with st.form(f"input_form_{selected_sheet}"):
                        form_data = {}
                        for header in required_columns:
                            clean_header = header.rstrip('*')
                            st.markdown(f'<span class="required-label">{clean_header} (bắt buộc)</span>', unsafe_allow_html=True)
                            form_data[clean_header] = st.text_input("", key=f"{selected_sheet}_{clean_header}_input")
                        for header in optional_columns:
                            clean_header = header.rstrip('*')
                            form_data[clean_header] = st.text_input(f"{clean_header} (tùy chọn)", key=f"{selected_sheet}_{clean_header}_input")
                        submit_data = st.form_submit_button("Gửi")

                        if submit_data:
                            missing_required = [header.rstrip('*') for header in required_columns if not form_data.get(header.rstrip('*'))]
                            if missing_required:
                                st.error(f"Vui lòng nhập các trường bắt buộc: {', '.join(missing_required)}")
                            else:
                                if add_data_to_sheet(sh, selected_sheet, form_data, st.session_state.username):
                                    st.success("🎉 Dữ liệu đã được nhập thành công!")
                                else:
                                    st.error("Lỗi khi nhập dữ liệu. Vui lòng thử lại.")

        if st.session_state.selected_function in ["all", "Xem và sửa dữ liệu"] and not st.session_state.force_change_password:
            # Xem và sửa dữ liệu đã nhập
            st.subheader("📊 Xem và sửa dữ liệu đã nhập")
            view_sheets = get_view_sheets(sh)
            if not view_sheets:
                st.error("Không tìm thấy sheet xem dữ liệu hợp lệ.")
            else:
                selected_view_sheet = st.selectbox("Chọn sheet để xem", view_sheets, key="view_sheet")
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input("Từ ngày", value=datetime.now().date() - timedelta(days=30), key="start_date")
                with col2:
                    end_date = st.date_input("Đến ngày", value=datetime.now().date(), key="end_date")
                search_keyword = st.text_input("Tìm kiếm bản ghi", key="view_search_keyword")
                
                # Nút áp dụng và làm mới
                col1, col2, col3 = st.columns(3)
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
                    if headers and user_data:
                        # Chuẩn bị dữ liệu cho ag-Grid
                        df = pd.DataFrame([row for _, row in user_data])
                        df.insert(0, 'row_idx', [row_idx for row_idx, _ in user_data])
                        df['sheet'] = selected_view_sheet

                        # Cấu hình ag-Grid
                        gb = GridOptionsBuilder.from_dataframe(df)
                        gb.configure_column(
                            "Sửa",
                            headerName="Sửa",
                            pinned="left",
                            width=100,
                            cellRenderer=JsCode("""
                                function(params) {
                                    return '<button class="edit-button" onclick="Streamlit.setComponentValue({row_idx: ' + params.data.row_idx + ', sheet: \'' + params.data.sheet + '\'})">Sửa</button>';
                                }
                            """)
                        )
                        gb.configure_column("row_idx", hide=True)
                        gb.configure_column("sheet", hide=True)
                        gb.configure_selection('single')
                        gb.configure_grid_options(domLayout='normal')
                        grid_options = gb.build()

                        # Hiển thị bảng
                        grid_response = AgGrid(
                            df,
                            gridOptions=grid_options,
                            update_mode=GridUpdateMode.VALUE_CHANGED,
                            allow_unsafe_jscode=True,
                            height=400,
                            fit_columns_on_grid_load=True,
                            custom_css={"#gridToolBar": {"display": "none"}},
                        )

                        # Xử lý sự kiện nhấn nút Sửa
                        if grid_response['component_value']:
                            st.session_state.edit_mode = True
                            st.session_state.edit_row_idx = grid_response['component_value']['row_idx']
                            st.session_state.edit_sheet = selected_view_sheet

                        # Form chỉnh sửa
                        if st.session_state.edit_mode and st.session_state.edit_sheet == selected_view_sheet:
                            st.subheader(f"Chỉnh sửa bản ghi #{st.session_state.edit_row_idx + 2}")
                            required_columns, optional_columns = get_columns(sh, selected_view_sheet)
                            with st.form(f"edit_form_{selected_view_sheet}_{st.session_state.edit_row_idx}"):
                                edit_data = {}
                                edit_row = user_data[st.session_state.edit_row_idx][1]
                                for header in required_columns:
                                    clean_header = header.rstrip('*')
                                    st.markdown(f'<span class="required-label">{clean_header} (bắt buộc)</span>', unsafe_allow_html=True)
                                    edit_data[clean_header] = st.text_input(
                                        "", 
                                        value=edit_row.get(clean_header, ''), 
                                        key=f"edit_{selected_view_sheet}_{clean_header}_{st.session_state.edit_row_idx}"
                                    )
                                for header in optional_columns:
                                    clean_header = header.rstrip('*')
                                    edit_data[clean_header] = st.text_input(
                                        f"{clean_header} (tùy chọn)", 
                                        value=edit_row.get(clean_header, ''), 
                                        key=f"edit_{selected_view_sheet}_{clean_header}_{st.session_state.edit_row_idx}"
                                    )
                                submit_edit = st.form_submit_button("Cập nhật")
                                cancel_edit = st.form_submit_button("Hủy")

                                if submit_edit:
                                    missing_required = [header.rstrip('*') for header in required_columns if not edit_data.get(header.rstrip('*'))]
                                    if missing_required:
                                        st.error(f"Vui lòng nhập các trường bắt buộc: {', '.join(missing_required)}")
                                    else:
                                        if update_data_in_sheet(sh, selected_view_sheet, st.session_state.edit_row_idx, edit_data, st.session_state.username):
                                            st.success("🎉 Bản ghi đã được cập nhật thành công!")
                                            st.session_state.edit_mode = False
                                            st.session_state.edit_row_idx = None
                                            st.session_state.edit_sheet = None
                                            st.session_state.filter_applied = False
                                            st.rerun()
                                        else:
                                            st.error("Lỗi khi cập nhật dữ liệu. Vui lòng thử lại.")
                                if cancel_edit:
                                    st.session_state.edit_mode = False
                                    st.session_state.edit_row_idx = None
                                    st.session_state.edit_sheet = None
                                    st.rerun()
                    else:
                        st.info("Không có dữ liệu nào được nhập trong khoảng thời gian hoặc từ khóa này.")

        if st.session_state.selected_function in ["all", "Tìm kiếm"] and not st.session_state.force_change_password:
            # Tìm kiếm
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
                    if headers and search_results:
                        df = pd.DataFrame(search_results)
                        st.dataframe(df)
                    else:
                        st.info("Không tìm thấy kết quả nào khớp với từ khóa.")

if __name__ == "__main__":
    main()