import streamlit as st
import hashlib
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import time

# --- Kết nối Google Sheets ---
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

# --- Kiểm tra xem chuỗi đã mã hóa SHA256 chưa ---
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
def get_users(sh):
    try:
        worksheet = sh.worksheet("User")
        data = worksheet.get_all_records()
        return data
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
            # Kiểm tra mật khẩu thô
            if stored_password == password:
                return user.get('Role', 'User'), True  # True: bắt buộc đổi mật khẩu
            # Kiểm tra mật khẩu hash
            if stored_password == hashed_input:
                return user.get('Role', 'User'), False  # False: không cần đổi
    return None, False

# --- Đổi mật khẩu ---
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
    except Exception as e:
        st.error(f"Lỗi khi đổi mật khẩu: {e}")
        return False

# --- Giao diện chính ---
def main():
    st.set_page_config(page_title="Quản lý Đăng nhập", page_icon="🔐")
    st.title("Ứng dụng quản lý - Đăng nhập")

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

    # Kiểm tra khóa tài khoản
    if st.session_state.lockout_time > time.time():
        st.error(f"Tài khoản bị khóa. Thử lại sau {int(st.session_state.lockout_time - time.time())} giây.")
        return

    if not st.session_state.login:
        with st.form("login_form"):
            username = st.text_input("Tên đăng nhập", max_chars=50)
            password = st.text_input("Mật khẩu", type="password", max_chars=50)
            submit = st.form_submit_button("Đăng nhập")

            if submit:
                if st.session_state.login_attempts >= 5:
                    st.session_state.lockout_time = time.time() + 300
                    st.error("Quá nhiều lần thử đăng nhập. Tài khoản bị khóa trong 5 phút.")
                    return

                sh = connect_to_gsheets()
                if not sh:
                    return

                role, force_change = check_login(sh, username.strip(), password)
                if role:
                    st.session_state.login = True
                    st.session_state.username = username.strip()
                    st.session_state.role = role
                    st.session_state.login_attempts = 0
                    st.session_state.show_change_password = force_change
                    st.session_state.force_change_password = force_change
                    st.success(f"Đăng nhập thành công với quyền: {role}")
                    if force_change:
                        st.warning("Mật khẩu của bạn chưa được mã hóa. Vui lòng đổi mật khẩu ngay.")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.session_state.login_attempts += 1
                    st.error(f"Sai tên đăng nhập hoặc mật khẩu. Còn {5 - st.session_state.login_attempts} lần thử.")
    else:
        st.write(f"👋 Xin chào **{st.session_state.username}**! Quyền: **{st.session_state.role}**")
        
        # Hiển thị nút Đăng xuất và Đổi mật khẩu nếu không bắt buộc đổi
        if not st.session_state.force_change_password:
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("Đăng xuất"):
                    st.session_state.login = False
                    st.session_state.username = ''
                    st.session_state.role = ''
                    st.session_state.login_attempts = 0
                    st.session_state.show_change_password = False
                    st.session_state.force_change_password = False
                    st.success("Đã đăng xuất!")
                    time.sleep(1)
                    st.rerun()
            
            with col2:
                if st.button("Đổi mật khẩu"):
                    st.session_state.show_change_password = True
                    st.rerun()

        # Hiển thị form đổi mật khẩu nếu cần
        if st.session_state.show_change_password:
            st.subheader("🔒 Đổi mật khẩu")
            with st.form("change_password_form"):
                old_pw = st.text_input("Mật khẩu cũ", type="password", max_chars=50)
                new_pw = st.text_input("Mật khẩu mới", type="password", max_chars=50)
                new_pw2 = st.text_input("Nhập lại mật khẩu mới", type="password", max_chars=50)
                submit_change = st.form_submit_button("Cập nhật mật khẩu")

                if submit_change:
                    if not old_pw or not new_pw or not new_pw2:
                        st.error("Vui lòng nhập đầy đủ các trường.")
                    elif new_pw != new_pw2:
                        st.error("Mật khẩu mới không khớp.")
                    else:
                        is_strong, msg = is_strong_password(new_pw)
                        if not is_strong:
                            st.error(msg)
                        else:
                            sh = connect_to_gsheets()
                            if not sh:
                                return
                            if change_password(sh, st.session_state.username, old_pw, new_pw):
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

if __name__ == "__main__":
    main()
