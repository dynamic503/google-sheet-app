
import streamlit as st
import hashlib
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# --- Cấu hình kết nối Google Sheets ---
def connect_to_gsheets():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    sheet_id = os.getenv("SHEET_ID")
    if not creds_json or not sheet_id:
        st.error("Bạn chưa cấu hình biến môi trường GOOGLE_CREDENTIALS_JSON hoặc SHEET_ID")
        st.stop()

    import json
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(sheet_id)
    return sh

# --- Kiểm tra mật khẩu đã hash chưa ---
def is_hashed(pw):
    return len(pw) == 64 and re.fullmatch(r'[0-9a-f]+', pw)

# --- Hash mật khẩu ---
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# --- Lấy user, tự động cập nhật hash password ---
def get_users(sh):
    worksheet = sh.worksheet("User")
    data = worksheet.get_all_records()

    updated = False
    for idx, user in enumerate(data):
        pw = user['Password']
        if not is_hashed(pw):
            hashed = hash_password(pw)
            worksheet.update_cell(idx + 2, 2, hashed)  # dòng idx+2, cột 2 (Password)
            user['Password'] = hashed
            updated = True
    if updated:
        st.success("Đã tự động mã hóa mật khẩu chưa hash.")
    return data

# --- Xác thực đăng nhập ---
def check_login(sh, username, password):
    users = get_users(sh)
    hashed_input = hash_password(password)
    for user in users:
        if user['Username'] == username and user['Password'] == hashed_input:
            return user['Role']
    return None

# --- Đổi mật khẩu ---
def change_password(sh, username, old_pw, new_pw):
    worksheet = sh.worksheet("User")
    data = worksheet.get_all_records()

    hashed_old = hash_password(old_pw)
    hashed_new = hash_password(new_pw)

    for idx, user in enumerate(data):
        if user['Username'] == username and user['Password'] == hashed_old:
            worksheet.update_cell(idx + 2, 2, hashed_new)
            return True
    return False

# --- Giao diện Streamlit ---
def main():
    st.title("Ứng dụng quản lý - Đăng nhập")

    if 'login' not in st.session_state:
        st.session_state.login = False
    if 'username' not in st.session_state:
        st.session_state.username = ''
    if 'role' not in st.session_state:
        st.session_state.role = ''

    if not st.session_state.login:
        username = st.text_input("Tên đăng nhập")
        password = st.text_input("Mật khẩu", type="password")
        if st.button("Đăng nhập"):
            try:
                sh = connect_to_gsheets()
            except Exception as e:
                st.error(f"Kết nối Google Sheets lỗi: {e}")
                return

            role = check_login(sh, username, password)
            if role:
                st.session_state.login = True
                st.session_state.username = username
                st.session_state.role = role
                st.success(f"Đăng nhập thành công với quyền: {role}")
                st.experimental_rerun()
            else:
                st.error("Sai tên đăng nhập hoặc mật khẩu")
    else:
        st.write(f"Chào bạn, **{st.session_state.username}**! Quyền: **{st.session_state.role}**")
        if st.button("Đăng xuất"):
            st.session_state.login = False
            st.session_state.username = ''
            st.session_state.role = ''
            st.experimental_rerun()

        st.subheader("Đổi mật khẩu")
        old_pw = st.text_input("Mật khẩu cũ", type="password")
        new_pw = st.text_input("Mật khẩu mới", type="password")
        new_pw2 = st.text_input("Nhập lại mật khẩu mới", type="password")
        if st.button("Cập nhật mật khẩu"):
            if not old_pw or not new_pw or not new_pw2:
                st.error("Vui lòng nhập đầy đủ thông tin")
            elif new_pw != new_pw2:
                st.error("Mật khẩu mới không trùng khớp")
            else:
                try:
                    sh = connect_to_gsheets()
                except Exception as e:
                    st.error(f"Kết nối Google Sheets lỗi: {e}")
                    return

                if change_password(sh, st.session_state.username, old_pw, new_pw):
                    st.success("Đổi mật khẩu thành công! Vui lòng đăng nhập lại.")
                    st.session_state.login = False
                    st.session_state.username = ''
                    st.session_state.role = ''
                    st.experimental_rerun()
                else:
                    st.error("Mật khẩu cũ không đúng")

if __name__ == "__main__":
    main()
