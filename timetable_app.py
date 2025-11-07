import pandas as pd
import streamlit as st
import re
import io
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json 
import numpy as np # (추가) 엑셀 쓰기 시, nan 값 처리를 위해

# --- 0. Streamlit 앱 기본 설정 ---
st.set_page_config(layout="wide")

# --- CSS 스타일 주입 (폰트, 그리드 고정) ---
CUSTOM_CSS = """
<style>
    /* 전체 기본 폰트 크기 줄이기 (기본 16px -> 14px) */
    body, .stApp, .stWidget {
        font-size: 14px;
    }
    /* 위젯 라벨(stRadio, stSelectbox) 폰트 크기 */
    .st-bu, .st-ag, .st-at, .st-bq, .st-ar, .st-as, label, .st-emotion-cache-1y4p8pa {
        font-size: 14px !important;
    }
    /* 헤더 크기 약간 조절 */
    h1 { font-size: 2.0rem; }
    h2 { font-size: 1.75rem; }
    h3 { font-size: 1.25rem; }
    
    /* 탭(Radio) 버튼 기본 스타일 (탭 디자인 아님) */
    div[role="radiogroup"] {
        justify-content: center; /* 버튼 그룹 중앙 정렬 */
    }
    
    /* 시간표 그리드 고정 (가장 중요) */
    table.timetable-grid { /* CSS 클래스 지정 */
        table-layout: fixed; /* 테이블 레이L아웃 고정 */
        width: 80%; /* 80%로 가로 폭 축소 */
        border-collapse: collapse;
    }
    table.timetable-grid th { /* 요일 헤더 (월~일) */
        width: 12.8%; 
        text-align: center; /* 헤더 중앙 정렬 */
        vertical-align: middle; /* 헤더 중앙 정렬 */
        font-size: 1.0rem; 
        background-color: #f0f2f6;
        padding: 8px;
        border: 1px solid #ddd;
    }
    table.timetable-grid td { /* 시간표 칸 (오전/오후/저녁) */
        height: 90px; /* 고정 높이 90px */
        vertical-align: middle; /* 세로 중앙 정렬 */
        text-align: center; /* 가로 중앙 정렬 */
        padding: 8px;
        border: 1px solid #ddd;
        width: 12.8%;
        word-wrap: break-word; 
    }
    /* 시간대 헤더 (오전/오후/저녁) - 굵게 */
    table.timetable-grid tr th:first-child, table.timetable-grid tr td:first-child {
        font-weight: bold;
        text-align: center;
        vertical-align: middle; /* 세로 중앙 정렬 */
        background-color: #f0f2f6;
        width: 10%; /* 시간대 컬럼 너비 */
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
# --- CSS 끝 ---

st.title("강사별 출강 현황 통합 시간표 📊")

# --- 1. Google Sheets 인증 및 연결 ---
try:
    creds_dict = {
        "type": st.secrets["gcp_type"],
        "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"],
        "private_key": st.secrets["gcp_private_key"].replace('\\n', '\n'), 
        "client_email": st.secrets["gcp_client_email"],
        "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"],
        "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"],
        "universe_domain": st.secrets["gcp_universe_domain"]
    }
    
    sheet_url = st.secrets["google_sheet_url"]
    admin_password = st.secrets["admin_password"]
    
    scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)
    
    sh = gc.open_by_url(sheet_url)
    ws_master = sh.worksheet('master_data')
    ws_address = sh.worksheet('address_book')
    
    try:
        ws_mapping = sh.worksheet('subject_mapping')
    except gspread.exceptions.WorksheetNotFound:
        st.error("오류: Google Sheet에 'subject_mapping' 탭이 없습니다! 관리자에게 문의하세요.")
        st.stop()

except Exception as e:
    st.error("Google Sheets 인증에 실패했습니다. Streamlit Cloud의 'Secrets' 설정이 올바른지 확인하세요.")
    st.error(f"오류: {e}")
    st.stop()

# --- 2. 헬퍼 함수 (데이터 포맷팅) (*** \n -> <br> 수정됨 ***) ---

def format_cell_helper(x):
    """(엑셀/화면 공통) 그리드 셀 내용을 만듭니다."""
    entries = []
    for _, row in x.iterrows():
        # 원본 데이터 가져오기
        academy = str(row['학원']) # (수정) 문자열로 변환
        subject = str(row['과목']) # (수정) 문자열로 변환
        course_type = str(row['강좌구분']) # (수정) 문자열로 변환

        subject_display = "" 
        if row['영역'] not in ['국어', '수학', '영어', '한국사']:
            subject_display = f"{subject}<br>"
        
        if 'is_excel' in x.attrs: 
            # --- 엑셀 로직 ---
            subject_display_excel = subject_display.replace("<br>", "\n")
            # (유지) 엑셀은 \n을 올바르게 처리하므로 academy 변환 불필요
            entries.append(
                f"{academy}\n{subject_display_excel}({course_type})"
            )
        else: 
            # --- 화면(HTML) 로직 ---
            subject_display_html = subject_display # 이미 <br>이거나 공란
            # *** (수정됨) 화면(HTML)에서 \n을 <br>로 치환 ***
            academy_html = academy.replace('\n', '<br>')
            
            entries.append(
                f"<b>{academy_html}</b><br>{subject_display_html}({course_type})"
            )
    
    if 'is_excel' in x.attrs:
        return "\n\n".join(entries) # 엑셀에서는 2줄 띄기
    else:
        return "<br><br>".join(entries) # 화면에서는 <br>


# --- 3. 엑셀 다운로드 함수 ---

@st.cache_data
def convert_df_to_excel(df, index=False): 
    """(단순 다운로드) 단일 DataFrame을 엑셀로 변환"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=index, sheet_name='Sheet1') 
    processed_data = output.getvalue()
    return processed_data

# (수정됨) 엑셀 다운로드 'I열 버그' 및 '\n' 버그, '요일 헤더' 버그 모두 해결
@st.cache_data
def generate_area_grid_excel(filtered_data, mapping_df, hardcoded_area_order):
    """영역별로 시트를 나누고, 각 시트에 강사별 그리드를 나열"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        
        time_slots = ['오전', '오후', '저녁']
        days = ['월', '화', '수', '목', '금', '토', '일']
        
        workbook = writer.book
        cell_format = workbook.add_format({
            'align': 'center', 
            'valign': 'vcenter', 
            'text_wrap': True,
            'border': 1
        })
        header_format = workbook.add_format({
            'bold': True, 
            'align': 'center', 
            'valign': 'vcenter', 
            'fg_color': '#f0f2f6', 
            'border': 1
        })
        title_format = workbook.add_format({'bold': True, 'font_size': 14})
        
        areas_in_data = list(filtered_data['영역'].unique())
        area_list = [area for area in hardcoded_area_order if area in areas_in_data and area != '[영역 전체]']

        for area in area_list:
            start_row = 0 
            df_area = filtered_data[filtered_data['영역'] == area]
            
            subjects_in_mapping = list(mapping_df[mapping_df['영역'] == area]['선택과목'].unique())
            subject_order_map = {subject: i for i, subject in enumerate(subjects_in_mapping)}
            
            instructors_by_subject = df_area.groupby('과목')['강사'].unique().to_dict()
            
            all_subjects_in_area = sorted(df_area['과목'].unique(), key=lambda s: (subject_order_map.get(s, 99), s))
            
            instructors_in_area = []
            for subject in all_subjects_in_area:
                for instructor in sorted(instructors_by_subject.get(subject, [])):
                    if instructor not in instructors_in_area:
                        instructors_in_area.append(instructor)

            if not instructors_in_area:
                continue 
            
            worksheet = writer.book.add_worksheet(area) 
            
            for instructor in instructors_in_area:
                worksheet.write(start_row, 0, f"🗓️ {instructor} 강사 시간표", title_format)
                start_row += 1
                
                instructor_data = df_area[df_area['강사'] == instructor]
                instructor_data.attrs['is_excel'] = True 
                
                timetable_agg = instructor_data.groupby(['시간대', '요일']).apply(format_cell_helper).reset_index(name='수업정보')
                timetable_pivot = timetable_agg.pivot(index='시간대', columns='요일', values='수업정보')
                
                # (수정) '요일' 상위 헤더 삭제
                timetable_pivot.columns.name = None
                
                display_df = timetable_pivot.reindex(index=time_slots, columns=days, fill_value="") 
                display_df = display_df.reset_index().rename(columns={'index': '시간대'})
                
                # 3. 엑셀에 헤더 쓰기 (to_excel 대신 수동)
                for c_idx, col_name in enumerate(display_df.columns):
                    worksheet.write(start_row, c_idx, col_name, header_format)
                
                # 4. 엑셀에 데이터 쓰기 (수동)
                for r_idx in range(len(display_df)):
                    for c_idx in range(len(display_df.columns)):
                        
                        # (수정) TypeError 및 \n 버그 동시 해결
                        cell_value = display_df.iloc[r_idx, c_idx]
                        
                        if pd.isna(cell_value) or cell_value == "":
                            cell_value = ''
                        else:
                            cell_value = str(cell_value) 
                        
                        worksheet.write_string(start_row + 1 + r_idx, c_idx, cell_value, cell_format)
                
                worksheet.set_column(0, 0, 10) # 시간대
                worksheet.set_column(1, 7, 20) # 월~일
                worksheet.set_row(start_row, 25, header_format) # 헤더 행
                for r_idx in range(len(display_df)):
                    # (수정) I열 버그 해결: cell_format 제거
                    worksheet.set_row(start_row + 1 + r_idx, 80) # 데이터 행 (80px)

                start_row += len(display_df) + 3 # 3(데이터) + 1(헤더) + 3(공백)
        
        # writer.close() # with 문이 자동으로 close
    return output.getvalue()


# --- 3. Google Sheet 데이터 로드 함수 ( '중복 제거' 로직 ) ---
@st.cache_data(ttl=60) # 60초마다 캐시 갱신
def load_data_from_gs():
    """Google Sheet에서 3개의 탭을 모두 읽어 DataFrame으로 반환"""
    master_df = pd.DataFrame(ws_master.get_all_records())
    address_df = pd.DataFrame(ws_address.get_all_records())
    mapping_df = pd.DataFrame(ws_mapping.get_all_records())
    
    # 로드 시점에 즉시 중복 제거
    if not master_df.empty:
        key_columns = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분']
        existing_key_columns = [col for col in key_columns if col in master_df.columns]
        master_df = master_df.drop_duplicates(subset=existing_key_columns, keep='first')
    
    # '자택 주소' 병합 로직
    if not master_df.empty:
        if not address_df.empty and '강사명' in address_df.columns:
            if '자택 주소' not in address_df.columns:
                st.warning("경고: 주소록(address_book) 시트에 '자택 주소' 컬럼이 없습니다. 빈 값으로 처리합니다.")
                address_df['자택 주소'] = '정보 없음'
                
            if '강사명' not in master_df.columns:
                master_df['강사명'] = master_df['강사']
            
            master_df = pd.merge(master_df, address_df[['강사명', '자택 주소']], on='강사명', how='left')
            master_df['자택 주소'] = master_df['자택 주소'].fillna('정보 없음')
        else:
            if not address_df.empty:
                 st.warning("경고: 주소록(address_book) 시트에 '강사명' 컬럼이 없어 주소록을 병합할 수 없습니다.")
            master_df['자택 주소'] = '정보 없음'
            
    # '영역' 및 '선택과목' 병합 로직
    if not master_df.empty and not mapping_df.empty:
        if '선택과목' not in mapping_df.columns or '영역' not in mapping_df.columns:
            st.warning("경고: 'subject_mapping' 탭에 '선택과목' 또는 '영역' 컬럼이 없습니다.")
            master_df['영역'] = '기타'
            master_df['선택과목'] = master_df['과목'] 
        else:
            master_df = pd.merge(master_df, mapping_df[['선택과목', '영역']], left_on='과목', right_on='선택과목', how='left')
            master_df['영역'] = master_df['영역'].fillna('한국사') # '기타' -> '한국사'로
    else:
        master_df['영역'] = '기타'
        master_df['선택과목'] = master_df['과목']
    
    # '최초 개강일' 계산
    if '개강일' in master_df.columns:
        master_df['개강일_dt'] = pd.to_datetime(master_df['개강일'], errors='coerce')
        df_first_appearance = master_df.groupby('강사')['개강일_dt'].min().reset_index()
        df_first_appearance = df_first_appearance.rename(columns={'개강일_dt': '최초 개강일'})
        master_df = pd.merge(master_df, df_first_appearance, on='강사', how='left')
    else:
        master_df['최초 개강일'] = pd.NaT

    return master_df, mapping_df 

# --- 4. 신규 강좌 파일 가공 함수 (기존 로직) ---
def process_new_lecture_file(file):
    df_list = []
    try:
        file_bytes = file.getvalue()
        file_extension = file.name.split('.')[-1].lower()
        engine = 'openpyxl'
        if file_extension == 'xls':
            engine = 'xlrd'
        df = pd.read_excel(io.BytesIO(file_bytes), header=1, engine=engine)
    except Exception as e:
        if "Expected BOF record" in str(e) or "Unsupported format" in str(e) or "corrupt file" in str(e):
            st.warning(f"'{file.name}'은(는) Excel 형식이 아닙니다. HTML로 읽기를 시도합니다.")
            try:
                try: df_list_html = pd.read_html(io.BytesIO(file_bytes), header=1, encoding='utf-8')
                except UnicodeDecodeError: df_list_html = pd.read_html(io.BytesIO(file_bytes), header=1, encoding='cp949')
                if not df_list_html: raise ValueError("HTML에서 테이블을 찾지 못했습니다.")
                df = df_list_html[0]
                df = df[pd.to_numeric(df['No'], errors='coerce').notna()]
            except Exception as html_e:
                st.error(f"'{file.name}' 파일 로드 최종 실패. HTML 오류: {html_e}")
                return pd.DataFrame()
        else:
            st.error(f"'{file.name}' 파일 로드 오류: {e}.")
            return pd.DataFrame()
    df = df[df['판매'] != '폐강']
    df = df[~df['강좌구분'].astype(str).str.contains('코어')]
    df['개강일'] = pd.to_datetime(df['개강일'], errors='coerce')
    df['연도'] = df['개강일'].dt.year.fillna(0).astype(int).astype(str)
    df['월'] = df['과정'].astype(str).str.extract(r'(\d+월)')
    missing_month = df['월'].isnull()
    df.loc[missing_month, '월'] = df[missing_month]['개강일'].dt.month.fillna(0).astype(int).astype(str) + '월'
    df['월'] = df['월'].replace('0월', pd.NA)
    df['학원'] = df['학원'].astype(str).str.replace('러셀', '').str.replace('CORE', '').str.strip()
    
    # (수정) process_new_lecture_file에서 원본 학원명의 \n을 \n (실제 줄바꿈)으로 변경
    df['학원'] = df['학원'].astype(str).str.replace(r'\\n', '\n', regex=True)
    
    df_exploded = df.assign(수업시간_분리=df['수업시간'].astype(str).str.split('\n')).explode('수업시간_분리')
    df_exploded['요일'] = df_exploded['수업시간_분리'].str.extract(r'([월화수목금토일])')
    df_exploded['시작시간'] = df_exploded['수업시간_분리'].str.extract(r'(\d{2}:\d{2})')
    def map_time_slot(start_time):
        if pd.isna(start_time): return pd.NA
        try: hour = int(start_time.split(':')[0])
        except: return pd.NA
        if hour < 12: return '오전'
        elif 12 <= hour < 18: return '오후'
        else: return '저녁'
    df_exploded['시간대'] = df_exploded['시작시간'].apply(map_time_slot)
    final_columns = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분', '개강일']
    df_processed = df_exploded[final_columns].copy()
    df_processed = df_processed.dropna(subset=['연도', '월', '강사', '요일', '시간대'])
    df_processed = df_processed.drop_duplicates()
    df_processed['개강일'] = df_processed['개강일'].astype(str)
    return df_processed

# --- 5. 관리자 모드 ('DB 갱신' 로직) ---
st.sidebar.header("👨‍💼 관리자 모드")
password_attempt = st.sidebar.text_input("비밀번호 입력", type="password")

if password_attempt == admin_password:
    st.sidebar.success("관리자 인증 성공!")
    
    st.sidebar.subheader("신규 데이터 갱신")
    new_lecture_files = st.sidebar.file_uploader(
        "신규 강좌 내역 파일 (XLS/XLSX/HTML)",
        type=["xls", "xlsx"],
        accept_multiple_files=True,
        help="갱신할 월의 강좌 내역 파일을 업로드하세요."
    )
    new_address_file = st.sidebar.file_uploader(
        "신규 강사 주소록 파일 (XLS/XLSX)",
        type=["xls", "xlsx"],
        help="갱신할 강사 주소록 파일을 업로드하세요. '강사명', '자택 주소' 컬럼 필수."
    )
    
    if st.sidebar.button("[DB 갱신하기]"):
        with st.spinner("데이터베이스 갱신 중... (기존 데이터 + 신규 데이터)"):
            try:
                st.write("1/4: 기존 마스터 데이터 로드 중...")
                existing_master_df = pd.DataFrame(ws_master.get_all_records())
                
                st.write("2/4: 신규 강좌 파일 가공 중...")
                new_dataframes = []
                for file in new_lecture_files:
                    processed_df = process_new_lecture_file(file)
                    new_dataframes.append(processed_df)
                
                if not new_dataframes:
                    st.error("갱신할 신규 강좌 파일이 없습니다.")
                    st.stop()
                    
                new_master_df = pd.concat(new_dataframes, ignore_index=True)
                
                st.write("3.1/4: 데이터 병합 중...")
                combined_master_df = pd.concat([existing_master_df, new_master_df], ignore_index=True)
                combined_master_df['개강일'] = combined_master_df['개강일'].astype(str)
                
                key_columns = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분']
                existing_key_columns = [col for col in key_columns if col in combined_master_df.columns]
                combined_master_df = combined_master_df.drop_duplicates(subset=existing_key_columns, keep='first')
                st.write(f"3.2/4: 중복 제거 완료 (기준: {len(existing_key_columns)}개 키)")
                
                st.write("3.3/4: 'master_data' 시트 업데이트 중...")
                ws_master.clear()
                ws_master.update([combined_master_df.columns.values.tolist()] + combined_master_df.astype(str).values.tolist())
                
                if new_address_file:
                    st.write("4/4: 'address_book' 시트 업데이트 중...")
                    address_df = pd.read_excel(new_address_file, engine='openpyxl' if new_address_file.name.endswith('xlsx') else 'xlrd')
                    if '강사명' not in address_df.columns or '자택 주소' not in address_df.columns:
                        st.error("업로드한 주소록 파일에 '강사명' 또는 '자택 주소' 컬럼이 없습니다! 주소록이 업데이트되지 않았습니다.")
                    else:
                        ws_address.clear()
                        ws_address.update([address_df.columns.values.tolist()] + address_df.astype(str).values.tolist())
                else:
                    st.warning("주소록 파일이 업로드되지 않았습니다. 'address_book' 시트는 갱신되지 않았습니다.")

                st.success("데이터베이스 갱신 완료!")
                st.info("데이터 캐시를 삭제합니다. 1분 후 앱이 자동 갱신됩니다.")
                st.cache_data.clear()
                st.rerun()

            except Exception as e:
                st.error(f"DB 갱신 중 오류 발생: {e}")

elif password_attempt:
    st.sidebar.error("비밀번호가 틀렸습니다.")

# --- 6. 메인 화면 (데이터 로드) ---
try:
    master_data, mapping_df = load_data_from_gs() 
except Exception as e:
    st.error("데이터 로드에 실패했습니다. 관리자 모드에서 DB 갱신이 필요할 수 있습니다.")
    st.error(f"오류: {e}")
    st.stop()

if master_data.empty:
    st.warning("데이터베이스가 비어있습니다. 관리자 모드에서 데이터를 갱신해주세요.")
    st.stop()
if mapping_df.empty:
    st.warning("경고: 'subject_mapping' 시트가 비어있습니다. 필터가 작동하지 않을 수 있습니다.")

# --- 7. (수정됨) 공용 필터 (페이지 상단) ---
if 'selected_instructor' not in st.session_state:
    st.session_state.selected_instructor = None
if 'main_view' not in st.session_state: 
    st.session_state.main_view = "전체 출강 현황" 

all_years = sorted(master_data['연도'].astype(str).unique(), reverse=True)

year_options = list(all_years)
year_index = 0
if st.session_state.get('year_select') in year_options:
    year_index = year_options.index(st.session_state.year_select)
    
r1_col1, r1_col2, r1_col_spacer = st.columns([1, 1, 4]) 
with r1_col1:
    selected_year = st.selectbox("연도", year_options, 
                                 index=year_index, 
                                 key="year_select")
    
all_months = sorted(master_data[master_data['연도'].astype(str) == selected_year]['월'].astype(str).unique())

month_options = list(all_months)
month_index = 0
if st.session_state.get('month_select') in month_options:
    month_index = month_options.index(st.session_state.month_select)
    
with r1_col2:
    selected_month = st.selectbox("월", month_options, 
                                  index=month_index, 
                                  key="month_select")

filtered_data = master_data[
    (master_data['연도'].astype(str) == selected_year) & 
    (master_data['월'].astype(str) == selected_month)
]
st.divider()

hardcoded_area_order = ['[영역 전체]', '국어', '수학', '영어', '사회탐구', '과학탐구', '논술&제2외국어', '한국사']

# --- 8. (수정됨) 탭(Tab) -> 라디오(Radio) 버튼으로 변경 ---
selected_view = st.radio(
    "보기 선택:",
    ["전체 출강 현황", "강사별 시간표"],
    horizontal=True,
    key="main_view", # 세션에 저장하여 탭 전환 버그 해결
    label_visibility="collapsed" 
)
st.divider()

# --- 9. (수정됨) '전체 출강 현황' 뷰 ---
if selected_view == "전체 출강 현황":
    st.header(f"📊 {selected_year}년 {selected_month} 전체 출강 현황")
    
    if filtered_data.empty:
        st.warning("해당 연/월에 데이터가 없습니다.")
    else:
        try:
            filtered_data['학원'] = filtered_data['학원'].fillna('학원미정')
            
            status_pivot = filtered_data.pivot_table(
                index=['영역', '과목', '강사'],
                columns='학원',
                values='요일', 
                aggfunc='count',
                fill_value=0 
            )
            
            def format_status(val):
                if val > 0:
                    return "■" # (요청사항) 채워진 사각형
                return None 
            
            status_pivot = status_pivot.applymap(format_status).fillna('') 
            
            area_order_map = {area: i for i, area in enumerate(hardcoded_area_order)} 
            subject_order_map = {subject: i for i, subject in enumerate(mapping_df['선택과목'])}
            
            pivot_index = status_pivot.index.to_frame(index=False) 
            
            pivot_index['area_order'] = pivot_index['영역'].map(area_order_map).fillna(99)
            pivot_index['subject_order'] = pivot_index['과목'].map(subject_order_map).fillna(99)
            
            sorted_indices = pivot_index.sort_values(
                by=['area_order', 'subject_order', '강사'],
                ascending=[True, True, True]
            ).index
            
            status_pivot_sorted = status_pivot.iloc[sorted_indices]

            st.info("표가 가로로 긴 경우, 표 내부에서 스크롤할 수 있습니다.")
            st.dataframe(status_pivot_sorted, use_container_width=True)
            
            status_excel = convert_df_to_excel(status_pivot_sorted, index=True)
            st.download_button(
                label="[출강 현황] 엑셀로 다운로드",
                data=status_excel,
                file_name=f"{selected_year}_{selected_month}_전체출강현황.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
        except Exception as e:
            st.error(f"출강 현황표 생성 중 오류 발생: {e}")
            st.dataframe(filtered_data) 

# --- 10. (수정됨) '강사별 시간표' 뷰 ---
else: # if selected_view == "강사별 시간표":
    col1, col2 = st.columns([1, 3]) 

    # --- 10-1. 좌측 탐색 패널 ---
    with col1:
        
        # --- (수정됨) Row 1: Area/Subject ---
        available_areas_in_mapping = list(mapping_df['영역'].unique())
        available_areas_in_data = list(master_data['영역'].unique())
        all_available_areas = sorted(list(set(available_areas_in_mapping + available_areas_in_data)))
        area_list = [area for area in hardcoded_area_order if area in all_available_areas]
        other_areas = [area for area in all_available_areas if area not in hardcoded_area_order and area != '[영역 전체]']
        area_list.extend(other_areas)
        
        r2_col1, r2_col2 = st.columns(2)
        with r2_col1:
            selected_area = st.selectbox("영역", area_list, key="area_select")

        # [필터 2] 로직
        if selected_area == '[영역 전체]':
            data_after_area_filter = filtered_data
        else:
            data_after_area_filter = filtered_data[filtered_data['영역'] == selected_area]

        subject_list = []
        disable_subject_filter = False
        
        if selected_area == '[영역 전체]':
            subject_list = ['[과목 전체]']
            disable_subject_filter = True
        elif selected_area in ['국어', '수학', '영어', '한국사']:
            subject_list = [selected_area] # (수정됨) 텍스트 변경
            disable_subject_filter = True
        else:
            subjects_in_mapping = list(mapping_df[mapping_data['영역'] == selected_area]['선택과목'].unique())
            subjects_in_data = list(data_after_area_filter[data_after_area_filter['영역'] == selected_area]['과목'].unique())
            ordered_subject_list = [subject for subject in subjects_in_mapping if subject in subjects_in_data]
            other_subjects = sorted([subject for subject in subjects_in_data if subject not in ordered_subject_list])
            
            if selected_area == '논술&제2외국어':
                 subject_list = ordered_subject_list + other_subjects
            else: # 사탐, 과탐
                 subject_list = ['전체'] + ordered_subject_list + other_subjects
        
        with r2_col2:
            selected_subject = st.selectbox("선택과목", subject_list, disabled=disable_subject_filter, key="subject_select")

        if selected_area == '[영역 전체]' or disable_subject_filter:
            data_after_subject_filter = data_after_area_filter
        elif selected_subject == '전체': 
            data_after_subject_filter = data_after_area_filter
        else:
            data_after_subject_filter = data_after_area_filter[data_after_area_filter['과목'] == selected_subject]

        # --- (수정됨) Row 2: 강사명 검색 ---
        search_query = st.text_input("강사명 검색 🔍", key="search_query")

        if search_query:
            searched_data = data_after_subject_filter[
                data_after_subject_filter['강사'].astype(str).str.contains(search_query, case=False)
            ]
        else:
            searched_data = data_after_subject_filter
        
        st.divider() # 구분선

        # --- (수정됨) Row 3: 강사 목록 (스크롤) ---
        instructors_list = sorted(searched_data['강사'].unique())

        if not instructors_list:
            st.warning("검색 결과가 없습니다.")
            selected_instructor = None
            st.session_state.selected_instructor = None 
        else:
            default_index = 0
            if st.session_state.selected_instructor in instructors_list:
                default_index = instructors_list.index(st.session_state.selected_instructor)
            
            month_start_date = pd.to_datetime(f'{selected_year}-{selected_month.replace("월","")}-01', format='%Y-%m-%d', errors='coerce')
            def format_instructor_name(instructor_name):
                first_lecture_date = master_data.loc[master_data['강사'] == instructor_name, '최초 개강일'].min()
                if pd.notna(first_lecture_date) and pd.notna(month_start_date):
                    if first_lecture_date >= month_start_date:
                        return f"{instructor_name} (신규)"
                return f"{instructor_name} (기존)"
            
            st.markdown(f"**강사 선택** (결과: {len(instructors_list)}명)")
            with st.container(height=400):
                selected_instructor = st.radio(
                    "강사 선택", 
                    instructors_list,
                    format_func=format_instructor_name,
                    index=default_index, 
                    key='instructor_radio',
                    label_visibility="collapsed"
                )
            st.session_state.selected_instructor = selected_instructor 

    # --- 10-2. 우측 시간표 패널 (*** '영역별 그리드 다운로드' 수정됨 ***) ---
    with col2:
        if selected_instructor:
            st.header(f"🗓️ {selected_instructor} 강사 시간표 ({selected_year} / {selected_month})")

            instructor_data = filtered_data[filtered_data['강사'] == selected_instructor]

            days = ['월', '화', '수', '목', '금', '토', '일']
            time_slots = ['오전', '오후', '저녁']
            
            try:
                instructor_data.attrs['is_excel'] = False 
                timetable_agg = instructor_data.groupby(['시간대', '요일']).apply(format_cell_helper).reset_index(name='수업정보')
                
                timetable_pivot = timetable_agg.pivot(index='시간대', columns='요일', values='수업정보')
                
                timetable_pivot.columns.name = None
                
                display_df = timetable_pivot.reindex(index=time_slots, columns=days, fill_value="")
                
                display_df = display_df.reset_index().rename(columns={'index': '시간대'})
                
                st.markdown(display_df.to_html(escape=False, na_rep="", classes="timetable-grid", index=False, header=True), unsafe_allow_html=True)
            
            except Exception as e:
                st.error(f"시간표를 그리는 중 오류 발생: {e}")
                st.dataframe(instructor_data)

            st.subheader("강사 정보")
            if not instructor_data.empty:
                instructor_info_full = master_data[master_data['강사'] == selected_instructor]
                if not instructor_info_full.empty:
                    instructor_info = instructor_info_full.iloc[0]
                    
                    first_lecture_date = instructor_info['최초 개강일']
                    is_new = False
                    if pd.notna(first_lecture_date) and pd.notna(month_start_date):
                        if first_lecture_date >= month_start_date:
                            is_new = True
                    
                    st.markdown(f"""
                    - **자택 주소**: {instructor_info['자택 주소']}
                    - **강사 상태**: {"신규 강사" if is_new else "기존 강사"} (최초 개강일: {first_lecture_date.strftime('%Y-%m-%d') if pd.notna(first_lecture_date) else '-'} )
                    """)
                
                st.subheader("데이터 다운로드")
                st.markdown("현재 선택된 **연/월**의 **모든 강사** 시간표를 다운로드합니다.")
                
                @st.cache_data
                def get_grid_excel_bytes(filtered_data, mapping_df, hardcoded_area_order):
                    # (수정) 캐시 무효화를 위해 함수 이름 변경 (v3)
                    return generate_area_grid_excel(filtered_data, mapping_df, hardcoded_area_order)

                excel_data_grid = get_grid_excel_bytes(filtered_data, mapping_df, hardcoded_area_order)
                
                st.download_button(
                    label=f"[{selected_month} 영역별 통합 그리드] 엑셀 다운로드",
                    data=excel_data_grid,
                    file_name=f"{selected_year}_{selected_month}_영역별_통합그리드.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="선택한 연/월의 전체 강사 그리드를 영역별 시트로 나누어 다운로드합니다."
                )
                
                excel_data_list = convert_df_to_excel(filtered_data.drop(columns=['개강일_dt', '최초 개강일', '선택과목'], errors='ignore'), index=False) 
                st.download_button(
                    label=f"[{selected_month} 전체 강사 목록] 엑셀 다운로드",
                    data=excel_data_list,
                    file_name=f"{selected_year}_{selected_month}_전체강사목록.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="선택한 연/월의 전체 강사 데이터를 목록(Raw Data) 형태로 다운로드합니다."
                )
            else:
                st.info("선택된 강사에 대한 표시할 정보가 없습니다.")
