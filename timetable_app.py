import pandas as pd
import streamlit as st
import re
import io
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json # 'json' 라이브러리 추가

# --- 0. Streamlit 앱 기본 설정 ---
st.set_page_config(layout="wide")
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
    
    # *** (수정됨) subject_mapping 시트 추가 로드 ***
    try:
        ws_mapping = sh.worksheet('subject_mapping')
    except gspread.exceptions.WorksheetNotFound:
        st.error("오류: Google Sheet에 'subject_mapping' 탭이 없습니다! 관리자에게 문의하세요.")
        st.stop()
    # *** (수정 끝) ***

except Exception as e:
    st.error("Google Sheets 인증에 실패했습니다. Streamlit Cloud의 'Secrets' 설정이 올바른지 확인하세요.")
    st.error(f"오류: {e}")
    st.stop()

# --- 2. 엑셀 다운로드 함수 ---
@st.cache_data
def convert_df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data = output.getvalue()
    return processed_data

# --- 3. Google Sheet 데이터 로드 함수 (*** 'subject_mapping' 병합 수정됨 ***) ---
@st.cache_data(ttl=60) # 60초마다 캐시 갱신
def load_data_from_gs():
    """Google Sheet에서 3개의 탭을 모두 읽어 DataFrame으로 반환"""
    master_df = pd.DataFrame(ws_master.get_all_records())
    address_df = pd.DataFrame(ws_address.get_all_records())
    
    # *** (수정됨) subject_mapping 데이터 로드 ***
    mapping_df = pd.DataFrame(ws_mapping.get_all_records())
    
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
            
    # *** (수정됨) '영역' 및 '선택과목' 병합 로직 ***
    if not master_df.empty and not mapping_df.empty:
        if '선택과목' not in mapping_df.columns or '영역' not in mapping_df.columns:
            st.warning("경고: 'subject_mapping' 탭에 '선택과목' 또는 '영역' 컬럼이 없습니다.")
            master_df['영역'] = '기타'
            master_df['선택과목'] = master_df['과목'] # 원본 과목을 그대로 사용
        else:
            # master_data의 '과목'과 subject_mapping의 '선택과목'을 기준으로 병합
            master_df = pd.merge(master_df, mapping_df, left_on='과목', right_on='선택과목', how='left')
            master_df['영역'] = master_df['영역'].fillna('기타 (미분류)') # 맵핑에 없는 과목 처리
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

    return master_df, mapping_df # (수정됨) mapping_df도 반환

# --- 4. 신규 강좌 파일 가공 함수 (기존 로직) ---
def process_new_lecture_file(file):
    """업로드된 신규 강좌 파일(xls, xlsx, html)을 가공하여 DataFrame으로 반환"""
    # (기존 코드와 동일, 변경 없음)
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

# --- 5. 관리자 모드 (사이드바) ---
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
        help="갱신할 강사 주소록 파일을 업로드하세요."
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
                
                st.write("3.1/4: 데이터 병합 및 중복 제거 중...")
                combined_master_df = pd.concat([existing_master_df, new_master_df], ignore_index=True)
                combined_master_df['개강일'] = combined_master_df['개강일'].astype(str)
                combined_master_df = combined_master_df.drop_duplicates()
                
                st.write("3.2/4: 'master_data' 시트 업데이트 중...")
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
                st.rerun() # (수정됨) st.experimental_rerun -> st.rerun

            except Exception as e:
                st.error(f"DB 갱신 중 오류 발생: {e}")

elif password_attempt:
    st.sidebar.error("비밀번호가 틀렸습니다.")

# --- 6. 메인 화면 (데이터 로드 및 필터) ---
try:
    # (수정됨) mapping_data도 함께 로드
    master_data, mapping_data = load_data_from_gs() 
except Exception as e:
    st.error("데이터 로드에 실패했습니다. 관리자 모드에서 DB 갱신이 필요할 수 있습니다.")
    st.error(f"오류: {e}")
    st.stop()

if master_data.empty:
    st.warning("데이터베이스가 비어있습니다. 관리자 모드에서 데이터를 갱신해주세요.")
    st.stop()
if mapping_data.empty:
    st.warning("경고: 'subject_mapping' 시트가 비어있습니다. 필터가 작동하지 않을 수 있습니다.")
    # (앱을 중지시키지는 않음)

# --- 7. 상단 필터 (연도, 월) ---
all_years = sorted(master_data['연도'].astype(str).unique(), reverse=True)
selected_year = st.selectbox("연도 선택", all_years)

all_months = sorted(master_data[master_data['연도'].astype(str) == selected_year]['월'].unique())
selected_month = st.selectbox("월 선택", all_months)

filtered_data = master_data[
    (master_data['연도'].astype(str) == selected_year) & 
    (master_data['월'] == selected_month)
]

# --- 8. 좌측 탐색 패널 (*** '2단 필터'로 수정됨 ***) ---
col1, col2 = st.columns([1, 3])

with col1:
    st.header("Step 2: 강사 탐색")

    # --- [필터 1] 영역 선택 ---
    # subject_mapping 시트의 '영역' 컬럼에서 고유값 리스트 생성
    area_list = ['[영역 전체]'] + sorted(list(mapping_data['영역'].unique()))
    selected_area = st.selectbox("1. 영역 선택", area_list)

    # 1차: '영역'으로 데이터 필터링
    if selected_area == '[영역 전체]':
        data_after_area_filter = filtered_data
    else:
        data_after_area_filter = filtered_data[filtered_data['영역'] == selected_area]

    # --- [필터 2] 선택과목 선택 ---
    if selected_area == '[영역 전체]':
        # '영역 전체'일 경우, 세부과목 필터는 비활성화 (혹은 모든 과목)
        subject_list = ['[과목 전체]']
        selected_subject = st.selectbox("2. 선택과목 선택", subject_list, disabled=True)
    else:
        # '영역'이 선택되면, 해당 영역의 '과목' 목록만 필터링
        subject_list = ['[' + selected_area + ' 전체]'] + sorted(list(
            data_after_area_filter[data_after_area_filter['영역'] == selected_area]['과목'].unique()
        ))
        selected_subject = st.selectbox("2. 선택과목 선택", subject_list)

    # 2차: '선택과목'으로 데이터 필터링
    if selected_area == '[영역 전체]':
        data_after_subject_filter = data_after_area_filter
    elif selected_subject == '[' + selected_area + ' 전체]':
        data_after_subject_filter = data_after_area_filter # '사회탐구 전체' 등
    else:
        data_after_subject_filter = data_after_area_filter[data_after_area_filter['과목'] == selected_subject]

    # --- [필터 3] 강사명 검색 ---
    search_query = st.text_input("3. 강사명 검색 🔍")

    # 3차: '강사명'으로 데이터 필터링
    if search_query:
        searched_data = data_after_subject_filter[
            data_after_subject_filter['강사'].astype(str).str.contains(search_query, case=False)
        ]
    else:
        searched_data = data_after_subject_filter
    
    # --- [결과] 강사 목록 ---
    instructors_list = sorted(searched_data['강사'].unique())

    if not instructors_list:
        st.warning("검색 결과가 없습니다.")
        selected_instructor = None
    else:
        month_start_date = pd.to_datetime(f'{selected_year}-{selected_month.replace("월","")}-01', format='%Y-%m-%d', errors='coerce')
        def format_instructor_name(instructor_name):
            first_lecture_date = master_data.loc[master_data['강사'] == instructor_name, '최초 개강일'].min()
            if pd.notna(first_lecture_date) and pd.notna(month_start_date):
                if first_lecture_date >= month_start_date:
                    return f"{instructor_name} (신규)"
            return f"{instructor_name} (기존)"

        selected_instructor = st.radio(
            f"강사 선택 (결과: {len(instructors_list)}명)", 
            instructors_list,
            format_func=format_instructor_name
        )

# --- 9. 우측 시간표 패널 ---
with col2:
    if selected_instructor:
        st.header(f"🗓️ {selected_instructor} 강사 시간표 ({selected_year} / {selected_month})")

        instructor_data = filtered_data[filtered_data['강사'] == selected_instructor]

        days = ['월', '화', '수', '목', '금', '토', '일']
        time_slots = ['오전', '오후', '저녁']
        
        try:
            timetable_agg = instructor_data.groupby(['시간대', '요일']).apply(
                lambda x: "<br><br>".join(
                    f"<b>{row['학원']}</b><br>{row['과목']}<br>({row['강좌구분']})"
                    for _, row in x.iterrows()
                )
            ).reset_index(name='수업정보')
            
            timetable_pivot = timetable_agg.pivot(index='시간대', columns='요일', values='수업정보')
            display_df = timetable_pivot.reindex(columns=days, index=time_slots, fill_value="")
            
            st.markdown(display_df.to_html(escape=False, na_rep=""), unsafe_allow_html=True)
        
        except Exception as e:
            st.error(f"시간표를 그리는 중 오류 발생: {e}")
            st.dataframe(instructor_data)

        st.subheader("강사 정보")
        instructor_info = master_data[master_data['강사'] == selected_instructor].iloc[0]
        
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
        excel_data = convert_df_to_excel(instructor_data)
        st.download_button(
            label="[선택한 강사의 현재 데이터] 엑셀로 다운로드",
            data=excel_data,
            file_name=f"{selected_year}_{selected_month}_{selected_instructor}_시간표.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
