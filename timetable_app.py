import pandas as pd
import streamlit as st
import io
import gspread
from google.oauth2.service_account import Credentials
import numpy as np

# --- 0. Streamlit 앱 기본 설정 ---
st.set_page_config(layout="wide", page_title="강사별 통합 시간표")

# --- CSS 스타일 주입 (폰트, 그리드 고정) ---
CUSTOM_CSS = """
<style>
    body, .stApp, .stWidget { font-size: 14px; }
    .st-bu, .st-ag, .st-at, .st-bq, .st-ar, .st-as, label, .st-emotion-cache-1y4p8pa { font-size: 14px !important; }
    h1 { font-size: 2.0rem; }
    h2 { font-size: 1.75rem; }
    div[role="radiogroup"] { justify-content: center; }
    
    /* 시간표 그리드 스타일 (웹 화면용) */
    table.timetable-grid {
        table-layout: fixed;
        width: 100%; /* 가로 폭 꽉 채우기 */
        border-collapse: collapse;
        margin-bottom: 20px;
    }
    table.timetable-grid th {
        width: 12%; 
        text-align: center;
        vertical-align: middle;
        font-size: 1.0rem; 
        background-color: #f0f2f6;
        padding: 8px;
        border: 1px solid #ddd;
    }
    table.timetable-grid td {
        height: 100px;
        vertical-align: middle;
        text-align: center;
        padding: 5px;
        border: 1px solid #ddd;
        width: 12%;
        word-wrap: break-word;
        line-height: 1.6; /* 줄간격 확보 */
        white-space: normal; /* 줄바꿈 허용 */
    }
    /* 시간대 컬럼 */
    table.timetable-grid tr th:first-child, table.timetable-grid tr td:first-child {
        width: 8%;
        font-weight: bold;
        background-color: #f8f9fa;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


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
        st.error("오류: Google Sheet에 'subject_mapping' 탭이 없습니다!")
        st.stop()

except Exception as e:
    st.error(f"Google Sheets 연결 오류: {e}")
    st.stop()


# --- 2. [핵심 수정] 헬퍼 함수: 줄바꿈 문자 완벽 처리 ---
def clean_text(text):
    """모든 형태의 줄바꿈 문자를 실제 파이썬 줄바꿈(\n)으로 통일"""
    if pd.isna(text):
        return ""
    text = str(text)
    # 1. 엑셀이나 구글시트에서 올 수 있는 이스케이프 문자(\n)를 실제 엔터키로 변경
    # 순서 중요: \\n(글자) -> \n(기호)
    text = text.replace('\\n', '\n').replace('\\\\n', '\n')
    return text.strip()

def format_cell_helper(x):
    """(엑셀/화면 공통) 그리드 셀 내용을 만듭니다."""
    entries = []
    for _, row in x.iterrows():
        # 데이터 클렌징 (줄바꿈 기호 통일)
        academy = clean_text(row['학원'])
        subject = clean_text(row['과목'])
        course_type = clean_text(row['강좌구분'])
        
        # 과목 표시 로직
        subject_display = "" 
        if row['영역'] not in ['국어', '수학', '영어', '한국사']:
            subject_display = subject
        
        # [엑셀용]
        if 'is_excel' in x.attrs and x.attrs['is_excel']: 
            content = f"{academy}\n"
            if subject_display:
                content += f"{subject_display}\n"
            content += f"({course_type})"
            entries.append(content)
            
        # [웹 화면용] <br> 태그 사용
        else: 
            # 실제 줄바꿈(\n)을 HTML 줄바꿈(<br>)으로 변경
            academy_html = academy.replace('\n', '<br>')
            subject_html = subject_display.replace('\n', '<br>')
            course_type_html = course_type.replace('\n', '<br>')
            
            subj_str = f"{subject_html}<br>" if subject_html else ""
            
            # HTML 조립
            entries.append(
                f"<b>{academy_html}</b><br>{subj_str}<span style='font-size:0.9em; color:gray'>({course_type_html})</span>"
            )
    
    if 'is_excel' in x.attrs and x.attrs['is_excel']:
        return "\n\n".join(entries) # 수업 간 두 줄 띄기
    else:
        return "<br><br>".join(entries) # 웹 화면 간격


# --- 3. 엑셀 다운로드 함수 ---
@st.cache_data
def convert_df_to_excel(df, index=False): 
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=index, sheet_name='Sheet1') 
    return output.getvalue()


# --- 4. [핵심 수정] 엑셀 그리드 생성 함수 (줄바꿈 인식 및 높이 자동조절) ---
@st.cache_data
def generate_area_grid_excel_v2(filtered_data, mapping_df, hardcoded_area_order):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        
        time_slots = ['오전', '오후', '저녁']
        days = ['월', '화', '수', '목', '금', '토', '일']
        workbook = writer.book
        
        # 스타일 정의
        cell_format = workbook.add_format({
            'align': 'center', 'valign': 'vcenter', 
            'text_wrap': True,  # [중요] 줄바꿈 허용
            'border': 1, 'font_size': 10
        })
        header_format = workbook.add_format({
            'bold': True, 'align': 'center', 'valign': 'vcenter', 
            'fg_color': '#DDEBF7', 'border': 1
        })
        time_col_format = workbook.add_format({
            'bold': True, 'align': 'center', 'valign': 'vcenter', 
            'bg_color': '#F2F2F2', 'border': 1
        })
        title_format = workbook.add_format({'bold': True, 'font_size': 14})
        
        # 영역 순회
        areas_in_data = list(filtered_data['영역'].unique())
        area_list = [area for area in hardcoded_area_order if area in areas_in_data and area != '[영역 전체]']

        for area in area_list:
            start_row = 0 
            df_area = filtered_data[filtered_data['영역'] == area]
            
            # 과목 정렬
            subjects_in_mapping = list(mapping_df[mapping_df['영역'] == area]['선택과목'].unique())
            subject_order_map = {subject: i for i, subject in enumerate(subjects_in_mapping)}
            all_subjects_in_area = sorted(df_area['과목'].unique(), key=lambda s: (subject_order_map.get(s, 99), s))
            
            instructors_by_subject = df_area.groupby('과목')['강사'].unique().to_dict()
            instructors_in_area = []
            for subject in all_subjects_in_area:
                for instructor in sorted(instructors_by_subject.get(subject, [])):
                    if instructor not in instructors_in_area: instructors_in_area.append(instructor)

            if not instructors_in_area: continue 
            
            worksheet = writer.book.add_worksheet(area) 
            # 컬럼 너비: 시간대는 좁게, 요일은 넓게
            worksheet.set_column(0, 0, 10)
            worksheet.set_column(1, 7, 22) 

            for instructor in instructors_in_area:
                worksheet.write(start_row, 0, f"🗓️ {instructor} 강사 시간표", title_format)
                start_row += 1
                
                inst_data = df_area[df_area['강사'] == instructor]
                inst_data.attrs['is_excel'] = True
                
                timetable_agg = inst_data.groupby(['시간대', '요일']).apply(format_cell_helper).reset_index(name='수업정보')
                timetable_pivot = timetable_agg.pivot(index='시간대', columns='요일', values='수업정보')
                timetable_pivot.columns.name = None
                
                display_df = timetable_pivot.reindex(index=time_slots, columns=days, fill_value="") 
                display_df = display_df.reset_index().rename(columns={'index': '시간대'})
                
                # 헤더
                for c_idx, col_name in enumerate(display_df.columns):
                    worksheet.write(start_row, c_idx, col_name, header_format)
                
                # 데이터 & 높이 조절
                for r_idx in range(len(display_df)):
                    worksheet.write(start_row + 1 + r_idx, 0, display_df.iloc[r_idx, 0], time_col_format)
                    
                    max_newlines = 0
                    for c_idx in range(1, len(display_df.columns)):
                        raw_val = display_df.iloc[r_idx, c_idx]
                        if pd.isna(raw_val) or raw_val == "":
                            val = ""
                        else:
                            # 여기서 다시 한 번 줄바꿈 문자 확인
                            val = str(raw_val).replace('\\n', '\n')
                            max_newlines = max(max_newlines, val.count('\n'))
                        
                        worksheet.write_string(start_row + 1 + r_idx, c_idx, val, cell_format)
                    
                    # [핵심] 줄바꿈 개수에 비례하여 행 높이 설정 (기본 60 + 줄바꿈당 15)
                    row_height = 60 + (max_newlines * 16)
                    worksheet.set_row(start_row + 1 + r_idx, row_height)

                start_row += len(display_df) + 3
        
    return output.getvalue()


# --- 5. 데이터 로드 (캐시 관리자 갱신용) ---
@st.cache_data
def load_data_from_gs():
    master_df = pd.DataFrame(ws_master.get_all_records())
    address_df = pd.DataFrame(ws_address.get_all_records())
    mapping_df = pd.DataFrame(ws_mapping.get_all_records())
    
    if not master_df.empty:
        key_cols = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분']
        exist_keys = [c for c in key_cols if c in master_df.columns]
        master_df = master_df.drop_duplicates(subset=exist_keys, keep='first')
        # [수정] 여기서 미리 replace 하지 않고 원본 그대로 가져가서 헬퍼 함수에서 처리함
    
    if not master_df.empty:
        if not address_df.empty and '강사명' in address_df.columns:
            if '강사명' not in master_df.columns: master_df['강사명'] = master_df['강사']
            if '자택 주소' not in address_df.columns: address_df['자택 주소'] = '정보 없음'
            master_df = pd.merge(master_df, address_df[['강사명', '자택 주소']], on='강사명', how='left')
            master_df['자택 주소'] = master_df['자택 주소'].fillna('정보 없음')
        else:
            master_df['자택 주소'] = '정보 없음'
            
    if not master_df.empty and not mapping_df.empty:
        if '선택과목' in mapping_df.columns:
            master_df = pd.merge(master_df, mapping_df[['선택과목', '영역']], left_on='과목', right_on='선택과목', how='left')
            master_df['영역'] = master_df['영역'].fillna('한국사')
        else:
            master_df['영역'] = '기타'
    else:
        master_df['영역'] = '기타'
    
    if '개강일' in master_df.columns:
        master_df['개강일_dt'] = pd.to_datetime(master_df['개강일'], errors='coerce')
        df_first = master_df.groupby('강사')['개강일_dt'].min().reset_index().rename(columns={'개강일_dt': '최초 개강일'})
        master_df = pd.merge(master_df, df_first, on='강사', how='left')
    else:
        master_df['최초 개강일'] = pd.NaT

    return master_df, mapping_df 


# --- 6. 파일 처리 함수 ---
def process_new_lecture_file(file):
    try:
        file_bytes = file.getvalue()
        file_ext = file.name.split('.')[-1].lower()
        engine = 'xlrd' if file_ext == 'xls' else 'openpyxl'
        df = pd.read_excel(io.BytesIO(file_bytes), header=1, engine=engine)
    except:
        try:
            df_html = pd.read_html(io.BytesIO(file_bytes), header=1)[0]
            df = df_html[pd.to_numeric(df_html['No'], errors='coerce').notna()]
        except:
            return pd.DataFrame()

    df = df[df['판매'] != '폐강']
    df = df[~df['강좌구분'].astype(str).str.contains('코어')]
    df['개강일'] = pd.to_datetime(df['개강일'], errors='coerce')
    df['연도'] = df['개강일'].dt.year.fillna(0).astype(int).astype(str)
    
    df['월'] = df['과정'].astype(str).str.extract(r'(\d+월)')
    df.loc[df['월'].isnull(), '월'] = df['개강일'].dt.month.fillna(0).astype(int).astype(str) + '월'
    df['월'] = df['월'].replace('0월', pd.NA)
    
    df['학원'] = df['학원'].astype(str).str.replace('러셀', '').str.replace('CORE', '').str.strip()
    # [수정] 파일 처리 시점에서는 줄바꿈 문자 건드리지 않음 (헬퍼에서 통일)
    
    df_exploded = df.assign(수업시간_분리=df['수업시간'].astype(str).str.split('\n')).explode('수업시간_분리')
    df_exploded['요일'] = df_exploded['수업시간_분리'].str.extract(r'([월화수목금토일])')
    df_exploded['시작시간'] = df_exploded['수업시간_분리'].str.extract(r'(\d{2}:\d{2})')
    
    def map_time(t):
        if pd.isna(t): return pd.NA
        try: h = int(t.split(':')[0])
        except: return pd.NA
        return '오전' if h < 12 else '오후' if h < 18 else '저녁'
        
    df_exploded['시간대'] = df_exploded['시작시간'].apply(map_time)
    cols = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분', '개강일']
    return df_exploded[cols].copy().dropna(subset=['연도', '월', '강사', '요일', '시간대']).drop_duplicates()


# --- 7. 관리자 모드 ---
st.sidebar.header("👨‍💼 관리자 모드")
pw = st.sidebar.text_input("비밀번호", type="password")

if pw == admin_password:
    st.sidebar.success("인증 성공")
    files = st.sidebar.file_uploader("강좌 파일", type=["xls", "xlsx"], accept_multiple_files=True)
    addr_file = st.sidebar.file_uploader("주소록 파일", type=["xls", "xlsx"])
    
    if st.sidebar.button("DB 갱신"):
        with st.spinner("갱신 중..."):
            try:
                exist_df = pd.DataFrame(ws_master.get_all_records())
                new_dfs = [process_new_lecture_file(f) for f in files]
                if not new_dfs: st.stop()
                
                new_master = pd.concat([exist_df] + new_dfs, ignore_index=True)
                new_master['개강일'] = new_master['개강일'].astype(str)
                
                keys = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분']
                new_master = new_master.drop_duplicates(subset=[k for k in keys if k in new_master.columns], keep='first')
                
                ws_master.clear()
                ws_master.update([new_master.columns.values.tolist()] + new_master.astype(str).values.tolist())
                
                if addr_file:
                    a_df = pd.read_excel(addr_file)
                    ws_address.clear()
                    ws_address.update([a_df.columns.values.tolist()] + a_df.astype(str).values.tolist())
                
                st.cache_data.clear()
                st.success("완료! 새로고침됩니다.")
                st.rerun()
            except Exception as e:
                st.error(f"오류: {e}")


# --- 8. 메인 로직 ---
try:
    master_data, mapping_df = load_data_from_gs()
except:
    st.stop()

if master_data.empty: st.stop()

st.title("강사별 출강 현황 통합 시간표 📊")

if 'selected_instructor' not in st.session_state: st.session_state.selected_instructor = None
if 'main_view' not in st.session_state: st.session_state.main_view = "전체 출강 현황"

years = sorted(master_data['연도'].astype(str).unique(), reverse=True)
y_idx = 0
if st.session_state.get('y_sel') in years: y_idx = years.index(st.session_state.y_sel)
c1, c2, _ = st.columns([1, 1, 4])
sel_y = c1.selectbox("연도", years, index=y_idx, key="y_sel")

months = sorted(master_data[master_data['연도'].astype(str) == sel_y]['월'].astype(str).unique())
m_idx = 0
if st.session_state.get('m_sel') in months: m_idx = months.index(st.session_state.m_sel)
sel_m = c2.selectbox("월", months, index=m_idx, key="m_sel")

data = master_data[(master_data['연도'].astype(str) == sel_y) & (master_data['월'].astype(str) == sel_m)]
st.divider()

view = st.radio("보기", ["전체 출강 현황", "강사별 시간표"], horizontal=True, label_visibility="collapsed", key="main_view")
st.divider()

hardcoded_areas = ['[영역 전체]', '국어', '수학', '영어', '사회탐구', '과학탐구', '논술&제2외국어', '한국사']

if view == "전체 출강 현황":
    st.header(f"📊 {sel_y}년 {sel_m} 전체 현황")
    if data.empty: st.warning("데이터 없음")
    else:
        piv = data.pivot_table(index=['영역', '과목', '강사'], columns='학원', values='요일', aggfunc='count', fill_value=0)
        piv = piv.applymap(lambda x: "■" if x > 0 else "").fillna('')
        
        a_map = {a: i for i, a in enumerate(hardcoded_areas)}
        s_map = {s: i for i, s in enumerate(mapping_df['선택과목'])}
        idx = piv.index.to_frame(index=False)
        idx['a_ord'] = idx['영역'].map(a_map).fillna(99)
        idx['s_ord'] = idx['과목'].map(s_map).fillna(99)
        
        st.dataframe(piv.iloc[idx.sort_values(['a_ord', 's_ord', '강사']).index], use_container_width=True)
        st.download_button("엑셀 다운로드", convert_df_to_excel(piv, True), f"{sel_y}_{sel_m}_전체현황.xlsx")

else:
    lc, rc = st.columns([1, 3])
    with lc:
        a_list = sorted(list(set(list(mapping_df['영역'].unique()) + list(master_data['영역'].unique()))))
        final_areas = [a for a in hardcoded_areas if a in a_list] + [a for a in a_list if a not in hardcoded_areas and a != '[영역 전체]']
        
        s_area = st.selectbox("영역", final_areas)
        d_area = data if s_area == '[영역 전체]' else data[data['영역'] == s_area]
        
        s_subjs = []
        if s_area in ['[영역 전체]', '국어', '수학', '영어', '한국사']: s_subjs = ['전체']
        else: s_subjs = ['전체'] + sorted(d_area['과목'].unique())
        
        sel_subj = st.selectbox("과목", s_subjs, disabled=(s_area in ['[영역 전체]', '국어', '수학', '영어', '한국사']))
        d_final = d_area if sel_subj == '전체' else d_area[d_area['과목'] == sel_subj]
        
        query = st.text_input("강사 검색")
        if query: d_final = d_final[d_final['강사'].str.contains(query, case=False)]
        
        st.divider()
        instructors = sorted(d_final['강사'].unique())
        if instructors:
            st.markdown(f"**강사 선택** ({len(instructors)}명)")
            sel_inst = st.radio("강사", instructors, label_visibility="collapsed")
            st.session_state.selected_instructor = sel_inst
        else:
            st.warning("검색 결과 없음")
            st.session_state.selected_instructor = None

    with rc:
        if st.session_state.selected_instructor:
            inst = st.session_state.selected_instructor
            st.header(f"🗓️ {inst} 강사 시간표")
            
            inst_d = data[data['강사'] == inst]
            inst_d.attrs['is_excel'] = False
            
            agg = inst_d.groupby(['시간대', '요일']).apply(format_cell_helper).reset_index(name='info')
            piv = agg.pivot(index='시간대', columns='요일', values='info')
            
            # 시간표 프레임 완성
            frame = pd.DataFrame(index=['오전', '오후', '저녁'], columns=['월', '화', '수', '목', '금', '토', '일']).fillna("")
            piv = piv.reindex(index=frame.index, columns=frame.columns, fill_value="")
            
            # 인덱스를 컬럼으로 변환하여 HTML 생성
            display_df = piv.reset_index().rename(columns={'index': '시간대'})
            
            st.markdown(display_df.to_html(escape=False, index=False, classes="timetable-grid"), unsafe_allow_html=True)
            
            row = master_data[master_data['강사'] == inst].iloc[0]
            st.write(f"- 주소: {row.get('자택 주소', '-')}")
            st.write(f"- 최초 개강: {row.get('최초 개강일', '-')}")
            
            st.divider()
            st.download_button(
                "통합 그리드 엑셀 다운로드", 
                generate_area_grid_excel_v2(data, mapping_df, hardcoded_areas), 
                f"{sel_y}_{sel_m}_통합그리드.xlsx"
            )
