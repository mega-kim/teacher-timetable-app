import pandas as pd
import streamlit as st
import io
import gspread
from google.oauth2.service_account import Credentials
import re  # 정규식 모듈

# --- 0. Streamlit 앱 기본 설정 ---
st.set_page_config(layout="wide", page_title="강사별 통합 시간표")

# 버전 확인용 (업데이트 반영 여부 확인)
st.caption("🚀 [System] 버전: 5.0 (NaN 제거 강화 + 드롭박스 위치 수정 + 엑셀 정렬 수정)")

# --- CSS 스타일 주입 ---
CUSTOM_CSS = """
<style>
    body, .stApp, .stWidget { font-size: 14px; }
    .st-bu, .st-ag, .st-at, .st-bq, .st-ar, .st-as, label, .st-emotion-cache-1y4p8pa { font-size: 14px !important; }
    h1 { font-size: 2.0rem; }
    h2 { font-size: 1.75rem; }
    
    /* 라디오 버튼 그룹 중앙 정렬 */
    div[role="radiogroup"] { justify-content: center; }
    
    /* 시간표 그리드 스타일 */
    table.timetable-grid {
        table-layout: fixed;
        width: 100%;
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
        line-height: 1.6;
        white-space: normal;
    }
    table.timetable-grid tr th:first-child, table.timetable-grid tr td:first-child {
        width: 8%;
        font-weight: bold;
        background-color: #f8f9fa;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# --- 1. Google Sheets 인증 ---
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
    try: ws_mapping = sh.worksheet('subject_mapping')
    except: st.error("매핑 시트 없음"); st.stop()
except Exception as e:
    st.error(f"연결 오류: {e}"); st.stop()

# --- 2. [수정] 텍스트 정제 함수 (NaN 제거 강화) ---
def clean_text_regex(text):
    """NaN, None, 줄바꿈 등을 완벽하게 처리하는 함수"""
    # 1. 비어있는 값 체크
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text)
    
    # 2. 'nan'이라는 글자가 들어오면 빈칸 처리 (대소문자 무관)
    if text.strip().lower() == 'nan':
        return ""

    # 3. 정규식: 백슬래시(\)가 1개 이상 있고 뒤에 n이 오는 모든 패턴을 실제 엔터키로
    text = re.sub(r'\\+n', '\n', text)
    
    return text.strip()

def format_cell_helper(x):
    entries = []
    for _, row in x.iterrows():
        # 정규식 클리닝 적용
        academy = clean_text_regex(row['학원'])
        subject = clean_text_regex(row['과목'])
        course_type = clean_text_regex(row['강좌구분'])
        
        # 데이터가 없으면 건너뜀 (NaN 방지)
        if not academy and not subject:
            continue

        subj_disp = ""
        if row['영역'] not in ['국어', '수학', '영어', '한국사']:
            subj_disp = subject
        
        # [엑셀용]
        if 'is_excel' in x.attrs and x.attrs['is_excel']:
            content = f"{academy}\n"
            if subj_disp: content += f"{subj_disp}\n"
            # [수정] 강좌구분이 있을 때만 괄호 추가
            if course_type: content += f"({course_type})"
            entries.append(content)
            
        # [웹 화면용]
        else:
            academy_html = academy.replace('\n', '<br>')
            subject_html = subj_disp.replace('\n', '<br>')
            course_type_html = course_type.replace('\n', '<br>')
            
            subj_str = f"{subject_html}<br>" if subject_html else ""
            # [수정] 강좌구분이 있을 때만 괄호 추가
            course_str = f"<span style='font-size:0.9em; color:gray'>({course_type_html})</span>" if course_type_html else ""
            entries.append(f"<b>{academy_html}</b><br>{subj_str}{course_str}")
    
    join_char = "\n\n" if ('is_excel' in x.attrs and x.attrs['is_excel']) else "<br><br>"
    return join_char.join(entries)

# --- 3. 엑셀 다운로드 ---
@st.cache_data
def convert_df_to_excel(df, index=False): 
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=index, sheet_name='Sheet1') 
    return output.getvalue()

# --- 4. 통합 그리드 엑셀 ---
@st.cache_data
def generate_area_grid_excel_v2(filtered_data, mapping_df, hardcoded_area_order):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        cell_fmt = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'border': 1, 'font_size': 10})
        head_fmt = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'fg_color': '#DDEBF7', 'border': 1})
        time_fmt = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#F2F2F2', 'border': 1})
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14})
        
        areas_in_data = list(filtered_data['영역'].unique())
        area_list = [area for area in hardcoded_area_order if area in areas_in_data and area != '[영역 전체]']

        for area in area_list:
            start_row = 0 
            df_area = filtered_data[filtered_data['영역'] == area]
            subjects_in_mapping = list(mapping_df[mapping_df['영역'] == area]['선택과목'].unique())
            subject_order_map = {subject: i for i, subject in enumerate(subjects_in_mapping)}
            all_subjects_in_area = sorted(df_area['과목'].unique(), key=lambda s: (subject_order_map.get(s, 99), s))
            
            inst_by_subj = df_area.groupby('과목')['강사'].unique().to_dict()
            inst_in_area = []
            for s in all_subjects_in_area:
                for i in sorted(inst_by_subj.get(s, [])):
                    if i not in inst_in_area: inst_in_area.append(i)
            if not inst_in_area: continue 
            
            ws = writer.book.add_worksheet(area)
            ws.set_column(0, 0, 10)
            ws.set_column(1, 7, 22) 

            for instructor in inst_in_area:
                ws.write(start_row, 0, f"🗓️ {instructor} 강사 시간표", title_fmt)
                start_row += 1
                
                inst_data = df_area[df_area['강사'] == instructor]
                inst_data.attrs['is_excel'] = True
                
                tt_agg = inst_data.groupby(['시간대', '요일']).apply(format_cell_helper).reset_index(name='info')
                tt_piv = tt_agg.pivot(index='시간대', columns='요일', values='info')
                
                disp = tt_piv.reindex(index=['오전', '오후', '저녁'], columns=['월', '화', '수', '목', '금', '토', '일'], fill_value="")
                disp = disp.reset_index().rename(columns={'index': '시간대'})
                
                for c, name in enumerate(disp.columns): ws.write(start_row, c, name, head_fmt)
                
                for r in range(len(disp)):
                    ws.write(start_row + 1 + r, 0, disp.iloc[r, 0], time_fmt)
                    max_nl = 0
                    for c in range(1, len(disp.columns)):
                        raw = disp.iloc[r, c]
                        # [중요] 여기서도 NaN 체크
                        val = clean_text_regex(raw) 
                        max_nl = max(max_nl, val.count('\n'))
                        ws.write_string(start_row + 1 + r, c, val, cell_fmt)
                    ws.set_row(start_row + 1 + r, 60 + (max_nl * 16))
                start_row += len(disp) + 3
    return output.getvalue()

# --- 5. 데이터 로드 ---
@st.cache_data
def load_data_from_gs():
    m_df = pd.DataFrame(ws_master.get_all_records())
    a_df = pd.DataFrame(ws_address.get_all_records())
    map_df = pd.DataFrame(ws_mapping.get_all_records())
    
    if not m_df.empty:
        # 중복 제거
        keys = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분']
        m_df = m_df.drop_duplicates(subset=[k for k in keys if k in m_df.columns], keep='first')
        
    if not m_df.empty:
        if not a_df.empty and '강사명' in a_df.columns:
            if '강사명' not in m_df.columns: m_df['강사명'] = m_df['강사']
            if '자택 주소' not in a_df.columns: a_df['자택 주소'] = '정보 없음'
            m_df = pd.merge(m_df, a_df[['강사명', '자택 주소']], on='강사명', how='left')
            m_df['자택 주소'] = m_df['자택 주소'].fillna('정보 없음')
        else: m_df['자택 주소'] = '정보 없음'
            
    if not m_df.empty and not map_df.empty:
        if '선택과목' in map_df.columns:
            m_df = pd.merge(m_df, map_df[['선택과목', '영역']], left_on='과목', right_on='선택과목', how='left')
            m_df['영역'] = m_df['영역'].fillna('한국사')
        else: m_df['영역'] = '기타'
    else: m_df['영역'] = '기타'
    
    if '개강일' in m_df.columns:
        m_df['개강일_dt'] = pd.to_datetime(m_df['개강일'], errors='coerce')
        first = m_df.groupby('강사')['개강일_dt'].min().reset_index().rename(columns={'개강일_dt': '최초 개강일'})
        m_df = pd.merge(m_df, first, on='강사', how='left')
    else: m_df['최초 개강일'] = pd.NaT
    return m_df, map_df 

# --- 6. 파일 처리 ---
def process_new_lecture_file(file):
    try:
        fb = file.getvalue()
        eng = 'xlrd' if file.name.endswith('xls') else 'openpyxl'
        df = pd.read_excel(io.BytesIO(fb), header=1, engine=eng)
    except:
        try: df = pd.read_html(io.BytesIO(fb), header=1)[0]
        except: return pd.DataFrame()

    df = df[df['판매'] != '폐강']
    df = df[~df['강좌구분'].astype(str).str.contains('코어')]
    df['개강일'] = pd.to_datetime(df['개강일'], errors='coerce')
    df['연도'] = df['개강일'].dt.year.fillna(0).astype(int).astype(str)
    df['월'] = df['과정'].astype(str).str.extract(r'(\d+월)')
    df.loc[df['월'].isnull(), '월'] = df['개강일'].dt.month.fillna(0).astype(int).astype(str) + '월'
    df['월'] = df['월'].replace('0월', pd.NA)
    
    df['학원'] = df['학원'].astype(str).str.replace('러셀', '').str.replace('CORE', '').str.strip()
    
    df_exp = df.assign(t_split=df['수업시간'].astype(str).str.split('\n')).explode('t_split')
    df_exp['요일'] = df_exp['t_split'].str.extract(r'([월화수목금토일])')
    df_exp['시작'] = df_exp['t_split'].str.extract(r'(\d{2}:\d{2})')
    
    def map_t(t):
        if pd.isna(t): return pd.NA
        try: h = int(t.split(':')[0])
        except: return pd.NA
        return '오전' if h < 12 else '오후' if h < 18 else '저녁'
    df_exp['시간대'] = df_exp['시작'].apply(map_t)
    
    cols = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분', '개강일']
    return df_exp[cols].copy().dropna(subset=['연도', '월', '강사', '요일', '시간대']).drop_duplicates()

# --- 7. 관리자 ---
st.sidebar.header("👨‍💼 관리자 모드")
if st.sidebar.text_input("비밀번호", type="password") == admin_password:
    st.sidebar.success("인증됨")
    ups = st.sidebar.file_uploader("강좌파일", accept_multiple_files=True)
    aup = st.sidebar.file_uploader("주소록")
    if st.sidebar.button("DB 갱신"):
        with st.spinner("처리중..."):
            exist = pd.DataFrame(ws_master.get_all_records())
            news = [process_new_lecture_file(f) for f in ups]
            if news:
                nm = pd.concat([exist] + news, ignore_index=True)
                nm['개강일'] = nm['개강일'].astype(str)
                keys = ['연도', '월', '강사', '과목', '요일', '시간대', '학원', '강좌구분']
                nm = nm.drop_duplicates(subset=[k for k in keys if k in nm.columns], keep='first')
                ws_master.clear(); ws_master.update([nm.columns.values.tolist()] + nm.astype(str).values.tolist())
            if aup:
                ad = pd.read_excel(aup); ws_address.clear(); ws_address.update([ad.columns.values.tolist()] + ad.astype(str).values.tolist())
            st.cache_data.clear(); st.success("완료"); st.rerun()

# --- 8. 메인 뷰 ---
try: m_df, map_df = load_data_from_gs()
except: st.stop()
if m_df.empty: st.stop()

st.title("강사별 통합 시간표 📊")

if 'selected_instructor' not in st.session_state: st.session_state.selected_instructor = None
if 'main_view' not in st.session_state: st.session_state.main_view = "전체 출강 현황"

ys = sorted(m_df['연도'].astype(str).unique(), reverse=True)
# [수정] 컬럼 한 번만 선언하여 나란히 배치
cols = st.columns([1,1,4])
y_sel = cols[0].selectbox("연도", ys, index=0, key="y_sel")
ms = sorted(m_df[m_df['연도'].astype(str)==y_sel]['월'].astype(str).unique())
m_sel = cols[1].selectbox("월", ms, index=0, key="m_sel")
data = m_df[(m_df['연도'].astype(str)==y_sel) & (m_df['월'].astype(str)==m_sel)]
st.divider()

view = st.radio("보기", ["전체 출강 현황", "강사별 시간표"], horizontal=True, label_visibility="collapsed", key="main_view")
st.divider()

hard_areas = ['[영역 전체]', '국어', '수학', '영어', '사회탐구', '과학탐구', '논술&제2외국어', '한국사']

if view == "전체 출강 현황":
    st.header(f"📊 {y_sel}년 {m_sel} 전체 현황")
    if data.empty: st.warning("데이터 없음")
    else:
        piv = data.pivot_table(index=['영역', '과목', '강사'], columns='학원', values='요일', aggfunc='count', fill_value=0)
        piv = piv.applymap(lambda x: "■" if x>0 else "").fillna('')
        a_map = {a:i for i,a in enumerate(hard_areas)}; s_map = {s:i for i,s in enumerate(map_df['선택과목'])}
        idx = piv.index.to_frame(index=False)
        idx['a'] = idx['영역'].map(a_map).fillna(99); idx['s'] = idx['과목'].map(s_map).fillna(99)
        # [수정] 정렬된 데이터프레임을 변수에 저장
        sorted_piv = piv.iloc[idx.sort_values(['a','s','강사']).index]
        st.dataframe(sorted_piv, use_container_width=True)
        # [수정] 엑셀 다운로드 시 정렬된 데이터프레임 사용
        st.download_button("엑셀 다운로드", convert_df_to_excel(sorted_piv, True), f"전체현황.xlsx")
else:
    lc, rc = st.columns([1,3])
    with lc:
        als = sorted(list(set(list(map_df['영역'].unique())+list(m_df['영역'].unique()))))
        final_as = [a for a in hard_areas if a in als] + [a for a in als if a not in hard_areas and a!='[영역 전체]']
        s_area = st.selectbox("영역", final_as)
        d_area = data if s_area=='[영역 전체]' else data[data['영역']==s_area]
        s_subjs = ['전체'] if s_area in ['[영역 전체]','국어','수학','영어','한국사'] else ['전체']+sorted(d_area['과목'].unique())
        sel_subj = st.selectbox("과목", s_subjs, disabled=(len(s_subjs)==1))
        d_fin = d_area if sel_subj=='전체' else d_area[d_area['과목']==sel_subj]
        q = st.text_input("검색"); 
        if q: d_fin = d_fin[d_fin['강사'].str.contains(q, case=False)]
        insts = sorted(d_fin['강사'].unique())
        
        if insts:
            # 스크롤바 영역 유지
            st.markdown(f"**강사 선택** ({len(insts)}명)")
            with st.container(height=400):
                st.session_state.selected_instructor = st.radio("강사", insts, label_visibility="collapsed")
        else:
            st.warning("결과 없음")

    with rc:
        if st.session_state.selected_instructor:
            inst = st.session_state.selected_instructor
            st.header(f"🗓️ {inst} 강사 시간표")
            inst_d = data[data['강사']==inst]; inst_d.attrs['is_excel'] = False
            agg = inst_d.groupby(['시간대', '요일']).apply(format_cell_helper).reset_index(name='info')
            piv = agg.pivot(index='시간대', columns='요일', values='info')
            disp = piv.reindex(index=['오전','오후','저녁'], columns=['월','화','수','목','금','토','일'], fill_value="").reset_index().rename(columns={'index':'시간대'})
            st.markdown(disp.to_html(escape=False, index=False, classes="timetable-grid"), unsafe_allow_html=True)
            
            st.divider()
            st.download_button("통합 그리드 다운로드", generate_area_grid_excel_v2(data, map_df, hard_areas), f"통합그리드.xlsx")
