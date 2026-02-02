import os
import psycopg2
from psycopg2.extras import DictCursor
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from dotenv import load_dotenv
from datetime import datetime
import json
import math # AMWS 계산용 추가
import folium
from folium import IFrame

# 로컬 환경에서는 .env를 읽고, Azure에서는 패스.
if os.path.exists('.env'):
    load_dotenv()
app = Flask(__name__)
app.secret_key = os.urandom(24)

# ==========================================
# 1. 공통 데이터베이스 연결 함수
# ==========================================
def get_db_connection():
    try:
        conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        sslmode='require', # Azure 연결을 위해 필수
        options='-c timezone=Asia/Seoul'  # 한국 시간대로 설정
        )
        # print('✓ 데이터베이스 연결 성공!') # 로그 너무 많으면 주석 처리
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        print('✗ 데이터베이스 연결 실패!')
        print(f'에러 상세: {e}')
        raise

# ==========================================
# 2. 기존 게시판 (Board) 라우트
# ==========================================
@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("SELECT id, title, author, created_at, view_count, like_count FROM board.posts ORDER BY created_at DESC")
    posts = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('index.html', posts=posts)

@app.route('/create/', methods=['GET'])
def create_form():
    return render_template('create.html')

@app.route('/create/', methods=['POST'])
def create_post():
    title = request.form.get('title')
    author = request.form.get('author')
    content = request.form.get('content')

    if not title or not author or not content:
        flash('모든 필드를 똑바로 채워주세요!!!!')
        return redirect(url_for('create_form'))
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute("INSERT INTO board.posts (title, content, author) VALUES (%s, %s, %s) RETURNING id", (title,author,content ))
    post_id = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    flash('게시글이 성공적으로 등록되었음')
    return redirect(url_for('view_post', post_id=post_id))

@app.route('/post/<int:post_id>')
def view_post(post_id):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    
    cursor.execute('UPDATE board.posts SET view_count = view_count + 1 WHERE id = %s', (post_id,))
    cursor.execute('SELECT * FROM board.posts WHERE id = %s', (post_id,))
    post = cursor.fetchone()
    
    if post is None:
        cursor.close()
        conn.close()
        flash('게시글을 찾을 수 없습니다.')
        return redirect(url_for('index'))
    
    cursor.execute('SELECT * FROM board.comments WHERE post_id = %s ORDER BY created_at', (post_id,))
    comments = cursor.fetchall()
    cursor.close()
    conn.close()
    
    user_ip = request.remote_addr
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM board.likes WHERE post_id = %s AND user_ip = %s', (post_id, user_ip))
    liked = cursor.fetchone()[0] > 0
    cursor.close()
    conn.close()
    
    return render_template('view.html', post=post, comments=comments, liked=liked)

@app.route('/edit/<int:post_id>', methods=['GET'])
def edit_form(post_id):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    cursor.execute('SELECT * FROM board.posts WHERE id = %s', (post_id,))
    post = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if post is None:
        flash('게시글을 찾을 수 없습니다.')
        return redirect(url_for('index'))
    
    return render_template('edit.html', post=post)

@app.route('/edit/<int:post_id>', methods=['POST'])
def edit_post(post_id):
    title = request.form.get('title')
    content = request.form.get('content')
    
    if not title or not content:
        flash('제목과 내용을 모두 입력해주세요.')
        return redirect(url_for('edit_form', post_id=post_id))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE board.posts SET title = %s, content = %s, updated_at = %s WHERE id = %s',
        (title, content, datetime.now(), post_id)
    )
    cursor.close()
    conn.close()
    
    flash('게시글이 성공적으로 수정되었습니다.')
    return redirect(url_for('view_post', post_id=post_id))

@app.route('/delete/<int:post_id>', methods=['POST'])
def delete_post(post_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM board.posts WHERE id = %s', (post_id,))
    cursor.close()
    conn.close()
    
    flash('게시글이 성공적으로 삭제되었습니다.')
    return redirect(url_for('index'))

@app.route('/post/comment/<int:post_id>', methods=['POST'])
def add_comment(post_id):
    author = request.form.get('author')
    content = request.form.get('content')
    
    if not author or not content:
        flash('작성자와 내용을 모두 입력해주세요.')
        return redirect(url_for('view_post', post_id=post_id))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO board.comments (post_id, author, content) VALUES (%s, %s, %s)',
        (post_id, author, content)
    )
    cursor.close()
    conn.close()
    
    flash('댓글이 등록되었습니다.')
    return redirect(url_for('view_post', post_id=post_id))

@app.route('/post/like/<int:post_id>', methods=['POST'])
def like_post(post_id):
    user_ip = request.remote_addr
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM board.likes WHERE post_id = %s AND user_ip = %s', (post_id, user_ip))
    already_liked = cursor.fetchone()[0] > 0
    
    if already_liked:
        cursor.execute('DELETE FROM board.likes WHERE post_id = %s AND user_ip = %s', (post_id, user_ip))
        cursor.execute('UPDATE board.posts SET like_count = like_count - 1 WHERE id = %s', (post_id,))
        message = '좋아요가 취소되었습니다.'
    else:
        cursor.execute('INSERT INTO board.likes (post_id, user_ip) VALUES (%s, %s)', (post_id, user_ip))
        cursor.execute('UPDATE board.posts SET like_count = like_count + 1 WHERE id = %s', (post_id,))
        message = '좋아요가 등록되었습니다.'
    
    cursor.close()
    conn.close()   
    flash(message)
    return redirect(url_for('view_post', post_id=post_id))


# ==========================================
# 3. FMS (Farm Management System) 라우트
# ==========================================
@app.route('/fms/')
def fms_result():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    
    cursor.execute('SELECT * FROM fms.total_result ORDER BY 육계번호')
    results = cursor.fetchall()
    
    cursor.execute('SELECT COUNT(*) as total FROM fms.total_result')
    total_count = cursor.fetchone()['total']
    
    cursor.execute("SELECT 부적합여부, COUNT(*) as count FROM fms.total_result GROUP BY 부적합여부")
    quality_stats = cursor.fetchall()
    quality_dict = {row['부적합여부']: row['count'] for row in quality_stats}
    
    cursor.execute("""
        SELECT 품종, COUNT(*) as total_count, SUM(CASE WHEN 부적합여부 = 'Pass' THEN 1 ELSE 0 END) as pass_count 
        FROM fms.total_result GROUP BY 품종 ORDER BY total_count DESC
    """)
    breed_stats = cursor.fetchall()
    
    cursor.execute("SELECT 고객사, COUNT(*) as count FROM fms.total_result GROUP BY 고객사 ORDER BY count DESC")
    customer_stats = cursor.fetchall()
    
    cursor.execute("SELECT gender, COUNT(*) as count FROM fms.chick_info GROUP BY gender ORDER BY gender")
    gender_stats = cursor.fetchall()
    
    cursor.execute("SELECT farm, COUNT(*) as count FROM fms.chick_info GROUP BY farm ORDER BY farm")
    farm_stats = cursor.fetchall()
    
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN vaccination1 = 1 THEN 1 ELSE 0 END) as vac1_done,
            SUM(CASE WHEN vaccination1 = 0 THEN 1 ELSE 0 END) as vac1_not,
            SUM(CASE WHEN vaccination2 = 1 THEN 1 ELSE 0 END) as vac2_done,
            SUM(CASE WHEN vaccination2 = 0 THEN 1 ELSE 0 END) as vac2_not
        FROM fms.chick_info
    """)
    vaccination_stats = cursor.fetchone()
    
    cursor.execute("""
        SELECT m.code_desc as breed_name, COUNT(c.chick_no) as count
        FROM fms.chick_info c
        JOIN fms.master_code m ON c.breeds = m.code AND m.column_nm = 'breeds'
        GROUP BY m.code_desc ORDER BY count DESC
    """)
    breed_distribution = cursor.fetchall()
    
    pass_count = quality_dict.get('Pass', 0)
    fail_count = quality_dict.get('Fail', 0)
    pass_rate = (pass_count / total_count * 100) if total_count > 0 else 0
    
    cursor.close()
    conn.close()
    
    stats = {
        'total_count': total_count,
        'pass_count': pass_count,
        'fail_count': fail_count,
        'pass_rate': round(pass_rate, 2),
        'breed_stats': breed_stats,
        'customer_stats': customer_stats,
        'gender_stats': gender_stats,
        'farm_stats': farm_stats,
        'vaccination_stats': vaccination_stats,
        'breed_distribution': breed_distribution
    }
    
    return render_template('fms_result.html', results=results, stats=stats)

@app.route('/fms/test')
def fms_test():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    try:
        cursor.execute('SELECT COUNT(*) as total FROM fms.total_result')
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return f"Total records: {result['total']}"
    except Exception as e:
        return f"Error: {str(e)}"

@app.route('/fms/check-data')
def fms_check_data():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    try:
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'fms' ORDER BY table_name")
        tables = cursor.fetchall()
        
        result = "<h1>FMS Schema 분석</h1>"
        result += "<h2>📊 Available Tables:</h2><ul>"
        for table in tables:
            result += f"<li><b>{table['table_name']}</b></li>"
        result += "</ul>"
        
        cursor.execute("SELECT * FROM fms.total_result LIMIT 1")
        sample = cursor.fetchone()
        
        result += "<h2>📋 total_result 컬럼:</h2><ul>"
        if sample:
            for col in sample.keys():
                result += f"<li><b>{col}</b>: {sample[col]}</li>"
        result += "</ul>"
        
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        return f"<h2>❌ Error:</h2><p>{str(e)}</p>"

@app.route('/fms/check-columns')
def fms_check_columns():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    result = "<h1>테이블별 컬럼 확인</h1>"
    tables = ['chick_info', 'prod_result', 'health_cond', 'env_cond', 'master_code']
    for table in tables:
        try:
            cursor.execute(f"SELECT * FROM fms.{table} LIMIT 1")
            sample = cursor.fetchone()
            result += f"<h2>📋 {table}</h2><ul>"
            if sample:
                for col in sample.keys():
                    result += f"<li><b>{col}</b>: {sample[col]}</li>"
            else:
                result += "<li>데이터 없음</li>"
            result += "</ul><hr>"
        except Exception as e:
            result += f"<h2>❌ {table}</h2><p>Error: {str(e)}</p><hr>"
    cursor.close()
    conn.close()
    return result

@app.route('/test-chart')
def test_chart():
    return '''
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Chart.js 테스트</title></head>
<body>
    <h1>Chart.js 기본 테스트</h1>
    <div style="width: 500px; height: 400px;"><canvas id="testChart"></canvas></div>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        const ctx = document.getElementById('testChart');
        new Chart(ctx, {type: 'bar', data: {labels: ['A','B','C'], datasets: [{label: 'Test', data: [12,19,3], backgroundColor: ['red','blue','green']}]}});
    </script>
</body>
</html>
    '''

# ==========================================
# 4. AMWS (Air Mission Weather System) 라우트
# ==========================================
def calculate_crosswind(wind_spd, wind_dir, runway_heading):
    """
    측풍 계산 로직 (AMWS 전용 헬퍼 함수)
    """
    if wind_spd is None or wind_dir is None:
        return 0
    # 각도를 라디안으로 변환
    diff = math.radians(wind_dir - runway_heading)
    crosswind = abs(wind_spd * math.sin(diff))
    return round(crosswind, 1)

@app.route('/amws/')
def amws():
    """AMWS 메인 대시보드 (지도 포함)"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=DictCursor)
    
    # 비행장 목록
    cur.execute("SELECT base_id, base_name FROM amws.airbases ORDER BY base_id")
    bases = cur.fetchall()
    
    # 항공자산 목록
    cur.execute("SELECT aircraft_id FROM amws.aircraft_assets ORDER BY aircraft_id")
    aircrafts = cur.fetchall()
    
    # 지도 생성 (Folium)
    m = folium.Map(
        location=[36.5, 127.5],
        zoom_start=7,
        tiles='OpenStreetMap'
    )
    
    # 각 비행장별로 마커 추가
    for base in bases:
        base_id = base['base_id']
        base_name = base['base_name']
        
        # 비행장 좌표 조회
        cur.execute("""
            SELECT lat, lon, runway_heading FROM amws.airbases WHERE base_id = %s
        """, (base_id,))
        base_info = cur.fetchone()
        
        if not base_info:
            continue
            
        lat = float(base_info['lat'])
        lon = float(base_info['lon'])
        
        # 최신 기상 데이터 및 작전 가능 항공자산 수 조회
        cur.execute("""
            WITH latest_weather AS (
                SELECT DISTINCT ON (base_id)
                    base_id,
                    obs_time,
                    wind_dir,
                    wind_spd_kts,
                    visibility_m,
                    ceiling_ft,
                    weather_desc
                FROM amws.weather_observations
                WHERE base_id = %s
                ORDER BY base_id, obs_time DESC
            )
            SELECT 
                w.obs_time,
                w.wind_dir,
                w.wind_spd_kts,
                w.weather_desc,
                COUNT(CASE 
                    WHEN ABS(w.wind_spd_kts * SIN(RADIANS(w.wind_dir - b.runway_heading))) <= a.max_crosswind_kts 
                    AND w.visibility_m >= a.min_visibility_m 
                    AND w.ceiling_ft >= a.min_ceiling_ft
                    AND (NOT a.precip_restricted OR (w.weather_desc NOT LIKE '%%RA%%' AND w.weather_desc NOT LIKE '%%SN%%'))
                    THEN 1 END) as go_count,
                COUNT(a.aircraft_id) as total_aircraft_count
            FROM amws.airbases b
            LEFT JOIN latest_weather w ON b.base_id = w.base_id
            CROSS JOIN amws.aircraft_assets a
            WHERE b.base_id = %s
            GROUP BY w.obs_time, w.wind_dir, w.wind_spd_kts, w.weather_desc
        """, (base_id, base_id))
        
        weather_data = cur.fetchone()
        
        if weather_data and weather_data['obs_time']:
            obs_time = weather_data['obs_time'].strftime('%H:%M')
            wind_spd = weather_data['wind_spd_kts'] or 0
            weather = weather_data['weather_desc'] or 'N/A'
            go_count = weather_data['go_count'] or 0
            total_count = weather_data['total_aircraft_count'] or 0
            
            # 마커 색상
            if total_count == 0:
                color = 'gray'
            elif go_count == total_count:
                color = 'green'
            elif go_count > 0:
                color = 'orange'
            else:
                color = 'red'
            
            # 팝업 HTML
            popup_html = f"""
            <div style="font-family: Arial; width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">{base_id} - {base_name}</h4>
                <table style="width: 100%; font-size: 12px;">
                    <tr><td><b>관측시간:</b></td><td>{obs_time}</td></tr>
                    <tr><td><b>풍속:</b></td><td>{wind_spd} kt</td></tr>
                    <tr><td><b>날씨:</b></td><td>{weather}</td></tr>
                    <tr style="background-color: #f0f0f0;">
                        <td><b>작전가능:</b></td>
                        <td><span style="color: {color}; font-weight: bold;">{go_count}/{total_count}</span></td>
                    </tr>
                </table>
                <p style="margin: 10px 0 0 0; font-size: 11px; color: #666;">
                    <a href="/amws/map" style="color: #667eea;">지도 페이지</a>에서 상세 정보 확인
                </p>
            </div>
            """
        else:
            color = 'gray'
            popup_html = f"""
            <div style="font-family: Arial; width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">{base_id} - {base_name}</h4>
                <p style="color: #999;">기상 데이터 없음</p>
            </div>
            """
        
        # 마커 추가
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{base_id} - {base_name}",
            icon=folium.Icon(color=color, icon='plane', prefix='fa')
        ).add_to(m)
    
    # 지도를 HTML로 변환
    map_html = m._repr_html_()
    
    cur.close()
    conn.close()
    
    return render_template('amws.html', bases=bases, aircrafts=aircrafts, map_html=map_html)

@app.route('/amws/analyze', methods=['POST'])
def amws_analyze():
    """AMWS 분석 로직 (AJAX 요청)"""
    data = request.get_json()
    base_id = data['base_id']
    aircraft_id = data['aircraft_id']
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. 기지 정보 및 최신 기상 조회
    query_weather = """
        SELECT 
            b.base_name, b.runway_heading,
            w.obs_time, w.wind_dir, w.wind_spd_kts, w.visibility_m, w.ceiling_ft, w.weather_desc
        FROM amws.airbases b
        JOIN amws.weather_observations w ON b.base_id = w.base_id
        WHERE b.base_id = %s
        ORDER BY w.obs_time DESC LIMIT 1
    """
    cur.execute(query_weather, (base_id,))
    weather = cur.fetchone()
    
    # 2. 항공기 제한치 조회
    query_aircraft = """
        SELECT max_crosswind_kts, min_visibility_m, min_ceiling_ft, precip_restricted
        FROM amws.aircraft_assets
        WHERE aircraft_id = %s
    """
    cur.execute(query_aircraft, (aircraft_id,))
    asset = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if not weather:
        return jsonify({'error': '해당 기지의 기상 데이터가 없습니다.'})

    # 데이터 매핑
    base_name, rwy_hdg, obs_time, w_dir, w_spd, w_vis, w_ceil, w_desc = weather
    max_xwind, min_vis, min_ceil, precip_limit = asset

    # 3. 측풍 계산
    curr_xwind = calculate_crosswind(w_spd, w_dir, rwy_hdg)
    
    # 4. 판정 로직
    reasons = []
    status = 'GO'
    status_color = 'success'
    
    if curr_xwind > max_xwind:
        status = 'NO-GO'
        reasons.append(f"측풍 초과 (현재: {curr_xwind}kt > 제한: {max_xwind}kt)")
    
    if w_vis < min_vis:
        status = 'NO-GO'
        reasons.append(f"시정 미확보 (현재: {w_vis}m < 제한: {min_vis}m)")
        
    if w_ceil < min_ceil:
        status = 'NO-GO'
        reasons.append(f"운고 낮음 (현재: {w_ceil}ft < 제한: {min_ceil}ft)")

    if precip_limit and ('RA' in w_desc or 'SN' in w_desc):
        status = 'NO-GO'
        reasons.append(f"강수 시 임무 제한 기종 ({w_desc})")

    if status == 'NO-GO':
        status_color = 'danger'
    
    result = {
        'base_name': base_name,
        'obs_time': obs_time.strftime('%H:%M Local'),
        'weather_desc': w_desc,
        'wind_info': f"{w_dir}° / {w_spd}kt",
        'rwy_info': f"{rwy_hdg}°",
        'crosswind': curr_xwind,
        'visibility': w_vis,
        'ceiling': w_ceil,
        'status': status,
        'status_color': status_color,
        'reasons': reasons,
        'limits': {
            'xwind': max_xwind,
            'vis': min_vis,
            'ceil': min_ceil
        }
    }
    
    return jsonify(result)

@app.route('/amws/monitor')
def amws_monitor():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=DictCursor)

    # 기지별 가장 최근 데이터 수신 시간 조회
    sql = """
        SELECT
            b.base_id,
            b.base_name,
            MAX(w.obs_time) as last_update,
            COUNT(w.obs_id) as total_records,
            NOW() - MAX(w.obs_time) as time_diff
        FROM amws.airbases b
        LEFT JOIN amws.weather_observations w ON b.base_id = w.base_id
        GROUP BY b.base_id, b.base_name
        ORDER BY b.base_id
    """
    cur.execute(sql)
    status_list = cur.fetchall()

    cur.close()
    conn.close()

    return render_template('amws_monitor.html', status_list=status_list)

@app.route('/amws/map')
def amws_map():
    """AMWS 지도 페이지 - Folium 기반 (2단계: 항공자산별 상세 정보)"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=DictCursor)
    
    # 모든 비행장 목록 조회
    cur.execute("SELECT base_id, base_name, lat, lon, runway_heading FROM amws.airbases ORDER BY base_id")
    bases = cur.fetchall()
    
    # Folium 지도 생성 (한반도 중심)
    m = folium.Map(
        location=[36.5, 127.5],
        zoom_start=7,
        tiles='OpenStreetMap'
    )
    
    # 각 비행장별로 항공자산 판정 조회 및 마커 추가
    for base in bases:
        base_id = base['base_id']
        base_name = base['base_name']
        lat = float(base['lat'])
        lon = float(base['lon'])
        
        # 쿼리 2번 실행: 특정 기지의 모든 항공자산 판정
        query_detail = """
            WITH latest_weather AS (
                SELECT 
                    base_id,
                    obs_time,
                    wind_dir,
                    wind_spd_kts,
                    visibility_m,
                    ceiling_ft,
                    weather_desc
                FROM amws.weather_observations
                WHERE base_id = %s
                ORDER BY obs_time DESC
                LIMIT 1
            )
            SELECT 
                b.base_id,
                b.base_name,
                a.aircraft_id,
                w.obs_time,
                w.wind_dir,
                w.wind_spd_kts,
                ROUND(ABS(w.wind_spd_kts * SIN(RADIANS(w.wind_dir - b.runway_heading)))::numeric, 1) as crosswind_kts,
                a.max_crosswind_kts,
                w.visibility_m,
                a.min_visibility_m,
                w.ceiling_ft,
                a.min_ceiling_ft,
                w.weather_desc,
                CASE 
                    WHEN w.wind_dir IS NULL OR w.wind_spd_kts IS NULL THEN 'NO DATA'
                    WHEN ABS(w.wind_spd_kts * SIN(RADIANS(w.wind_dir - b.runway_heading))) > a.max_crosswind_kts THEN 'NO-GO'
                    WHEN w.visibility_m < a.min_visibility_m THEN 'NO-GO'
                    WHEN w.ceiling_ft < a.min_ceiling_ft THEN 'NO-GO'
                    WHEN a.precip_restricted AND (w.weather_desc LIKE '%%RA%%' OR w.weather_desc LIKE '%%SN%%') THEN 'NO-GO'
                    ELSE 'GO'
                END as status
            FROM amws.airbases b
            CROSS JOIN amws.aircraft_assets a
            LEFT JOIN latest_weather w ON b.base_id = w.base_id
            WHERE b.base_id = %s
            ORDER BY a.aircraft_id
        """
        
        cur.execute(query_detail, (base_id, base_id))
        assets = cur.fetchall()
        
        if not assets or not assets[0]['obs_time']:
            # 데이터 없음
            color = 'gray'
            popup_html = f"""
            <div style="font-family: Arial; width: 350px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">{base_id} - {base_name}</h4>
                <p style="color: #999;">기상 데이터 없음</p>
            </div>
            """
        else:
            # 기상 데이터 추출
            obs_time = assets[0]['obs_time'].strftime('%H:%M')
            wind_dir = assets[0]['wind_dir']
            wind_spd = assets[0]['wind_spd_kts']
            weather = assets[0]['weather_desc']
            
            # GO/NO-GO 카운트
            go_count = sum(1 for a in assets if a['status'] == 'GO')
            total_count = len(assets)
            
            # 마커 색상
            if go_count == total_count:
                color = 'green'
            elif go_count > 0:
                color = 'orange'
            else:
                color = 'red'
            
            # 항공자산별 상세 테이블 생성
            asset_rows = ""
            for asset in assets:
                status = asset['status']
                aircraft_id = asset['aircraft_id']
                crosswind = asset['crosswind_kts'] or 0
                xwind_limit = asset['max_crosswind_kts']
                vis = asset['visibility_m'] or 0
                vis_limit = asset['min_visibility_m']
                ceil = asset['ceiling_ft'] or 0
                ceil_limit = asset['min_ceiling_ft']
                
                # 상태별 색상
                if status == 'GO':
                    status_color = '#28a745'
                    status_icon = '✅'
                elif status == 'NO-GO':
                    status_color = '#dc3545'
                    status_icon = '❌'
                else:
                    status_color = '#6c757d'
                    status_icon = '⚠️'
                
                # 위반 항목 표시
                violations = []
                if crosswind > xwind_limit:
                    violations.append(f"측풍{crosswind}kt")
                if vis < vis_limit:
                    violations.append(f"시정{vis}m")
                if ceil < ceil_limit:
                    violations.append(f"운고{ceil}ft")
                
                violation_text = ", ".join(violations) if violations else "-"
                
                asset_rows += f"""
                <tr style="font-size: 11px;">
                    <td style="padding: 3px 5px;"><b>{aircraft_id}</b></td>
                    <td style="padding: 3px 5px; text-align: center; color: {status_color}; font-weight: bold;">{status_icon} {status}</td>
                    <td style="padding: 3px 5px; font-size: 10px; color: #666;">{violation_text}</td>
                </tr>
                """
            
            # 팝업 HTML (항공자산별 상세 정보 포함)
            popup_html = f"""
            <div style="font-family: Arial; width: 400px; max-height: 500px; overflow-y: auto;">
                <h4 style="margin: 0 0 10px 0; color: #333; border-bottom: 2px solid #333; padding-bottom: 5px;">
                    {base_id} - {base_name}
                </h4>
                
                <div style="background-color: #f8f9fa; padding: 8px; margin-bottom: 10px; border-radius: 4px;">
                    <table style="width: 100%; font-size: 12px;">
                        <tr><td><b>관측시간:</b></td><td>{obs_time}</td></tr>
                        <tr><td><b>풍향/풍속:</b></td><td>{wind_dir}° / {wind_spd} kt</td></tr>
                        <tr><td><b>날씨:</b></td><td>{weather}</td></tr>
                        <tr style="background-color: #e9ecef;">
                            <td><b>작전가능:</b></td>
                            <td><span style="color: {color}; font-weight: bold; font-size: 14px;">{go_count}/{total_count}</span></td>
                        </tr>
                    </table>
                </div>
                
                <h5 style="margin: 10px 0 5px 0; color: #555; font-size: 13px;">📋 항공자산별 판정</h5>
                <table style="width: 100%; border-collapse: collapse; font-size: 11px;">
                    <thead>
                        <tr style="background-color: #343a40; color: white;">
                            <th style="padding: 5px; text-align: left;">기종</th>
                            <th style="padding: 5px; text-align: center;">상태</th>
                            <th style="padding: 5px; text-align: left;">제한사항</th>
                        </tr>
                    </thead>
                    <tbody>
                        {asset_rows}
                    </tbody>
                </table>
            </div>
            """
        
        # 마커 추가
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=450),
            tooltip=f"{base_id} - {base_name} (클릭하여 상세 정보)",
            icon=folium.Icon(color=color, icon='plane', prefix='fa')
        ).add_to(m)
    
    cur.close()
    conn.close()
    
    # 지도를 HTML로 변환
    map_html = m._repr_html_()
    
    return render_template('amws_map.html', map_html=map_html)

@app.route('/amws/mission-matrix')
def amws_mission_matrix():
    """AMWS 작전 가능 현황 매트릭스 테이블"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=DictCursor)
    
    # 쿼리 1번 실행: 전체 기지/항공자산 조합 매트릭스
    query = """
        WITH latest_weather AS (
            SELECT DISTINCT ON (base_id)
                base_id,
                obs_time,
                wind_dir,
                wind_spd_kts,
                visibility_m,
                ceiling_ft,
                weather_desc
            FROM amws.weather_observations
            ORDER BY base_id, obs_time DESC
        )
        SELECT 
            b.base_id,
            b.base_name,
            b.runway_heading,
            a.aircraft_id,
            a.max_crosswind_kts,
            a.min_visibility_m,
            a.min_ceiling_ft,
            a.precip_restricted,
            w.obs_time,
            w.wind_dir,
            w.wind_spd_kts,
            w.visibility_m,
            w.ceiling_ft,
            w.weather_desc,
            ROUND(ABS(w.wind_spd_kts * SIN(RADIANS(w.wind_dir - b.runway_heading)))::numeric, 1) as crosswind_kts,
            CASE 
                WHEN w.wind_dir IS NULL OR w.wind_spd_kts IS NULL THEN 'NO DATA'
                WHEN ABS(w.wind_spd_kts * SIN(RADIANS(w.wind_dir - b.runway_heading))) > a.max_crosswind_kts THEN 'NO-GO'
                WHEN w.visibility_m < a.min_visibility_m THEN 'NO-GO'
                WHEN w.ceiling_ft < a.min_ceiling_ft THEN 'NO-GO'
                WHEN a.precip_restricted AND (w.weather_desc LIKE '%%RA%%' OR w.weather_desc LIKE '%%SN%%') THEN 'NO-GO'
                ELSE 'GO'
            END as mission_status,
            ARRAY_REMOVE(ARRAY[
                CASE WHEN w.wind_dir IS NOT NULL AND ABS(w.wind_spd_kts * SIN(RADIANS(w.wind_dir - b.runway_heading))) > a.max_crosswind_kts 
                    THEN '측풍' END,
                CASE WHEN w.visibility_m < a.min_visibility_m 
                    THEN '시정' END,
                CASE WHEN w.ceiling_ft < a.min_ceiling_ft 
                    THEN '운고' END,
                CASE WHEN a.precip_restricted AND (w.weather_desc LIKE '%%RA%%' OR w.weather_desc LIKE '%%SN%%') 
                    THEN '강수' END
            ], NULL) as no_go_reasons
        FROM amws.airbases b
        CROSS JOIN amws.aircraft_assets a
        LEFT JOIN latest_weather w ON b.base_id = w.base_id
        ORDER BY b.base_id, a.aircraft_id
    """
    
    cur.execute(query)
    matrix_data = cur.fetchall()
    
    # 항공자산 목록 추출 (컬럼 헤더용)
    cur.execute("SELECT DISTINCT aircraft_id FROM amws.aircraft_assets ORDER BY aircraft_id")
    aircraft_list = [row['aircraft_id'] for row in cur.fetchall()]
    
    # 비행장별로 데이터 그룹화
    bases_dict = {}
    for row in matrix_data:
        base_id = row['base_id']
        if base_id not in bases_dict:
            bases_dict[base_id] = {
                'base_name': row['base_name'],
                'obs_time': row['obs_time'].strftime('%H:%M') if row['obs_time'] else 'N/A',
                'weather': row['weather_desc'] or 'N/A',
                'assets': {}
            }
        
        bases_dict[base_id]['assets'][row['aircraft_id']] = {
            'status': row['mission_status'],
            'crosswind': row['crosswind_kts'],
            'reasons': row['no_go_reasons'] if row['no_go_reasons'] else []
        }
    
    cur.close()
    conn.close()
    
    return render_template('amws_mission_table.html', 
                          bases_dict=bases_dict, 
                          aircraft_list=aircraft_list)