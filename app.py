import os
import psycopg2
from psycopg2.extras import DictCursor
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from dotenv import load_dotenv
from datetime import datetime
import json

# 로컬 환경에서는 .env를 읽고, Azure에서는 패스.
if os.path.exists('.env'):
    load_dotenv()
app = Flask(__name__)
app.secret_key = os.urandom(24)

# 데이터베이스 연결 함수
def get_db_connection():
    try:
        conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        sslmode='require' # Azure 연결을 위해 필수
        )
        print('✓ 데이터베이스 연결 성공!')
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        print('✗ 데이터베이스 연결 실패!')
        print(f'에러 상세: {e}')
        print(f'연결 정보:')
        print(f'  - host: {os.getenv("DB_HOST")}')
        print(f'  - port: {os.getenv("DB_PORT")}')
        print(f'  - dbname: {os.getenv("DB_NAME")}')
        print(f'  - user: {os.getenv("DB_USER")}')
        print(f'  - sslmode: require')
        raise

@app.route('/')
def index():
    # 1. 데이터 베이스에 접속
    conn = get_db_connection()
    print('get_db_connection', conn)
    cursor = conn.cursor(cursor_factory=DictCursor)
    # 2. SELECT
    cursor.execute("SELECT id, title, author, created_at, view_count, like_count FROM board.posts ORDER BY created_at DESC")
    posts = cursor.fetchall()
    cursor.close()
    conn.close()
    # 3. index.html 파일에 변수로 넘겨주기
    return render_template('index.html', posts = posts)

@app.route('/create/', methods=['GET'] )
def create_form():
    return render_template('create.html')

@app.route('/create/',methods=['POST']  )
def create_post():
    #1. 폼에 있는 정보들을 get
    title = request.form.get('title')
    author = request.form.get('author')
    content = request.form.get('content')

    if not title or not author or not content:
        flash('모든 필드를 똑바로 채워주세요!!!!')
        return redirect(url_for('create_form'))
    
    # 1. 데이터 베이스에 접속
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    # 2. INSERT
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

@app.route('/fms/')
def fms_result():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    
    # fms.total_result 뷰에서 데이터 조회
    cursor.execute('SELECT * FROM fms.total_result ORDER BY 육계번호')
    results = cursor.fetchall()
    
    # 1. 전체 통계
    cursor.execute('SELECT COUNT(*) as total FROM fms.total_result')
    total_count = cursor.fetchone()['total']
    
    # 2. 적합/부적합 현황
    cursor.execute("SELECT 부적합여부, COUNT(*) as count FROM fms.total_result GROUP BY 부적합여부")
    quality_stats = cursor.fetchall()
    quality_dict = {row['부적합여부']: row['count'] for row in quality_stats}
    
    # 3. 품종별 통계
    cursor.execute("""
        SELECT 
            품종, 
            COUNT(*) as total_count, 
            SUM(CASE WHEN 부적합여부 = 'Pass' THEN 1 ELSE 0 END) as pass_count 
        FROM fms.total_result 
        GROUP BY 품종 
        ORDER BY total_count DESC
    """)
    breed_stats = cursor.fetchall()
    
    # 4. 고객사별 통계
    cursor.execute("SELECT 고객사, COUNT(*) as count FROM fms.total_result GROUP BY 고객사 ORDER BY count DESC")
    customer_stats = cursor.fetchall()
    
    # ===== 새로운 그래프 데이터 =====
    
    # 5. 성별 분포
    cursor.execute("""
        SELECT 
            gender,
            COUNT(*) as count
        FROM fms.chick_info
        GROUP BY gender
        ORDER BY gender
    """)
    gender_stats = cursor.fetchall()
    
    # 6. 농장별 병아리 수
    cursor.execute("""
        SELECT 
            farm,
            COUNT(*) as count
        FROM fms.chick_info
        GROUP BY farm
        ORDER BY farm
    """)
    farm_stats = cursor.fetchall()
    
    # 7. 접종현황 (vaccination1, vaccination2)
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN vaccination1 = 1 THEN 1 ELSE 0 END) as vac1_done,
            SUM(CASE WHEN vaccination1 = 0 THEN 1 ELSE 0 END) as vac1_not,
            SUM(CASE WHEN vaccination2 = 1 THEN 1 ELSE 0 END) as vac2_done,
            SUM(CASE WHEN vaccination2 = 0 THEN 1 ELSE 0 END) as vac2_not
        FROM fms.chick_info
    """)
    vaccination_stats = cursor.fetchone()
    
    # 8. 품종별 분포 (코드명으로)
    cursor.execute("""
        SELECT 
            m.code_desc as breed_name,
            COUNT(c.chick_no) as count
        FROM fms.chick_info c
        JOIN fms.master_code m ON c.breeds = m.code AND m.column_nm = 'breeds'
        GROUP BY m.code_desc
        ORDER BY count DESC
    """)
    breed_distribution = cursor.fetchall()
    
    # 5. 적합률 계산
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
        # 새로운 데이터
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
    """FMS 데이터베이스 구조 확인"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    
    try:
        # fms 스키마의 모든 테이블 확인
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'fms'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        
        result = "<h1>FMS Schema 분석</h1>"
        result += "<h2>📊 Available Tables:</h2><ul>"
        for table in tables:
            result += f"<li><b>{table['table_name']}</b></li>"
        result += "</ul>"
        
        # total_result 샘플 데이터
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
    """각 테이블의 컬럼 확인"""
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
    """Chart.js 테스트"""
    return '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Chart.js 테스트</title>
</head>
<body>
    <h1>Chart.js 기본 테스트</h1>
    <div style="width: 500px; height: 400px;">
        <canvas id="testChart"></canvas>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        console.log('Chart available:', typeof Chart !== 'undefined');
        
        const ctx = document.getElementById('testChart');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['A', 'B', 'C'],
                datasets: [{
                    label: 'Test',
                    data: [12, 19, 3],
                    backgroundColor: ['red', 'blue', 'green']
                }]
            }
        });
        console.log('Chart created!');
    </script>
</body>
</html>
    '''
if __name__ == '__main__':
    app.run(debug=True)

