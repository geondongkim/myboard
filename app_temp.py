import os
import psycopg2
from psycopg2.extras import DictCursor
from flask import Flask, render_template
from dotenv import load_dotenv

# 로컬 환경에서는 .env를 읽고, 클라우드 환경에서는 환경 변수를 직접 사용합니다.
if os.path.exists('.env'):
    load_dotenv()

app = Flask(__name__)

# 데이터베이스 연결 함수
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        # sslmode='require' # Azure 연결을 위해 필수
    )
    conn.autocommit = True
    return conn

# 메인 페이지 - FMS 결과 보기
@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)
    # 앞서 생성한 FMS 뷰 조회
    cursor.execute("SELECT * FROM fms.total_result LIMIT 10")
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('fms_result_temp.html', results=results)

if __name__ == '__main__':
    app.run(debug=True)
 
 