import requests
import psycopg2
import math
import datetime
from datetime import timedelta
from dotenv import load_dotenv
import os

# 로컬 환경에서는 .env를 읽고, Azure에서는 패스.
if os.path.exists('.env'):
    load_dotenv()

# ==========================================
# 1. 설정 정보 (Configuration)
# ==========================================
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),  # 본인의 DB 이름
    'user': os.getenv('DB_USER'),      # 본인의 DB 유저
    'password': os.getenv('DB_PASSWORD'),  # 본인의 DB 비밀번호
    'port': os.getenv('DB_PORT')
}

# 공공데이터포털 일반 인증키 (Decoding Key 권장)
API_KEY = "115f8b99b25da9f32ca31ed27c809f099e792b567721304e248df98379c83b5d"
API_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

# ==========================================
# 2. 기상청 격자 변환 함수 (Lat/Lon -> Grid X,Y)
# ==========================================
def map_to_grid(lat, lon):
    """
    위경도를 기상청 격자 좌표(X, Y)로 변환하는 공식
    (기상청 가이드 문서 기반)
    """
    RE = 6371.00877  # 지구 반경(km)
    GRID = 5.0       # 격자 간격(km)
    SLAT1 = 30.0     # 투영 위도1
    SLAT2 = 60.0     # 투영 위도2
    OLON = 126.0     # 기준점 경도
    OLAT = 38.0      # 기준점 위도
    XO = 43          # 기준점 X좌표
    YO = 136         # 기준점 Y좌표

    DEGRAD = math.pi / 180.0

    re = RE / GRID
    slat1 = SLAT1 * DEGRAD
    slat2 = SLAT2 * DEGRAD
    olon = OLON * DEGRAD
    olat = OLAT * DEGRAD

    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = math.pow(sf, sn) * math.cos(slat1) / sn
    ro = math.tan(math.pi * 0.25 + olat * 0.5)
    ro = re * sf / math.pow(ro, sn)

    ra = math.tan(math.pi * 0.25 + lat * DEGRAD * 0.5)
    ra = re * sf / math.pow(ra, sn)
    theta = lon * DEGRAD - olon
    if theta > math.pi: theta -= 2.0 * math.pi
    if theta < -math.pi: theta += 2.0 * math.pi
    theta *= sn

    x = math.floor(ra * math.sin(theta) + XO + 0.5)
    y = math.floor(ro - ra * math.cos(theta) + YO + 0.5)
    return x, y

# ==========================================
# 3. 메인 로직
# ==========================================
def fetch_and_store_weather():
    conn = None
    try:
        # DB 연결
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1) 비행장 목록 가져오기
        cur.execute("SELECT base_id, lat, lon FROM amws.airbases")
        airbases = cur.fetchall()

        # API 호출 시간 설정 (현재 시간에서 40분 전 기준 - 데이터 생성 딜레이 고려)
        now = datetime.datetime.now()
        base_date = now.strftime("%Y%m%d")
        # 정각 데이터는 보통 매시 40분경 발표됨
        if now.minute < 40:
            check_time = now - timedelta(hours=1)
            base_time = check_time.strftime("%H00")
        else:
            base_time = now.strftime("%H00")

        print(f"[*] Fetching Weather Data... (Base Time: {base_date} {base_time})")

        for base in airbases:
            b_id, lat, lon = base
            # DB에서 조회한 값이 Decimal일 수 있으므로 float로 변환
            lat = float(lat)
            lon = float(lon)
            nx, ny = map_to_grid(lat, lon)

            # 2) API 호출
            params = {
                'serviceKey': API_KEY,
                'pageNo': '1',
                'numOfRows': '1000',
                'dataType': 'JSON',
                'base_date': base_date,
                'base_time': base_time,
                'nx': nx,
                'ny': ny
            }

            response = requests.get(API_URL, params=params)

            if response.status_code != 200:
                print(f"[-] API Error for {b_id}: {response.status_code}")
                continue

            data = response.json()
            items = data['response']['body']['items']['item']

            # 데이터 파싱용 딕셔너리
            weather_data = {}
            for item in items:
                weather_data[item['category']] = float(item['obsrValue'])

            # 3) 데이터 가공 및 단위 변환
            # PTY: 강수형태 (0:없음, 1:비, 2:비/눈, 3:눈, 5:빗방울, 6:빗방울눈날림, 7:눈날림)
            # WSD: 풍속 (m/s) -> Knots 변환 (1 m/s = 1.94384 kts)
            # VEC: 풍향 (deg)

            pty_code = int(weather_data.get('PTY', 0))
            wind_spd_ms = float(weather_data.get('WSD', 0))
            wind_dir = int(weather_data.get('VEC', 0))

            wind_spd_kts = round(wind_spd_ms * 1.94384, 1)

            # --- [중요] 시정(Vis) 및 운고(Ceiling) 추정 로직 ---
            # 일반 공공데이터는 공항 전용(METAR) 시정/운고를 제공하지 않으므로 PTY 기반 추정
            # 실제 서비스시에는 '항공기상청' API를 써야 정확하지만, 미니 프로젝트용으로 로직 구현

            visibility_m = 9999
            ceiling_ft = 30000
            weather_desc = "SKC" # Default

            if pty_code == 0:
                weather_desc = "SKC" # 맑음
            elif pty_code in [1, 5]: # 비
                weather_desc = "RA"
                visibility_m = 4000
                ceiling_ft = 2000
            elif pty_code in [2, 6]: # 진눈깨비
                weather_desc = "RASN"
                visibility_m = 2000
                ceiling_ft = 1000
            elif pty_code in [3, 7]: # 눈
                weather_desc = "SN"
                visibility_m = 1500
                ceiling_ft = 800

            # 4) DB Insert
            sql = """
            INSERT INTO amws.weather_observations
            (base_id, obs_time, wind_dir, wind_spd_kts, visibility_m, ceiling_ft, weather_desc)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            # obs_time은 API 기준 시간 + 현재 분 사용
            current_obs_time = datetime.datetime.strptime(f"{base_date}{base_time}", "%Y%m%d%H%M")

            cur.execute(sql, (b_id, current_obs_time, wind_dir, wind_spd_kts, visibility_m, ceiling_ft, weather_desc))
            print(f"[+] Inserted {b_id}: Wind {wind_spd_kts}kt / {weather_desc}")

        conn.commit()
        print("[*] Data Update Complete.")

    except Exception as e:
        import traceback
        print(f"[!] Error: {e}")
        print(traceback.format_exc())
        if conn: conn.rollback()

    finally:
        if conn: conn.close()

if __name__ == "__main__":
    fetch_and_store_weather()
