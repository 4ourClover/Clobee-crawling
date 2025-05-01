'''
카드사 이벤트 내역 가져오기
- 작성자 : 이슬기
- 내용 : 카드사 이벤트 업데이트, 이벤트 진행여부 업데이트
'''

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import os, time
import psycopg2
import socket

# 브랜드 매핑
brand_mapping = {
    '우리카드': 301,
    '신한카드': 302,
    'IBK기업은행': 303,
    'KB국민카드': 304,
    '하나카드': 305,
    '삼성카드': 306,
    '현대카드': 307,
    '롯데카드': 308,
    'NH농협카드': 309,
    'BC 바로카드': 310,
    '씨티카드': 311,
    'KG모빌리언스': 312,
    'MG새마을금고': 313,
    '우체국': 314,
    '카카오뱅크': 315,
    '케이뱅크': 316,
    '카카오페이': 317,
    '네이버페이': 318,
    '토스페이': 319,
    '토스뱅크': 320,
    '현대백화점': 321,
    '광주은행': 322,
    '신협': 323,
    '제주은행': 324,
    'BNK부산은행': 325,
    'BNK경남은행': 326,
    'Sh수협은행': 327,
    'SSGPAY. CARD': 328,
    'iM뱅크': 329,
    '전북은행': 330,
    'SC제일은행': 331,
    '차이': 332,
    '엔에이치엔페이코': 333,
    'KB증권': 334,
    '미래에셋증권': 335,
    '코나카드': 336,
    '트래블월렛': 337,
    'KDB산업은행': 338,
    '한국투자증권': 339,
    '한패스': 340,
    'DB금융투자': 341,
    'NH투자증권': 342,
    'SBI저축은행': 343,
    '유안타증권': 344,
    '유진투자증권': 345,
    '토스': 346,
    '핀크카드': 347,
    'SK증권': 348,
    '다날': 349,
    '머니트리': 350,
    '교보증권': 351,
    '아이오로라': 352,
    '핀트': 353
}

# 현재 IP 가져오기
hostname = socket.gethostname()
local_ip = socket.gethostbyname(hostname)

# 로컬 여부 확인
is_local = local_ip.startswith("127.") or local_ip.startswith("192.168.") or local_ip == "localhost"

# .env 파일 로드
load_dotenv()

# DB 환경변수 가져오기
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# PostgreSQL 연결 설정
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)
cur = conn.cursor()

# ChromeDriver 경로 설정
if is_local:
    driver_path = "/Users/iseulgi/Desktop/clobee/Clobee-crawling/chromedriver-mac/chromedriver"
else:
    driver_path = "/usr/bin/chromedriver" # 서버 드라이버

service = Service(executable_path=driver_path)
driver = webdriver.Chrome(service=service)

url = "https://www.card-gorilla.com/benefit"
driver.get(url)
time.sleep(3)

# 페이지 소스 가져오기
soup = BeautifulSoup(driver.page_source, "html.parser")

# 각 카드 이벤트 블록 가져오기
cards = soup.select(".benefit_lst > .inner > .ctnr")

driver.quit()

# 데이터 저장
for card in cards:
    try:
        corp = card.select_one(".corp").get_text(strip=True)
        event_title = card.select_one(".brand").get_text(strip=True)
        subj = card.select_one(".subj").get_text(strip=True)
        date = card.select_one(".date").get_text(strip=True).split(" ")
        href = card.get("href")
        
        event_card_corp = brand_mapping[corp]
        
        # 이벤트 준비중이면 무시
        if event_title == "이벤트 준비중":
            continue
        
        # 중복 확인
        cur.execute("""
            SELECT 1 FROM event_info
            WHERE event_card_corp = %s AND event_title = %s AND event_desc = %s
        """, (event_card_corp, event_title, subj))
        
        exists = cur.fetchone()
        if exists:
            print(f"중복된 이벤트: {event_card_corp} - {event_title}")
            continue
        
        with conn:
            cur.execute("""
                INSERT INTO event_info (event_card_corp, event_title, event_desc, event_type_cd, event_start_day, event_end_day, event_status_cd, event_card_url, is_deleted)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (event_card_corp, event_title, subj, 601, date[0], date[2], 702, href, False))
            conn.commit()
    
    except Exception as e:
        print(f"{card} 카드 크롤링 실패: {e}")
        continue

# 이벤트 종료날짜 지난 이벤트 불러오기
cur.execute("""
    SELECT event_info_id FROM event_info
    WHERE event_status_cd = 702
        AND event_end_day < CURRENT_DATE
""")

rows = cur.fetchall()

for row in rows:
    event_info_id = row[0]
    try:
        cur.execute("""
            UPDATE event_info
            SET event_status_cd = 703
            WHERE event_info_id = %s
        """, (event_info_id))
    except Exception as e:
        print(f"event_info_id : {event_info_id} 업데이트 실패: {e}")
        continue

# 종료
cur.close()
conn.close()

print(f"🎉 모든 카드사 이벤트 데이터 DB 업데이트 완료!")