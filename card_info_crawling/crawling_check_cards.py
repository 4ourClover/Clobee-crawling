from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import psycopg2
import os
from datetime import datetime
import time

# .env 파일 로드
load_dotenv()

# DB 환경변수 가져오기
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# ChromeDriver 경로 설정
service = Service(executable_path="C:/02.devEnv/chromedriver-win64/chromedriver.exe")
driver = webdriver.Chrome(service=service)

# 사이트 접속
driver.get('https://www.card-gorilla.com/card?cate=CHK')

# 명시적 대기
wait = WebDriverWait(driver, 10)
wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#q-app > section > div.card > section > div > div.card_list')))

# 카드 더보기 버튼 계속 누르기
while True:
    try:
        more_button = driver.find_element(By.CSS_SELECTOR, '#q-app > section > div.card > section > div > div.card_list > div.ftr > a.lst_more')
        if more_button.is_displayed():
            driver.execute_script("arguments[0].click();", more_button)
            time.sleep(1)
        else:
            print("--- 모든 카드 로딩 완료 (버튼 안보임) ---")
            break
    except:
        print("--- 모든 카드 로딩 완료 (버튼 없음) ---")
        break

# 모든 카드 요소 찾기
card_elements = driver.find_elements(By.CSS_SELECTOR, '#q-app > section > div.card > section > div > div.card_list > ul > li')
print(f"총 카드 수: {len(card_elements)}개")

# 카드 데이터 추출
check_cards = []
for i in range(1, len(card_elements) + 1):
    try:
        img = driver.find_element(By.CSS_SELECTOR, f'#q-app > section > div.card > section > div > div.card_list > ul > li:nth-child({i}) > div > div.card_img > p > img').get_attribute('src')
        name = driver.find_element(By.CSS_SELECTOR, f'#q-app > section > div.card > section > div > div.card_list > ul > li:nth-child({i}) > div > div.card_data > div.name > p > span.card_name').text.strip()
        corp = driver.find_element(By.CSS_SELECTOR, f'#q-app > section > div.card > section > div > div.card_list > ul > li:nth-child({i}) > div > div.card_data > div.name > p > span.card_corp').text.strip()

        # 연회비 초기값 None
        domestic_fee = None
        international_fee = None

        fee_spans = driver.find_elements(By.CSS_SELECTOR, f'#q-app > section > div.card > section > div > div.card_list > ul > li:nth-child({i}) > div > div.card_data > div.ex > p.in_for > span')
        
        for span in fee_spans:
            text = span.text.strip()
            if '국내전용' in text:
                if '없음' in text:
                    domestic_fee = 0
                else:
                    fee = text.replace('국내전용', '').replace('원', '').replace(',', '').strip()
                    domestic_fee = int(fee) if fee.isdigit() else 0
            elif '해외겸용' in text:
                if '없음' in text:
                    international_fee = 0
                else:
                    fee = text.replace('해외겸용', '').replace('원', '').replace(',', '').strip()
                    international_fee = int(fee) if fee.isdigit() else 0

        check_cards.append({
            'rank': i, 
            'name': name,
            'corp': corp,
            'image_url': img,
            'domestic_fee': domestic_fee,
            'international_fee': international_fee
        })
    except Exception as e:
        print(f"{i}번 카드 크롤링 실패: {e}")
        continue

driver.quit()

# PostgreSQL 연결
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

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

next_brand_id = max(brand_mapping.values()) + 1

# 체크카드
card_type = 402

# 현재 DB 카드 이름 읽기
cursor.execute('SELECT card_name FROM card_info')
db_card_names = set(row[0] for row in cursor.fetchall())

# 새로 크롤링한 카드 이름
new_card_names = set(card['name'] for card in check_cards)

# 삭제 대상 (DB에는 있는데 새로 크롤링에는 없는 카드)
cards_to_delete = db_card_names - new_card_names
for card_name in cards_to_delete:
    cursor.execute('DELETE FROM card_info WHERE card_name = %s', (card_name,))

# 카드 삽입/업데이트
for card in check_cards:
    now = datetime.now()
    brand_id = brand_mapping.get(card['corp'])
    if brand_id is None:
        brand_id = next_brand_id
        brand_mapping[card['corp']] = brand_id
        next_brand_id += 1
        print(f"새로운 브랜드 등록: {card['corp']} → {brand_id}")

    if card['name'] in db_card_names:
        # UPDATE (rank 수정)
        cursor.execute('''
            UPDATE card_info
            SET card_rank = %s, updated_at = %s
            WHERE card_name = %s
        ''', (card['rank'], now, card['name']))
    else:
        # INSERT
        cursor.execute('''
            INSERT INTO card_info (
                card_name, card_brand, card_domestic_annual_fee, card_expiry_date,
                card_type, card_image_url, card_views, created_at, updated_at,
                card_global_annual_fee, card_rank
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            card['name'],
            brand_id,
            card['domestic_fee'],
            None,
            card_type,
            card['image_url'],
            0,
            now,
            now,
            card['international_fee'],
            card['rank']
        ))

conn.commit()
cursor.close()
conn.close()

print("🎉 모든 카드 데이터 DB 업데이트 완료!")