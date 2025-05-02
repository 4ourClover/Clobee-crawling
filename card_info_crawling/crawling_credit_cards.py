from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import psycopg2
import platform
import os
from datetime import datetime
import time
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from brand_mapping import brand_mapping

def run_credit_cards_crawler():
    # OS 구분
    system = platform.system()

    # 크롬드라이버 경로 지정
    if system == 'Windows':
        driver_path = os.path.abspath('chromedriver-win/chromedriver.exe')
    elif system == 'Darwin':  # macOS
        driver_path = os.path.abspath('chromedriver-mac/chromedriver')
    elif system == 'Linux':
        driver_path = '/usr/bin/chromedriver'
    else:
        raise Exception(f'Unsupported OS: {system}')


    # .env 파일 로드
    dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    load_dotenv(dotenv_path=dotenv_path)

    # DB 환경변수 가져오기
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service)
    
    # 사이트 접속
    driver.get('https://www.card-gorilla.com/card?cate=CHK')

    # 사이트 접속
    driver.get('https://www.card-gorilla.com/card?cate=CRD')

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
    credit_cards = []
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

            credit_cards.append({
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

    next_brand_id = max(brand_mapping.values()) + 1

    card_type = 401

    # DB에 있는 (card_name, card_type) 가져오기
    cursor.execute('SELECT card_name, card_type FROM card_info')
    db_cards = set((row[0], row[1]) for row in cursor.fetchall())

    # 크롤링한 (card_name, card_type)
    new_cards = set((card['name'], card_type) for card in credit_cards)

    # 삭제할 카드 (DB에는 있는데 크롤링한 데이터에는 없는 경우)
    cards_to_delete = db_cards - new_cards
    for card_name, card_type_value in cards_to_delete:
        if card_type_value == card_type:  # 현재 실행 중인 카드 타입(401)만 삭제
            cursor.execute('DELETE FROM card_info WHERE card_name = %s AND card_type = %s', (card_name, card_type_value))

    # 카드 삽입/업데이트
    for idx, card in enumerate(credit_cards, start=1):
        now = datetime.now()

        # 등록되지 않은 카드 회사 확인 및 등록
        brand_id = brand_mapping.get(card['corp'])
        if brand_id is None:
            brand_id = next_brand_id
            brand_mapping[card['corp']] = brand_id
            next_brand_id += 1
            print(f"새로운 브랜드 등록: {card['corp']} → {brand_id}")

        if (card['name'], card_type) in db_cards:
            # 업데이트
            cursor.execute('''
                UPDATE card_info
                SET card_rank = %s, updated_at = %s
                WHERE card_name = %s AND card_type = %s
            ''', (idx, now, card['name'], card_type))
        else:
            # 삽입
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
                idx
            ))

    conn.commit()
    cursor.close()
    conn.close()

    print("🎉 모든 카드 데이터 DB 업데이트 완료!")

if __name__ == '__main__':
    run_credit_cards_crawler()