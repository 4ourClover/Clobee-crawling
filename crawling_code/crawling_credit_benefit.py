from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from airflow.hooks.postgres_hook import PostgresHook
from dotenv import load_dotenv
import psycopg2
import platform
import os, socket
import time
import sys, logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests

def transform_stores(stores, brand):
    """
    stores 배열의 항목을 브랜드에 따라 변환합니다.
    
    :param stores: 가맹점 목록 배열
    :param brand: 카드사 브랜드 (예: '삼성카드', 'KB국민카드' 등)
    :return: 변환된 가맹점 목록 배열
    """
    # 변환 맵핑 정의 (키워드 -> {카드사: [가맹점 목록]})
    store_mappings = {
        "대형마트": {
            "KB국민카드": ["이마트", "롯데마트", "홈플러스"],
        },
        "마트": {
            "신한카드": ["이마트", "롯데마트", "홈플러스"]
        },
        "대형 할인점": {
            "KB국민카드": ["이마트", "홈플러스", "롯데마트", "메가마트", "Y_MART(영암마트)"]
        },
        "할인점": {
            "삼성카드": ["이마트", "트레이더스", "롯데마트", "홈플러스"]
        },
        "영화": {
            "삼성카드": ["CGV", "롯데시네마", "메가박스"],
            "IBK기업은행": ["CGV", "롯데시네마", "메가박스"],
            "NH농협카드": ["CGV", "롯데시네마"]
        },
        "편의점": {
            "삼성카드": ["CU", "GS25", "세븐일레븐", "미니스톱", "이마트24"],
            "신한카드": ["GS25", "CU"],
            "IBK기업은행": ["GS25", "CU", "세븐일레븐"]
        },
        "4대 편의점": {
            "신한카드": ["GS25", "CU", "세븐일레븐", "이마트24"]
        },
        "커피": {
            "신한카드": ["스타벅스", "이디야"],
            "NH농협카드": ["스타벅스"]
        },
        "커피전문점(오프라인)": {
            "삼성카드": ["스타벅스", "투썸플레이스", "카페베네", "탐앤탐스", "커피빈", "엔제리너스", "할리스커피", "파스쿠찌", "아티제", "폴 바셋"]
        },
        "주요 커피전문점": {
            "IBK기업은행": ["스타벅스", "커피빈", "카페베네", "탐앤탐스", "엔제리너스", "투썸플레이스", "할리스", "달.콤"]
        },
        "생활": {
            "신한카드": ["올리브영", "다이소"]
        },
        "생활잡화": {
            "삼성카드": ["다이소"]
        },
        "버거": {
            "KB국민카드": ["맥도날드", "버거킹", "롯데리아"]
        },
        "백화점": {
            "우리카드": ["롯데", "현대", "신세계"],
            "삼성카드": ["신세계", "롯데", "현대", "갤러리아", "동아", "대구", "세이백화점", "AK플라자"]
        },
        "슈퍼마켓": {
            "우리카드": ["이마트 에브리데이", "롯데슈퍼", "홈플러스 익스프레스", "GS 수퍼마켓"]
        },
        # 교통과 주유는 이전 요청의 규칙도 추가
        "교통": {
            # 모든 카드사에 대해 동일한 변환
            "ALL": ["지하철", "버스 터미널"]
        },
        "주유": {
            # 모든 카드사에 대해 동일한 변환
            "ALL": ["GS칼텍스", "SK에너지", "S-OIL", "에이치디현대오일뱅크"]
        },
        "정유사": {
            # 모든 카드사에 대해 동일한 변환
            "ALL": ["GS칼텍스", "SK에너지", "S-OIL", "에이치디현대오일뱅크"]
        }
    }
    
    # 변환된 결과를 저장할 새 배열
    transformed_stores = []
    
    # 각 가맹점 항목을 처리
    for store in stores:
        replaced = False
        
        # 더 구체적인(긴) 키워드부터 처리하도록 정렬
        sorted_keywords = sorted(store_mappings.keys(), key=len, reverse=True)

        # 변환 매핑에서 일치하는 키워드가 있는지 확인
        for keyword in sorted_keywords:
            # 정확한 일치를 확인하거나 더 정확한 매칭 조건 사용
            if store.strip() == keyword or (keyword in store and len(keyword) > 3):  # 최소 4글자 이상 키워드만 부분 매칭
                # 키워드가 발견되면, 브랜드에 맞는 매핑 찾기
                if brand in store_mappings[keyword]:
                    # 브랜드별 매핑이 있으면 추가
                    transformed_stores.extend(store_mappings[keyword][brand])
                    replaced = True
                    break
                elif "ALL" in store_mappings[keyword]:
                    # 모든 브랜드에 대한 공통 매핑이 있으면 추가
                    transformed_stores.extend(store_mappings[keyword]["ALL"])
                    replaced = True
                    break
        
        # 변환되지 않은 항목은 그대로 추가
        if not replaced:
            transformed_stores.append(store)
    
    return transformed_stores


def run_credit_cards_benefit_crawler():
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    # OS 구분
    system = platform.system()
    options = Options()

    # 크롬드라이버 경로 지정
    if system == 'Windows':
        driver_path = os.path.abspath('chromedriver-win/chromedriver.exe')
    elif system == 'Darwin':  # macOS
        driver_path = os.path.abspath('chromedriver-mac/chromedriver')
    elif system == 'Linux':
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.binary_location = "/usr/bin/google-chrome"
        driver_path = '/usr/bin/chromedriver'
    else:
        raise Exception(f'Unsupported OS: {system}')

    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    # 사이트 접속
    driver.get('https://www.card-gorilla.com/card?cate=CRD')
    
    # 로컬 여부 확인
    is_local = local_ip.startswith("127.") or local_ip.startswith("192.168.") or local_ip == "localhost"
    
    if is_local:
        pg_hook = PostgresHook(postgres_conn_id="dev_pg")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info("⭕ Local DB Load Done")
    else:
        pg_hook = PostgresHook(postgres_conn_id="my_pg")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info("⭕ Prod(connection) DB Load Done")

    print("✅ 데이터베이스 연결 성공")

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

    card_elements = driver.find_elements(By.CSS_SELECTOR, '#q-app > section > div.card > section > div > div.card_list > ul > li')
    num_cards = len(card_elements)

    # 카드 데이터 추출
    credit_cards = []
    for i in range(1, num_cards + 1):
        try:
            name = driver.find_element(By.CSS_SELECTOR, f'#q-app > section > div.card > section > div > div.card_list > ul > li:nth-child({i}) > div > div.card_data > div.name > p > span.card_name').text.strip()
            params = {"cardName": name}
            headers = {
                'Accept': 'application/json;charset=utf-8',
                'User-Agent': 'Python-requests/2.31.0'
            }

            try:
                print(f"API 호출 시작: {name}")

                response = requests.get(
                    "http://localhost:8080/api/card/getCardId", 
                    params=params,
                    headers=headers
                )
                print(f"응답 상태: {response.status_code}")
                print(f"실제 요청 URL: {response.url}")

                response.raise_for_status()  # HTTP 에러 체크
                card_id = response.json()
                print(f"카드명: {name} -> 카드ID: {card_id}")
            except requests.exceptions.RequestException as e:
                print(f"API 호출 실패: {e}")
                break

            brand = driver.find_element(By.CSS_SELECTOR, f'#q-app > section > div.card > section > div > div.card_list > ul > li:nth-child({i}) > div > div.card_data > div.name > p > span.card_corp').text.strip()

            # 혜택 조건 정보 가져오기
            condition = driver.find_element(By.CSS_SELECTOR, 
                f'#q-app > section > div.card > section > div > div.card_list > ul > li:nth-child({i}) > div > div.card_data > div.ex > p.l_mth').text.strip()
            

            # 한 카드에 대한 모든 혜택 항목 가져오기
            benefit_items = driver.find_elements(By.CSS_SELECTOR, 
                f'#q-app > section > div.card > section > div > div.card_list > ul > li:nth-child({i}) > div > div.card_data > div.sale > p')

            for j, benefit_item in enumerate(benefit_items, 1):
                try:
                    # 가맹점 정보
                    store_text = benefit_item.find_element(By.CSS_SELECTOR, 'i').text.strip()
                    
                    # 가맹점 구분자로 분리 (/, ·, ,, ・ 등)
                    # 모든 구분자를 하나의 통일된 구분자로 먼저 변경한 후 분리
                    store_text = store_text.replace('/', ',').replace('·', ',').replace('・', ',')

                    # 전체 텍스트에 숫자가 포함되어 있는지 확인
                    has_digit = any(char.isdigit() for char in store_text)

                    # 숫자가 포함되어 있으면 전체를 분리하지 않고 하나의 항목으로 처리
                    if has_digit:
                        stores = [store_text]
                    else:
                        # 숫자가 없는 경우만 쉼표로 분리
                        stores = [s.strip() for s in store_text.split(',') if s.strip()]

                    
                    # 카드사 브랜드에 따라 가맹점 변환
                    # 여기서 brand는 카드사 이름 (예: "삼성카드", "KB국민카드" 등)
                    stores = transform_stores(stores, brand)


                    # 혜택
                    benefit_percent = benefit_item.find_element(By.CSS_SELECTOR, 'span > b').text.strip()

                    benefit_desc = benefit_item.find_element(By.CSS_SELECTOR, 'span').text.strip()

                    benefit_desc = benefit_desc.replace(benefit_percent, '').strip()
                    
                    for store in stores:
                        #카드 데이터를 리스트에 추가
                        credit_cards.append({
                            '카드 id' : card_id,
                            '카드 이름': name,
                            '가맹점': store,
                            '혜택': benefit_percent,
                            '혜택 설명': benefit_desc,
                            '혜택 조건': condition 
                        })
                
                except Exception as e:
                    print(f"혜택 항목 {j} 추출 실패: {e}")
                

        except Exception as e:
            print(f"{i}번 카드 크롤링 실패: {e}")
            continue

    print(f"\n=== 크롤링 완료 ===")
    print(f"총 {len(credit_cards)}개 혜택 데이터 수집")

    driver.quit()

    # ⭐ 중요: 모든 변수를 try 블록 외부에서 초기화
    conn = None
    cursor = None
    saved_count = 0
    skipped_count = 0
    error_count = 0

    try:
        cursor.execute("DELETE FROM card_benefit_detail")
        deleted_count = cursor.rowcount
        print(f"🗑️ 기존 혜택 데이터 {deleted_count}개 삭제")
            
        for i, item in enumerate(credit_cards, 1):
            try:
                card_id = item.get('카드 id')
                card_name = item.get('카드 이름', '').strip()
                store = item.get('가맹점', '').strip()
                benefit = item.get('혜택', '').strip()
                benefit_desc = item.get('혜택 설명', '').strip()
                condition = item.get('혜택 조건', '').strip()
                
                # 필수 데이터 검증
                if not card_id or not store or not benefit:
                    print(f"❌ 항목 {i}: 필수 데이터 누락 - 카드ID:{card_id}, 가맹점:{store}, 혜택:{benefit}")
                    skipped_count += 1
                    continue
                
                # 데이터 삽입
                cursor.execute("""
                    INSERT INTO card_benefit_detail (
                        card_info_id,
                        card_benefit_store,
                        card_benefit_discnt_price,
                        card_benefit_desc,
                        card_benefit_condition
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    card_id,
                    store,
                    benefit,
                    benefit_desc,
                    condition
                ))
            
                saved_count += 1
                
                # 진행 상황 표시 (100개마다)
                if i % 100 == 0:
                    print(f"📊 진행률: {i}/{len(credit_cards)} ({i/len(credit_cards)*100:.1f}%)")
                
            except psycopg2.IntegrityError as e:
                print(f"⚠️ 항목 {i}: 중복 또는 제약조건 위반 - {store}")
                skipped_count += 1
                continue
                
            except Exception as e:
                print(f"❌ 항목 {i}: 저장 실패 - {e}")
                error_count += 1
                continue
        
        # 커밋
        conn.commit()
        
        print(f"\n🎉 저장 완료!")
        print(f"   - 저장 성공: {saved_count}개")
        print(f"   - 건너뛴 항목: {skipped_count}개")
        print(f"   - 오류 발생: {error_count}개")
        print(f"   - 전체 처리율: {(saved_count/len(credit_cards)*100):.1f}%")

    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        if conn:
            conn.rollback()
    
    finally:
        # 리소스 정리
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("🔚 데이터베이스 연결 종료")

if __name__ == '__main__':
    run_credit_cards_benefit_crawler()