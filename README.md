# Clobee 배치

FastAPI를 바탕으로 일정 시간마다 실행되는 배치 목록입니다.

| 파일명                   | 설명                            | FastAPI URL                |
|------------------------|--------------------------------|----------------------------|
| `crawling_card_events` | 카드사 이벤트 내역을 가져옵니다. | `/cardEventsInfo`          |
| `crawling_check_cards` | 체크카드 정보를 불러옵니다.     | `/checkCardInfo`           |
| `crawling_credit_cards`| 신용카드 정보를 불러옵니다.     | `/creditCardInfo`          |