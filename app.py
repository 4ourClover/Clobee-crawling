# app.py
from fastapi import FastAPI
from card_info_crawling.crawling_check_cards import run_check_cards_crawler
from card_info_crawling.crawling_credit_cards import run_credit_cards_crawler
from event_info_card.crawling_card_events import card_events_crawler

app = FastAPI()

@app.get("/checkCardInfo")
def checkCardInfo():
    result = run_check_cards_crawler()
    return {"status": "success", "result": result}

@app.get("/creditCardInfo")
def creditCardInfo():
    result = run_credit_cards_crawler()
    return {"status": "success", "result": result}

@app.get("/cardEventsInfo")
def cardEvents():
    result = card_events_crawler()
    return {"status": "success", "result": result}
