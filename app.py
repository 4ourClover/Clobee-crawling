from fastapi import FastAPI
import requests

app = FastAPI()

# Airflow API endpoint
AIRFLOW_API = "http://localhost:8080/api/v1"
AIRFLOW_AUTH = ("admin", "admin")  # 기본 admin 계정, 필요 시 환경변수로 분리

@app.get("/")
def hello():
    return {"message": "hi"}

@app.post("/run-dag/{dag_id}")
def run_dag(dag_id: str):
    response = requests.post(
        f"{AIRFLOW_API}/dags/{dag_id}/dagRuns",
        auth=AIRFLOW_AUTH,
        json={}
    )
    return {
        "status": "triggered" if response.status_code == 200 else "failed",
        "details": response.json()
    }

@app.get("/dag-status/{dag_id}/{dag_run_id}")
def dag_status(dag_id: str, dag_run_id: str):
    response = requests.get(
        f"{AIRFLOW_API}/dags/{dag_id}/dagRuns/{dag_run_id}",
        auth=AIRFLOW_AUTH
    )
    return response.json()
