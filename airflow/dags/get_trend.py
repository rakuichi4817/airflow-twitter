from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from rakutwi import get_api, get_db, get_timestamp


default_args = {
    "owner": "Rakuichi",
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": True,
    "wait_for_downstream": True,
}


@dag(default_args=default_args, schedule_interval=timedelta(minutes=60), start_date=days_ago(0), tags=["example"], catchup=False)
def task_flow():
    
    @task(multiple_outputs=True)
    def get_trends():
        # apiインスタンスの取得
        api = get_api()
        # トレンドの取得
        trends = api.trends_place(23424856)[0]["trends"]
        timestamp = get_timestamp()

        return {"timestamp": timestamp, "trends": trends}
    
    @task()
    def insert_db(timestamp, trends):
        insert_record_lst = []
        for rank, tweet in enumerate(trends, 1):
            insert_record = (
                timestamp, rank, tweet["name"], tweet["tweet_volume"], tweet["url"])
            insert_record_lst.append(insert_record)
        # DBへの接続確保
        conn, cur = get_db()

        # トレンド情報の挿入
        cur.executemany("INSERT INTO trends VALUES (?, ?, ?, ?, ?)", insert_record_lst)
        conn.commit()
        
    result = get_trends()
    insert_db(result["timestamp"], result["trends"])
    
    
dag = task_flow()
