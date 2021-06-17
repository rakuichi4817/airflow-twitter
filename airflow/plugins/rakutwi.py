import os
import json
import sqlite3
from datetime import datetime

import tweepy

CURRENTPATH = "/opt/airflow"
# デフォルトのファイルパス
default_key_fpath = os.path.join(CURRENTPATH, "user_keys" ,"twitter_api_key.json")
db_path = os.path.join(CURRENTPATH, "rakutwi.db")

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_db(dbname=db_path):
    # コネクションの確立
    conn = sqlite3.connect(dbname)
    # カーソルオブジェクトを作成
    cur = conn.cursor()

    return (conn, cur)


def read_json(fpath=default_key_fpath):
    """jsonファイルの読み込み

    Args:
        fpath (str, optional): 読み込むjsonファイルパス. デフォルトはinstance/twitter_api_key.json.

    Returns:
        dict: 各キー情報
    """
    with open(fpath, "r", encoding="utf-8")as fobj:
        return json.load(fobj)


def get_api(api_keys=None):
    """tweepyのAPIインスタンスを取得する

    Args:
        api_keys (dict, optional): 各キー情報が入った辞書型. デフォルトはNone.

    Returns:
        tweepy.api.API: apiインスンタンス
    """
    # 引数で指定されていなければデフォルト読み込み
    if api_keys is None:
        api_keys = read_json()

    # 各設定の読み込み
    CONSUMER_KEY = api_keys["API Key"]
    CONSUMER_SECRET = api_keys["API Secret Key"]
    ACCESS_TOKEN = api_keys["Access Token"]
    ACCESS_TOKEN_SECRET = api_keys["Access Token Secret"]

    # APIアクセス
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    return api
