# 基本となるairflowイメージ
FROM apache/airflow:2.1.0

# 追加ライブラリのインストール
ADD requirements.txt /opt/airflow/
WORKDIR /opt/airflow/
RUN pip install -r requirements.txt