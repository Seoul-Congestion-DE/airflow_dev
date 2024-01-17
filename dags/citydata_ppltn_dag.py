from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from contextlib import closing
import pendulum
import requests
import json
import os
import logging
from dotenv import load_dotenv
from airflow.operators.dagrun_operator import TriggerDagRunOperator

load_dotenv()  # .env 파일 로드

seoul_api_key = os.getenv('SEOUL_API_KEY')

# Snowflake 연결 ID를 지정합니다.
SNOWFLAKE_CONN_ID = 'snowflake'

# 지역 리스트
locations = [
    "강남 MICE 관광특구",
    "동대문 관광특구",
    "명동 관광특구",
    "이태원 관광특구",
    "잠실 관광특구",
    "종로·청계 관광특구",
    "홍대 관광특구",
    "경복궁",
    "광화문·덕수궁",
    "보신각",
    "서울 암사동 유적",
    "창덕궁·종묘",
    "가산디지털단지역",
    "강남역",
    "건대입구역",
    "고덕역",
    "고속터미널역",
    "교대역",
    "구로디지털단지역",
    "구로역",
    "군자역",
    "남구로역",
    "대림역",
    "동대문역",
    "뚝섬역",
    "미아사거리역",
    "발산역",
    "북한산우이역",
    "사당역",
    "삼각지역",
    "서울대입구역",
    "서울식물원·마곡나루역",
    "서울역",
    "선릉역",
    "성신여대입구역",
    "수유역",
    "신논현역·논현역",
    "신도림역",
    "신림역",
    "신촌·이대역",
    "양재역",
    "역삼역",
    "연신내역",
    "오목교역·목동운동장",
    "왕십리역",
    "용산역",
    "이태원역",
    "장지역",
    "장한평역",
    "천호역",
    "총신대입구(이수)역",
    "충정로역",
    "합정역",
    "혜화역",
    "홍대입구역 9번 출구",
    "회기역",
    "4·19 카페거리",
    "가락시장",
    "가로수길",
    "광장(전통)시장",
    "김포공항",
    "낙산공원·이화마을",
    "노량진",
    "덕수궁길·정동길",
    "방배역 먹자골목",
    "북촌한옥마을",
    "서촌",
    "성수카페거리",
    "수유리 먹자골목",
    "쌍문동 맛집거리",
    "압구정로데오거리",
    "여의도",
    "연남동",
    "영등포 타임스퀘어",
    "외대앞",
    "용리단길",
    "이태원 앤틱가구거리",
    "인사동·익선동",
    "창동 신경제 중심지",
    "청담동 명품거리",
    "청량리 제기동 일대 전통시장",
    "해방촌·경리단길",
    "DDP(동대문디자인플라자)",
    "DMC(디지털미디어시티)",
    "강서한강공원",
    "고척돔",
    "광나루한강공원",
    "광화문광장",
    "국립중앙박물관·용산가족공원",
    "난지한강공원",
    "남산공원",
    "노들섬",
    "뚝섬한강공원",
    "망원한강공원",
    "반포한강공원",
    "북서울꿈의숲",
    "불광천",
    "서리풀공원·몽마르뜨공원",
    "서울대공원",
    "서울숲공원",
    "시청광장",
    "아차산",
    "양화한강공원",
    "어린이대공원",
    "여의도한강공원",
    "월드컵공원",
    "응봉산",
    "이촌한강공원",
    "잠실종합운동장",
    "잠실한강공원",
    "잠원한강공원",
    "청계산",
    "청와대"
]

@task
def extract():
    responses = []
    try:
        for location in locations:
            url = f'http://openapi.seoul.go.kr:8088/{seoul_api_key}/json/citydata_ppltn/1/5/{location}'
            response = requests.get(url)
            responses.append(response.content.decode('utf-8'))
    except Exception as e:
        logging.error(f"Error occurred while extracting data: {e}")
        raise
    else:
        logging.info(f'Responses: {responses}')
        return responses
    
@task
def transform(json_strs):
    data_list = []
    try:
        for json_str in json_strs:
            data = json.loads(json_str)
            try:
                records = list(data['SeoulRtd.citydata_ppltn'][0].values())
            except KeyError:
                logging.error("'SeoulRtd.citydata_ppltn' key not found in data", data)
                continue
            is_prediction = True if records[-2] == "Y" else False
            data_list.append({'records': records, 'is_prediction': is_prediction})
    except Exception as e:
        logging.error(f"Error occurred while transforming data: {e}")
        raise
    else:
        return data_list

@task
def load(table, pre_table, data_list):
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        with closing(conn.cursor()) as cur:
            try:
                # Full refresh by deleting existing data
                cur.execute(f"DELETE FROM DEV.RAW_DATA.{table};")
                cur.execute(f"DELETE FROM DEV.RAW_DATA.{pre_table};")
                
                # Start the transaction
                cur.execute("BEGIN;")
                
                for data in data_list:
                    records = data['records']
                    is_prediction = data['is_prediction']
                    
                    values = ', '.join(f"'{record}'" if isinstance(record, str) else str(record) for record in records[:-1])
                    if is_prediction:
                        for p in records[-1]:
                            area_code = f"'{records[0]}', "
                            pre_values = area_code + ', '.join(f"'{value}'" if isinstance(value, str) else str(value) for value in p.values())
                            sql = f"INSERT INTO DEV.RAW_DATA.{pre_table} VALUES ({pre_values})"
                            cur.execute(sql)
                    sql = f"INSERT INTO DEV.RAW_DATA.{table} VALUES ({values})"
                    cur.execute(sql)
                    
                # Commit the transaction
                cur.execute("COMMIT;")
            except Exception as e:
                logging.error(f"Error occurred while loading data to {table}", e)
                cur.execute("ROLLBACK;")
                raise
    except Exception as e:
        logging.error(f"Error occurred while connecting to Snowflake: {e}")
        raise

with DAG(
    dag_id='citydata_ppltn_v1',
    start_date=pendulum.yesterday(),
    # Schedule to run every hour
    schedule_interval= None,
) as dag:
    
    # 직접 작성
    table_name = 'CityData'
    pre_table_name = 'ForecastData'

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load_data = load(table_name, pre_table_name, transformed_data)

    extracted_data >> transformed_data >> load_data

    trigger_dag1 = TriggerDagRunOperator(
        task_id='trigger_dag1',
        trigger_dag_id="extract_and_append_to_existing_table",
        dag=dag
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_dag2',
        trigger_dag_id="congestion_analytics_dag",
        dag=dag
    )

    load_data >> trigger_dag1
    load_data >> trigger_dag2