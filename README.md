# 서울시 실시간 인구수에 따른 주요 장소 복잡도 현황파악
## 프로젝트 개요
### 내용
서울의 주요 장소 실시간 인구수와 예측 인구 현황을 시각화하여 혼잡 상태를 실시간으로 확인할 수 있도록 하는 프로젝트입니다.
### 기간
- 24/01/08(월) ~ 24/01/12(금)
### 팀원

| 이름   | 역할               |
| ------ | ------------------ |
| 정오영 | DW 구축, 관리|
| 이상평 | ETL, Airflow       |
| 이상진 | ETL, Airflow       |
| 조수현 | ETL, Airflow       |

### 기술 스택

언어
- Python, SQL
  
기술 및 프레임 워크
- API, Airflow, Docker, Amazon EC2, Superset, Snowflake

### 활용 데이터
∙ 서울 열린 데이터 광장 – 서울시 실시간 인구데이터
https://data.seoul.go.kr/dataList/OA-21778/A/1/datasetView.do

## 프로젝트 진행 과정
![구조](https://github.com/Seoul-Congestion-DE/airflow_dev/assets/88651495/40cea23b-353b-4539-aea1-3bf54284e471)
-AWS에서 EC2 인스턴스 사용
- Docker Container 내에 Airflow와 Superset 설정
- Airflow에서 OPEN-API DATA를 Extract, Transform, Load → Data Warehouse(Snowflake)에 적재
- Schema table을 참조하여 대시보드 시각화

## 프로젝트 결과 (일부 차트)
### 실시간 인구 지표 WORD CLOUD
![WORD CLOUD](https://github.com/Seoul-Congestion-DE/airflow_dev/assets/88651495/ba41c74d-60aa-403c-8b68-decf12ae949b)

### 예상 인구 지표 BoxFlot
![Boxflot](https://github.com/Seoul-Congestion-DE/airflow_dev/assets/88651495/9d924c4f-6001-4c8f-9d80-723e72ebd391)
