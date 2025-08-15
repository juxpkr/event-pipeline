# 현재 프로젝트 구조 (2025.08.14)

## 📁 전체 디렉토리 구조

```
de-pipeline-template/
├── 📁 dags/                          # Airflow DAG 파일들
│   ├── hello_airflow_dag.py          # 테스트용 DAG
│   └── spark_pipeline_dag.py         # 스파크 파이프라인 DAG
│
├── 📁 data/                          # 샘플 데이터
│   └── crypto_stream_data_sample.jsonl
│
├── 📁 my_dbt_project/               # dbt 데이터 변환 프로젝트
│   ├── models/
│   │   ├── staging/                 # 원본 데이터 정제
│   │   │   └── sources.yml          # 소스 테이블 정의
│   │   └── marts/                   # 최종 비즈니스 테이블
│   │       ├── crypto_daily_summary.sql
│   │       └── schema.yml
│   ├── dbt_project.yml             # dbt 프로젝트 설정
│   └── README.md
│
├── 📁 src/                          # 소스 코드 모음
│   ├── producer/                    # 📡 데이터 수집 (카프카 프로듀서)
│   │   ├── producer.py              # 실시간 암호화폐 데이터 수집
│   │   ├── sample_data.json         # 샘플 데이터
│   │   ├── test_crypto_api.py       # API 테스트
│   │   └── test_crypto_websocket.py # 웹소켓 테스트
│   ├── processing/                  # ⚙️ 데이터 처리 (스파크)
│   │   ├── spark_processor.py       # 스파크 스트리밍 처리 (기존 build_travel_datamart.py)
│   │   └── data_validator.py        # 데이터 검증 (기존 verify_datamart.py)
│   └── tests/                       # 🧪 테스트 파일들
│       └── test_wiki_stream.py      # 위키피디아 스트림 테스트
│
├── 📁 logs/                         # Airflow 실행 로그들 (gitignore)
├── 📁 plugins/                      # Airflow 플러그인 (빈 폴더)
├── 📁 postgres-data/                # PostgreSQL 데이터베이스 파일들 (gitignore)
├── 📁 spark-ivy-cache/              # Spark 의존성 캐시 (gitignore)
│
├── 📄 docker-compose.yaml           # 전체 서비스 도커 설정
├── 📄 Dockerfile                    # Airflow 커스텀 이미지
├── 📄 run_spark.sh                  # 스파크 실행 스크립트
├── 📄 requirements.txt              # Python 의존성
│
├── 📄 .dockerignore                 # Docker 빌드 제외 파일
├── 📄 .gitignore                    # Git 제외 파일 (업데이트됨)
├── 📄 README.md                     # 프로젝트 개요
├── 📄 CLAUDE.md                     # Claude 컨텍스트 파일
├── 📄 ARCHITECTURE_DESIGN.md        # 아키텍처 설계 문서
├── 📄 REFACTOR_PLAN.md              # 리팩토링 계획
└── 📄 CURRENT_PROJECT_STRUCTURE.md  # 현재 프로젝트 구조 (이 파일)
```

## 🐳 Docker 서비스 구성

```yaml
services:
  - zookeeper          # 카프카 코디네이터
  - kafka             # 메시지 스트리밍
  - minio             # S3 호환 객체 저장소
  - mc                # MinIO 클라이언트
  - spark-master      # 스파크 마스터 노드
  - spark-worker      # 스파크 워커 노드
  - postgres          # 메타데이터 및 최종 데이터
  - airflow-init      # Airflow 초기화
  - airflow-webserver # Airflow 웹 UI
  - airflow-scheduler # Airflow 스케줄러
```


### 🎯 **팀 역할 분담 **

- **DE1**: `processing/`, 인프라 관리, 모니터링
- **DE2**: `producer/`, 데이터 수집 파이프라인
- **DA1**: `my_dbt_project/`, 데이터 모델링 및 변환
- **DA2**: 분석, 시각화, 데이터 검증

## 🔧 주요 설정 파일

- **docker-compose.yaml**: 전체 인프라 정의
- **dbt_project.yml**: dbt 프로젝트 설정
- **requirements.txt**: Python 패키지 의존성
