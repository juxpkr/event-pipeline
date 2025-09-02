# **🏗️ 지정학적 리스크 분석 플랫폼 - 전체 구조도**

## **📋 1. 프로젝트 개요**
- **목적**: GDELT & Wikimedia 데이터 기반 실시간 지정학적 리스크 분석
- **아키텍처**: 하이브리드 (배치 + 실시간 스트리밍)
- **기술 스택**: 완전 오픈소스 기반

---

## **🏛️ 2. 전체 서비스 아키텍처**

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Pipeline Architecture                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [Data Sources]          [Ingestion]          [Processing]      │
│  ┌─────────────┐         ┌─────────┐          ┌─────────────┐   │
│  │   GDELT     │────────▶│  Kafka  │─────────▶│   Spark     │   │
│  │ (15min 배치) │         │         │          │   Master    │   │
│  └─────────────┘         └─────────┘          │   Worker    │   │
│                                               └─────────────┘   │
│  ┌─────────────┐         ┌─────────┐          ┌─────────────┐   │
│  │ Wikimedia   │────────▶│  Kafka  │─────────▶│   Spark     │   │
│  │ (실시간 스트림)│         │         │          │ Streaming   │   │
│  └─────────────┘         └─────────┘          └─────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## **🐳 3. Docker Compose 서비스 구성**

### **3.1 메시징 & 스트리밍 레이어**
- **Zookeeper** (`2181`) - Kafka 클러스터 관리
- **Kafka** (`9092`) - 실시간 메시지 큐
- **Kafka-setup** - 토픽 자동 생성

### **3.2 컴퓨팅 & 처리 레이어**  
- **Spark Master** (`8081`, `7077`) - 분산 처리 마스터
- **Spark Worker** - 워커 노드
- **Spark Thrift Server** (`10001`) - dbt 연결용 SQL 엔드포인트

### **3.3 스토리지 & 메타데이터**
- **MinIO** (`9000`, `9001`) - S3 호환 오브젝트 스토리지
- **PostgreSQL** (`5432`) - 메타데이터 & 분석 DB
- **Hive Metastore** (`9083`) - 데이터 카탈로그

### **3.4 오케스트레이션**
- **Airflow Webserver** (`8082`) - 워크플로우 UI
- **Airflow Scheduler** - 스케줄링 엔진
- **Airflow Init** - 초기화

### **3.5 개발 & 분석**
- **Jupyter Lab** (`8888`) - 노트북 환경
- **dbt** - 데이터 변환 엔진
- **Superset** (`8088`) - 시각화 대시보드

### **3.6 모니터링 스택** 🆕
- **Prometheus** (`9090`) - 메트릭 수집
- **Grafana** (`3000`) - 모니터링 대시보드
- **Tools-downloader** - JMX 에이전트 다운로드

---

## **📁 4. 프로젝트 디렉토리 구조 (상세)**

```
event-pipeline/
├── 🔧 config/                              # 🎛️ 서비스별 설정 파일들
│   ├── airflow/
│   │   ├── airflow.env                     # Airflow 환경변수
│   │   └── init-airflow.sh                 # Airflow 초기화 스크립트
│   ├── dbt/
│   │   └── dbt.env                         # dbt 환경변수
│   ├── grafana/
│   │   └── grafana.env                     # Grafana 환경변수 🆕
│   ├── hive-metastore/
│   │   └── hive-metastore.env              # Hive Metastore 환경변수
│   ├── jupyter/
│   │   └── jupyter.env                     # Jupyter Lab 환경변수
│   ├── kafka/
│   │   ├── kafka.env                       # Kafka 환경변수
│   │   └── setup-topics.sh                 # Kafka 토픽 생성 스크립트
│   ├── minio/
│   │   ├── minio.env                       # MinIO 환경변수
│   │   └── setup-minio.sh                  # MinIO 버킷 생성 스크립트
│   ├── postgres/
│   │   ├── postgres.env                    # PostgreSQL 환경변수
│   │   ├── healthcheck.sh                  # PostgreSQL 헬스체크
│   │   └── init-postgres.sh                # PostgreSQL 초기화
│   ├── spark/
│   │   └── spark.env                       # Spark 클러스터 환경변수
│   ├── superset/
│   │   ├── superset.env                    # Superset 환경변수
│   │   └── init-superset.sh                # Superset 초기화 스크립트
│   ├── tools/
│   │   └── download-jmx.sh                 # JMX Prometheus 에이전트 다운로드
│   └── zookeeper/
│       └── zookeeper.env                   # Zookeeper 환경변수
│
├── 📊 dags/                                # 🔄 Airflow DAG 파이프라인
│   ├── gdelt_etl_dag.py                    # GDELT ETL 파이프라인 (구 버전)
│   ├── gdelt_pipeline_dag.py               # GDELT 메인 파이프라인
│   └── gold_to_postgresql_migration_dag.py # Gold → PostgreSQL 마이그레이션
│
├── 💾 src/                                 # 🐍 Python 소스 코드
│   ├── ingestion/                          # 데이터 수집 모듈
│   │   └── gdelt/
│   │       ├── gdelt_producer.py           # GDELT 데이터 Kafka Producer
│   │       ├── gdelt_raw_producer.py       # GDELT Raw 데이터 Producer
│   │       └── producer_gdelt_microbatch.py # GDELT 마이크로배치 Producer
│   ├── processing/                         # 데이터 처리 모듈
│   │   ├── batch/
│   │   │   ├── gdelt_silver_processor.py   # GDELT Silver Layer 처리
│   │   │   └── process_gdelt_data.py       # GDELT 데이터 처리 메인
│   │   └── migration/
│   │       └── gold-to-postgresql-migration.py # Gold → PostgreSQL 마이그레이션
│   ├── tests/
│   │   └── test_wiki_stream.py             # Wikimedia 스트림 테스트
│   └── utils/
│       └── spark_builder.py                # Spark 세션 빌더 유틸리티
│
├── 🔄 transforms/                          # 📊 dbt 데이터 변환 프로젝트
│   ├── .dbt/
│   │   ├── .user.yml                       # dbt 사용자 설정
│   │   └── profiles.yml                    # dbt 프로파일 설정
│   ├── models/                             # dbt 모델들
│   │   ├── staging/                        # 🥈 Silver Layer (정제된 데이터)
│   │   │   ├── sources.yml                 # 데이터 소스 정의
│   │   │   └── stg_gdelt_microbatch_events.sql # GDELT 이벤트 스테이징
│   │   └── marts/                          # 🥇 Gold Layer (비즈니스 로직)
│   │       ├── schema.yml                  # 마트 스키마 정의
│   │       └── gdelt_microbatch_country_analysis.sql # 국가별 분석 마트
│   ├── macros/
│   │   └── get_custom_schema.sql           # dbt 커스텀 매크로
│   ├── target/                             # dbt 컴파일 결과물
│   │   ├── compiled/                       # 컴파일된 SQL
│   │   ├── run/                           # 실행 결과
│   │   ├── manifest.json                   # dbt 메니페스트
│   │   └── run_results.json               # 실행 결과 로그
│   ├── dbt_project.yml                     # dbt 프로젝트 설정
│   └── Dockerfile                          # dbt 컨테이너 이미지
│
├── 📈 monitoring/                          # 📊 모니터링 설정 🆕
│   ├── prometheus/
│   │   └── prometheus.yml                  # Prometheus 설정
│   └── grafana/                           # Grafana 대시보드 설정
│
├── 🚀 spark-base/                         # ⚡ Spark 커스텀 이미지
│   ├── Dockerfile                          # Spark 베이스 이미지
│   ├── entrypoint.sh                       # Spark 엔트리포인트
│   ├── hive-site.xml                       # Hive 설정
│   └── spark-defaults.conf                 # Spark 기본 설정
│
├── 🗂️ hive/                               # 🏪 Hive Metastore 설정
│   ├── Dockerfile                          # Hive Metastore 이미지
│   ├── entrypoint.sh                       # Hive 엔트리포인트
│   ├── core-site.xml                       # Hadoop Core 설정
│   └── hive-site.xml                       # Hive 설정
│
├── 🏗️ airflow/                           # ✈️ Airflow 커스텀 이미지
│   └── Dockerfile                          # Airflow 이미지
│
├── 📝 notebooks/                           # 📖 Jupyter 노트북
│   ├── gold_gdelt_test.ipynb              # Gold Layer 테스트
│   ├── simple_gdelt_test.ipynb            # GDELT 단순 테스트
│   └── untitled.ipynb                      # 기타 노트북
│
├── 🎛️ superset/                           # 📊 Superset 설정
│   └── superset_config.py                  # Superset Python 설정
│
├── 📝 scripts/                             # 🔧 유틸리티 스크립트
│   └── dbt_health_check.py                 # dbt 헬스체크 스크립트
│
├── 📁 logs/                                # 📋 Airflow 로그 디렉토리
│   ├── dag_id=gdelt_pipeline_full/         # GDELT 파이프라인 로그
│   ├── dag_id=gold_to_postgresql_migration/ # 마이그레이션 로그
│   ├── dag_processor_manager/              # DAG 프로세서 로그
│   └── scheduler/                          # 스케줄러 로그
│
├── ⚙️ 설정 파일들
│   ├── .env                               # 환경변수 메인 파일
│   ├── docker-compose.yaml                # 🐳 인프라 정의 (메인)
│   ├── Dockerfile                         # Jupyter Lab 이미지
│   ├── requirements.txt                   # Python 의존성
│   └── .github/workflows/ci.yml           # GitHub Actions CI/CD
│
└── 📚 문서화
    ├── structure.md                       # 프로젝트 구조도 (현재 파일)
    └── README.md                          # 프로젝트 README
```

---

## **🔄 5. 데이터 플로우**

```
GDELT (배치) ────┐
                ├──▶ Kafka ──▶ Spark ──▶ Silver Layer (MinIO)
Wikimedia (실시간)─┘                        │
                                           ▼
                                      dbt Transform
                                           │
                                           ▼
                                     Gold Layer (MinIO)
                                           │
                                           ▼
                              PostgreSQL ◄─┘ (migration)
                                           │
                                           ▼
                                    Superset/Grafana
```

---

## **🎯 6. 핵심 기능별 매핑**

| **기능** | **담당 서비스** | **포트** | **역할** |
|----------|----------------|----------|----------|
| 데이터 수집 | Kafka | 9092 | GDELT/Wikimedia 스트림 |
| 배치 처리 | Spark Master/Worker | 8081, 7077 | 대용량 데이터 처리 |
| 실시간 처리 | Spark Streaming | - | 실시간 이벤트 처리 |
| 데이터 변환 | dbt + Thrift Server | 10001 | Silver→Gold 변환 |
| 워크플로우 | Airflow | 8082 | 파이프라인 스케줄링 |
| 시각화 | Superset | 8088 | 대시보드 |
| 모니터링 | Grafana + Prometheus | 3000, 9090 | 시스템 모니터링 🆕 |
| 개발환경 | Jupyter | 8888 | EDA & 프로토타이핑 |

---

## **🚀 7. 핵심 특징**

### **7.1 기술적 혁신**
- **완전한 오픈소스 스택** - 벤더 락인 해결
- **하이브리드 아키텍처** - 실시간 + 배치 처리 동시 지원
- **Medallion 아키텍처** - Bronze/Silver/Gold 레이어 완벽 구현
- **완전한 모니터링** - Prometheus + Grafana 스택
- **프로덕션 레디** - Airflow 기반 완전 자동화

### **7.2 데이터 레이어**
- **Bronze Layer**: 원시 데이터 (Kafka Topics)
- **Silver Layer**: 정제된 데이터 (Spark Processing → MinIO)
- **Gold Layer**: 비즈니스 로직 적용 (dbt Transform → MinIO)
- **Serving Layer**: 분석용 데이터 (PostgreSQL Migration)

### **7.3 확장성**
- **수평적 확장**: Spark Worker 노드 추가 가능
- **수직적 확장**: 각 서비스별 리소스 조정 가능
- **모듈화**: 서비스별 독립적 배포 및 업데이트 가능