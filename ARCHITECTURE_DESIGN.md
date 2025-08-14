# GDELT 지정학 리스크 파이프라인 아키텍처

## 🏗️ 전체 시스템 아키텍처

### 데이터 플로우
```
GDELT API → Ingestion Service → Kafka → Spark Streaming → PostgreSQL (Silver)
                                                               ↓
PostgreSQL (Gold) ← dbt Transformation ← Silver Layer Data
                ↓
    Grafana Dashboard (실시간 모니터링)
```

## 📁 최종 파일 구조

```
gdelt-geopolitical-risk-pipeline/
├── 📁 config/                           # 환경별 설정
│   ├── dev.yaml                         # 로컬 개발환경
│   ├── staging.yaml                     # 통합 테스트환경  
│   ├── prod.yaml                        # 운영환경 (미래)
│   └── database.yaml                    # DB 연결 정보
│
├── 📁 infrastructure/                   # 인프라 관리
│   ├── docker/
│   │   ├── docker-compose.yaml          # 전체 서비스 정의
│   │   ├── docker-compose.dev.yaml      # 개발환경 오버라이드
│   │   └── Dockerfile                   # 커스텀 이미지
│   └── k8s/                            # Kubernetes 배포 (Phase 3)
│       ├── deployments/
│       └── services/
│
├── 📁 ingestion/                        # 데이터 수집 계층
│   ├── gdelt/                          # GDELT 데이터 수집
│   │   ├── __init__.py
│   │   ├── gdelt_producer.py           # GDELT API → Kafka
│   │   ├── gdelt_client.py             # GDELT API 클라이언트
│   │   ├── config/
│   │   │   ├── gdelt_config.yaml
│   │   │   └── country_mapping.json
│   │   └── utils/
│   │       ├── data_validator.py
│   │       └── error_handler.py
│   ├── crypto/                         # 기존 암호화폐 (학습용)
│   │   ├── crypto_producer.py
│   │   └── websocket_producer.py
│   └── common/                         # 공통 유틸리티
│       ├── kafka_client.py
│       └── monitoring.py
│
├── 📁 processing/                       # 데이터 처리 계층  
│   ├── streaming/                      # 실시간 스트림 처리
│   │   ├── __init__.py
│   │   ├── gdelt_stream_processor.py   # GDELT 스트리밍 처리
│   │   ├── crypto_stream_processor.py  # 암호화폐 스트리밍
│   │   └── base_processor.py           # 공통 처리 로직
│   ├── batch/                          # 배치 처리
│   │   ├── gdelt_batch_processor.py    # 대용량 히스토리 데이터
│   │   └── data_backfill.py            # 누락 데이터 보완
│   ├── schemas/                        # 데이터 스키마 정의
│   │   ├── gdelt_schema.py
│   │   └── common_types.py
│   └── utils/
│       ├── spark_helpers.py
│       └── data_quality.py
│
├── 📁 transformation/                   # dbt 데이터 변환
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/                    # Silver Layer (원본 정제)
│   │   │   ├── stg_gdelt_events.sql    # GDELT 이벤트 정제
│   │   │   ├── stg_gdelt_actors.sql    # 행위자 정보 정제
│   │   │   ├── stg_gdelt_locations.sql # 지리정보 정제
│   │   │   └── schema.yml
│   │   ├── intermediate/               # 중간 변환 단계
│   │   │   ├── int_risk_calculations.sql
│   │   │   └── int_geospatial_agg.sql
│   │   └── marts/                      # Gold Layer (비즈니스 로직)
│   │       ├── geopolitical_risk/      # 지정학 리스크 마트
│   │       │   ├── country_risk_scores.sql
│   │       │   ├── regional_tensions.sql
│   │       │   ├── conflict_indicators.sql
│   │       │   └── schema.yml
│   │       └── monitoring/             # 모니터링 마트
│   │           ├── pipeline_metrics.sql
│   │           └── data_quality_checks.sql
│   ├── macros/                         # dbt 매크로
│   │   ├── risk_scoring.sql
│   │   └── geo_functions.sql
│   ├── tests/                          # 데이터 품질 테스트
│   └── docs/                           # dbt 문서
│
├── 📁 orchestration/                    # 워크플로우 오케스트레이션
│   ├── dags/                           # Airflow DAG
│   │   ├── __init__.py
│   │   ├── gdelt_daily_pipeline.py     # 일일 배치 파이프라인
│   │   ├── gdelt_streaming_dag.py      # 스트리밍 파이프라인 관리
│   │   ├── data_quality_dag.py         # 데이터 품질 모니터링
│   │   └── backfill_dag.py             # 과거 데이터 보완
│   ├── plugins/                        # Airflow 플러그인
│   │   ├── gdelt_operators.py
│   │   └── monitoring_hooks.py
│   └── config/
│       └── airflow_config.py
│
├── 📁 monitoring/                       # 모니터링 설정
│   ├── grafana/                        # 대시보드 정의
│   │   ├── dashboards/
│   │   │   ├── geopolitical-overview.json
│   │   │   ├── pipeline-health.json
│   │   │   └── data-quality.json
│   │   ├── datasources/
│   │   │   └── postgresql.yaml
│   │   └── provisioning/
│   ├── prometheus/                     # (Phase 2에서 추가)
│   │   ├── prometheus.yml
│   │   └── rules/
│   └── alerts/
│       ├── business_alerts.yaml       # 비즈니스 알람 (위험도 임계값)
│       └── system_alerts.yaml         # 시스템 알람
│
├── 📁 analysis/                         # 데이터 분석 (DA들 작업공간)
│   ├── notebooks/                      # Jupyter 노트북
│   │   ├── exploratory/                # EDA
│   │   │   ├── gdelt_data_exploration.ipynb
│   │   │   └── risk_model_analysis.ipynb
│   │   └── validation/                 # 데이터 검증
│   │       └── data_quality_validation.ipynb
│   ├── models/                         # 분석 모델
│   │   ├── risk_scoring/
│   │   │   ├── goldstein_analyzer.py
│   │   │   └── cameo_classifier.py
│   │   └── forecasting/
│   │       └── tension_predictor.py
│   ├── reports/                        # 분석 리포트
│   │   ├── weekly_risk_assessment.md
│   │   └── model_performance.md
│   └── visualization/                  # 시각화 코드
│       ├── risk_heatmap.py
│       └── trend_analysis.py
│
├── 📁 scripts/                          # 운영 스크립트
│   ├── setup/                          # 환경 설정
│   │   ├── local_setup.sh              # 로컬 개발환경 설정
│   │   ├── docker_setup.sh             # Docker 환경 초기화
│   │   └── database_init.sql           # DB 초기 스키마
│   ├── deployment/                     # 배포 스크립트
│   │   ├── deploy_dev.sh
│   │   └── deploy_staging.sh
│   ├── maintenance/                    # 유지보수
│   │   ├── cleanup_old_data.py
│   │   └── reprocess_failed_batches.py
│   └── utilities/                      # 유틸리티
│       ├── run_spark_local.sh
│       └── kafka_topic_manager.py
│
├── 📁 tests/                            # 테스트 코드
│   ├── unit/                           # 단위 테스트
│   │   ├── test_ingestion/
│   │   ├── test_processing/
│   │   └── test_transformation/
│   ├── integration/                    # 통합 테스트
│   │   ├── test_e2e_pipeline.py
│   │   └── test_data_quality.py
│   └── fixtures/                       # 테스트 데이터
│       ├── sample_gdelt_data.json
│       └── expected_outputs.json
│
├── 📁 docs/                             # 프로젝트 문서
│   ├── architecture/
│   │   ├── system_design.md
│   │   ├── data_flow.md
│   │   └── scaling_strategy.md
│   ├── setup/
│   │   ├── local_development.md
│   │   ├── team_onboarding.md
│   │   └── deployment_guide.md
│   ├── api/
│   │   ├── gdelt_api_reference.md
│   │   └── internal_apis.md
│   └── troubleshooting/
│       ├── common_issues.md
│       └── performance_tuning.md
│
├── 📄 .env.example                      # 환경변수 템플릿
├── 📄 .gitignore                        # Git 제외 파일
├── 📄 README.md                         # 프로젝트 개요
├── 📄 requirements.txt                  # Python 의존성
├── 📄 docker-compose.yaml               # 메인 Docker Compose
├── 📄 Makefile                         # 편의 명령어
└── 📄 CLAUDE.md                        # Claude 컨텍스트 파일
```

## 🔄 데이터 레이어 구조

### Bronze Layer (Raw Data)
- **위치**: Kafka Topics
- **책임**: 원본 데이터 수집 및 임시 저장
- **형식**: JSON (GDELT API 원본 형태)

### Silver Layer (Cleaned Data)  
- **위치**: PostgreSQL `silver_` 테이블들
- **책임**: 데이터 정제, 스키마 표준화
- **처리**: Spark Streaming → PostgreSQL

### Gold Layer (Business Data)
- **위치**: PostgreSQL `gold_` 테이블들  
- **책임**: 비즈니스 로직, 집계, 지표 계산
- **처리**: dbt 변환 → PostgreSQL

## 👥 팀별 작업 영역

### 너 (PL + 아키텍트)
- `infrastructure/`, `monitoring/`, `scripts/`
- `orchestration/` (전체 파이프라인 조율)
- `config/` (환경 설정 관리)

### DE (동료)  
- `ingestion/gdelt/`, `processing/`
- `transformation/models/staging/`
- Silver Layer 전체 담당

### DA1 (데이터 모델링)
- `transformation/models/marts/`
- `transformation/macros/`, `transformation/tests/`
- Gold Layer 비즈니스 로직

### DA2 (분석 & 시각화)
- `analysis/`, `monitoring/grafana/dashboards/`
- 데이터 검증 및 인사이트 도출