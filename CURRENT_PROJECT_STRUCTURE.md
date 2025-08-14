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

## 🔄 데이터 플로우

```
WebSocket API (업비트) → Kafka → Spark Streaming → PostgreSQL
                                     ↓
                               MinIO (Delta Lake)
                                     ↓
                               dbt 변환 → 최종 테이블
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

## 🎯 현재 구현 상태

### ✅ 완료된 기능
- 실시간 암호화폐 데이터 수집 (WebSocket → Kafka)
- 스파크 스트리밍 처리 (Bronze → Silver Layer)
- PostgreSQL 저장 및 MinIO Delta Lake 저장
- Airflow를 통한 파이프라인 오케스트레이션
- dbt를 통한 데이터 변환 (Silver → Gold Layer)
- **프로젝트 구조 체계화** (producer/processing/tests 분리)

### 🚧 진행 중
- GDELT 지정학 데이터 파이프라인으로 전환 예정
- 모니터링 시스템 (Grafana) 추가 예정
- 데이터 품질 검증 강화

### 🎯 **팀 역할 분담 (4명)**
- **PL/Architect (너)**: `processing/`, 인프라 관리, 모니터링
- **DE (동료)**: `producer/`, 데이터 수집 파이프라인
- **DA1**: `my_dbt_project/`, 데이터 모델링 및 변환
- **DA2**: 분석, 시각화, 데이터 검증

## 🔧 주요 설정 파일

- **docker-compose.yaml**: 전체 인프라 정의
- **dbt_project.yml**: dbt 프로젝트 설정
- **requirements.txt**: Python 패키지 의존성
- **CLAUDE.md**: AI 어시스턴트 컨텍스트

## 📝 개발 히스토리

1. **초기**: 파일 기반 암호화폐 데이터 파이프라인
2. **개선**: WebSocket 실시간 스트리밍으로 전환
3. **확장**: Kafka 스트레스 테스트 및 성능 최적화
4. **전환**: GDELT 지정학 리스크 분석 프로젝트로 피벗
5. **구조화**: 4인 팀 협업을 위한 구조 단순화
6. **현재**: src 폴더 체계화 (producer/processing/tests 분리) 완료

## 🎯 다음 단계

1. GDELT 데이터 수집 모듈 개발
2. 지정학 리스크 스코어링 모델 구현
3. Grafana 모니터링 대시보드 구축
4. 팀 협업 워크플로우 확립