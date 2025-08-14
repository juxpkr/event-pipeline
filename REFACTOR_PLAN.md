# 프로젝트 구조 개선 계획

## 목표 구조 (GDELT 프로젝트용)

```
de-pipeline-template/
├── 📁 config/                     # 설정 파일들
│   ├── dev.yaml
│   ├── staging.yaml  
│   └── prod.yaml
├── 📁 infrastructure/             # 인프라 관련
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── k8s/ (향후)
├── 📁 ingestion/                  # 데이터 수집
│   ├── gdelt/
│   │   ├── gdelt_producer.py
│   │   └── config/
│   ├── crypto/ (기존)
│   │   ├── crypto_producer.py
│   │   └── websocket_producer.py
│   └── utils/
├── 📁 processing/                 # 데이터 처리
│   ├── streaming/
│   │   ├── gdelt_stream_processor.py
│   │   └── crypto_stream_processor.py
│   ├── batch/
│   │   └── gdelt_batch_processor.py
│   └── utils/
├── 📁 transformation/             # dbt 프로젝트
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_gdelt_events.sql
│   │   │   └── stg_crypto_data.sql
│   │   ├── intermediate/
│   │   └── marts/
│   │       ├── geopolitical_risk.sql
│   │       └── crypto_analytics.sql
│   ├── macros/
│   └── tests/
├── 📁 orchestration/              # Airflow
│   ├── dags/
│   │   ├── gdelt_pipeline_dag.py
│   │   └── crypto_pipeline_dag.py
│   └── plugins/
├── 📁 monitoring/                 # 모니터링 (향후)
│   ├── grafana/
│   └── alerts/
├── 📁 analysis/                   # 데이터 분석
│   ├── notebooks/
│   ├── risk_models/
│   └── visualization/
├── 📁 scripts/                    # 유틸리티 스크립트
│   ├── setup.sh
│   └── run_spark.sh
├── 📁 tests/                      # 테스트
│   ├── unit/
│   └── integration/
└── 📁 docs/                       # 문서
    ├── setup.md
    └── team-guide.md
```

## 단계별 마이그레이션

### Phase 1: 디렉토리 생성 및 기본 파일 이동
- [ ] 새 디렉토리 구조 생성
- [ ] 기존 파일들 적절한 위치로 이동
- [ ] docker-compose.yaml 경로 수정

### Phase 2: 설정 파일 분리
- [ ] 하드코딩된 설정을 config/ 파일로 이동
- [ ] 환경별 설정 분리 (dev/test/prod)

### Phase 3: GDELT 도메인 추가
- [ ] GDELT 관련 새 모듈 추가
- [ ] 기존 crypto 모듈과 분리

### Phase 4: 분석 환경 구축
- [ ] Jupyter notebook 환경 설정
- [ ] 분석가들을 위한 analysis/ 구조 정리
- [ ] 공통 유틸리티 정리