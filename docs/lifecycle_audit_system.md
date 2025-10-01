# GDELT Event Lifecycle 감사 시스템

## 📋 개요

기존 복잡한 4점 감사 시스템을 **실제 이벤트 생명주기 추적**으로 혁신한 데이터 품질 보장 시스템입니다.

### 핵심 아이디어
- **각 event_id의 실제 여정 추적**: Bronze → Silver → Gold → Postgres
- **Delta Lake 기반 lifecycle 테이블**: 실시간 상태 관리
- **정확한 조인 성공률 측정**: 15시간 순회 조인 로직의 실제 성과 측정
- **자동 데이터 유실 탐지**: 24시간 이상 대기 이벤트 자동 만료

## 🏗️ 시스템 아키텍처

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Bronze    │    │   Silver    │    │    Gold     │    │ PostgreSQL  │
│   Layer     │    │   Layer     │    │   Layer     │    │             │
└─────┬───────┘    └─────┬───────┘    └─────┬───────┘    └─────────────┘
      │                  │                  │
      ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  Event Lifecycle Delta Table                           │
│  global_event_id | bronze_arrival | detailed_joined | status | batch   │
│  ─────────────────────────────────────────────────────────────────────  │
│  20241201001     | 2024-12-01 15:00| 2024-12-01 18:30| JOINED | b123   │
│  20241201002     | 2024-12-01 15:00| NULL            | WAITING| b123   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                            ┌─────────────────┐
                            │ LifecycleAuditor│
                            │   감사 시스템    │
                            └─────────────────┘
                                    │
                                    ▼
                            ┌─────────────────┐
                            │   Prometheus    │
                            │   & Grafana     │
                            └─────────────────┘
```

## 📁 새로 추가된 파일들

### 1. **핵심 컴포넌트들**

#### 📄 `src/audit/lifecycle_tracker.py`
**역할**: Event Lifecycle 데이터 관리 (CRUD)
- Delta Lake 테이블 초기화
- Bronze 도착 이벤트 기록 (WAITING 상태)
- 조인 성공 시 상태 업데이트 (JOINED 상태)
- 오래된 이벤트 만료 처리 (EXPIRED 상태)

```python
# 주요 메서드
track_bronze_arrival(events_df, batch_id)      # Bronze 도착 시 INSERT
mark_events_joined(event_ids, batch_id)        # 조인 성공 시 UPDATE
expire_old_waiting_events(hours_threshold=24)  # 자동 만료 처리
```

#### 📄 `src/validation/lifecycle_auditor.py`
**역할**: 4가지 핵심 감사 로직 실행
- **수집 정확성**: GDELT 예상 vs 실제 추적 이벤트
- **조인 성공률**: 12-24시간 성숙 이벤트의 조인율
- **데이터 유실 탐지**: 24시간+ 대기 이벤트 발견
- **Gold-Postgres 동기화**: 100% 일치 검증

```python
# 주요 감사 메서드
audit_collection_accuracy()     # 수집률 검증
audit_join_yield()              # 조인 성공률 측정
audit_data_loss_detection()     # 유실 의심 이벤트 탐지
audit_gold_postgres_sync()      # 동기화 정확성 검증
```

#### 📄 `src/validation/lifecycle_metrics_exporter.py`
**역할**: Prometheus 메트릭 전송
- 감사 결과를 12개 핵심 메트릭으로 변환
- Pushgateway를 통한 Grafana 연동
- 실시간 알람 트리거

### 2. **기존 파이프라인 수정**

#### 🔧 `src/processing/gdelt_silver_processor.py` (수정됨)
**추가된 기능**:
- Bronze → Silver 처리 후 lifecycle 기록
- 3-way join 성공 후 JOINED 상태 업데이트

```python
# 추가된 코드
lifecycle_tracker = EventLifecycleTracker(spark)
tracked_count = lifecycle_tracker.track_bronze_arrival(events_silver_df, batch_id)
updated_count = lifecycle_tracker.mark_events_joined(joined_event_ids, batch_id)
```

### 3. **운영 및 배포**

#### 📄 `dags/gdelt_lifecycle_audit_dag.py`
**역할**: Airflow를 통한 정기 감사 실행
- **4시간마다** lifecycle audit 실행
- **48시간 이상** 대기 이벤트 자동 만료
- Docker Swarm 환경에서 실행

#### 📄 `spark-base/requirements-spark.txt` (수정됨)
**추가됨**: `prometheus_client==0.20.0`

## 🔄 데이터 흐름 (Data Flow)

### 1. **이벤트 추적 시작**
```
GDELT Source → Bronze Layer → Silver Processor
                                      ↓
                         lifecycle_tracker.track_bronze_arrival()
                                      ↓
              audit.event_lifecycle 테이블에 INSERT
              Status: WAITING, bronze_arrival_time: NOW()
```

### 2. **3-Way 조인 성공 시**
```
Silver Processor (3-way join 완료) → joined_event_ids 추출
                                             ↓
                          lifecycle_tracker.mark_events_joined()
                                             ↓
                          audit.event_lifecycle 테이블 UPDATE
                          Status: WAITING → JOINED, detailed_joined_time: NOW()
```

### 3. **정기 감사 실행** (4시간마다)
```
LifecycleAuditor.run_full_audit()
        ↓
┌─ 수집 정확성 (최근 1시간)
├─ 조인 성공률 (12-24시간 전 이벤트)
├─ 데이터 유실 탐지 (24시간+ 대기)
└─ Gold-Postgres 동기화 검증
        ↓
MetricsExporter → Prometheus → Grafana
```

## 📊 핵심 메트릭 & 알람

### Grafana 대시보드 메트릭
- **`gdelt_collection_rate`**: 수집률 (목표: 70%+)
- **`gdelt_join_yield`**: 조인 성공률 (목표: 80%+)
- **`gdelt_suspicious_events`**: 의심 이벤트 수 (목표: 0개)
- **`gdelt_pipeline_health`**: 전체 파이프라인 상태 (1=HEALTHY, 0=UNHEALTHY)

### 알람 규칙
```promql
# Critical
gdelt_pipeline_health == 0           # 전체 실패
gdelt_collection_rate < 50           # 심각한 수집 실패
gdelt_gold_postgres_sync < 100       # 동기화 실패

# Warning
gdelt_join_yield < 80                # 조인 성공률 저하
gdelt_suspicious_events > 0          # 의심 이벤트 발생
```

## 🚀 배포 및 실행

### Docker Swarm 환경 배포
```bash
# 1. 이미지 재빌드 (prometheus_client 추가)
docker service update --force event-pipeline_spark-master
docker service update --force event-pipeline_spark-worker

# 2. DAG 자동 등록 (파일만 추가하면 됨)
# gdelt_lifecycle_audit_dag.py → Airflow UI에서 활성화

# 3. 수동 테스트 (필요시)
docker exec -it <spark-container> python /opt/airflow/src/validation/lifecycle_auditor.py
```

## 🎯 기대 효과

### AS-IS (기존)
- "뭔가 이상한데 뭐가 문제인지 모름" 😕
- 복잡한 600줄 코드로 부정확한 감사
- 15시간 순회 조인 로직을 정확히 검증 불가

### TO-BE (개선 후)
- "Join Yield 75% → 배치 XYZ 문제 → 재처리 필요" 🎯
- 실제 이벤트 여정 추적으로 정확한 감사
- Grafana 실시간 모니터링 + 자동 알람
- 24시간+ 유실 이벤트 자동 탐지 & 만료

**진짜 데이터 엔지니어링다운 감사 시스템 완성!** 🔥