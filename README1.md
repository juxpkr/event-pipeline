# 🚀 GDELT 데이터 파이프라인 프로젝트

**팀원 온보딩 가이드 - 개발 환경 구축부터 첫 실행까지**

## 🎯 이 프로젝트가 뭐야?

**GDELT 글로벌 이벤트 데이터**를 실시간으로 수집하고 분석하는 데이터 파이프라인이야. 
뉴스, 정치 이벤트 등을 분석해서 지정학적 위험도를 측정하는 게 최종 목표!

## 👥 우리 팀 구성
- **데이터 엔지니어 2명**: 파이프라인 구축
- **데이터 분석가 2명**: 데이터 분석 및 시각화
- **개발 기간**: 1개월

## 🏗️ 기술 스택 (몰라도 괜찮아!)

```
📊 데이터 수집 → 🔄 실시간 처리 → 💾 저장 → 📈 분석
   (BigQuery)     (Kafka+Spark)    (MinIO)   (Jupyter)
```

**Docker가 모든 걸 알아서 설치해줄 거야!** 🐳

## ⚡ 5분만에 시작하기

### 1️⃣ 필수 설치 (한 번만!)

**Windows:**
```bash
# 1. Docker Desktop 설치
https://www.docker.com/products/docker-desktop/

# 2. Git 설치  
https://git-scm.com/downloads

# 3. VSCode 설치 (선택사항)
https://code.visualstudio.com/
```

**Mac:**
```bash
# 1. Docker Desktop 설치
https://www.docker.com/products/docker-desktop/

# 2. 터미널에서
brew install git
```

### 2️⃣ 프로젝트 다운로드

```bash
# 터미널/명령 프롬프트에서
git clone https://github.com/your-repo/de-pipeline-template.git
cd de-pipeline-template
```

### 3️⃣ 환경 설정 (중요!)

```bash
# .env 파일에서 GCP 키 설정 필요
# 팀 리더한테 키 파일 받아서 .secrets/ 폴더에 넣기
```

### 4️⃣ 마법의 한 줄 명령어! ✨

```bash
docker-compose up -d
```

**이게 다야! Docker가 모든 서비스를 알아서 설치하고 실행해줘**

### 5️⃣ 브라우저에서 확인

| 🌐 접속 주소 | 📝 용도 | 👤 사용자 |
|------------|---------|-----------|
| http://localhost:8082 | 파이프라인 관리 (Airflow) | 전체 팀 |
| http://localhost:8888 | 데이터 분석 (Jupyter) | **분석가** |
| http://localhost:8081 | 처리 상태 확인 (Spark) | 엔지니어 |
| http://localhost:9001 | 데이터 저장소 (MinIO) | 엔지니어 |

**로그인 정보: admin/admin (모든 서비스 동일)**

## 🎮 역할별 가이드

### 📊 **데이터 분석가라면:**

1. **Jupyter 노트북 사용** (http://localhost:8888)
```python
# 데이터 불러오기 (복붙해서 사용!)
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Analysis").getOrCreate()

# GDELT 데이터 읽기
df = spark.read.format("delta").load("s3a://silver/gdelt_events")
df.show()
```

2. **분석 예제**
```python
# 국가별 이벤트 수
df.groupBy("Actor1CountryCode").count().show()

# 시계열 분석
df.groupBy("event_date").count().orderBy("event_date").show()
```

### 🛠️ **데이터 엔지니어라면:**

1. **Airflow에서 파이프라인 관리** (http://localhost:8082)
2. **코드 수정**:
   - `src/ingestion/` - 데이터 수집 로직
   - `src/processing/` - 데이터 처리 로직
   - `dags/` - 파이프라인 스케줄링

## 🆘 문제 해결

### "Docker가 안 켜져요!"
```bash
# Docker Desktop이 실행 중인지 확인
# Windows: 트레이 아이콘 확인
# Mac: 상단 메뉴바 확인
```

### "8082 포트를 사용할 수 없어요!"
```bash
# 다른 프로그램이 포트 사용 중
docker-compose down  # 모든 서비스 중단
docker-compose up -d # 다시 시작
```

### "데이터가 안 보여요!"
```bash
# 1. Airflow에서 DAG 실행했는지 확인
# 2. GCP 키 설정했는지 확인
# 3. 팀 리더에게 문의!
```

## 📚 배경 지식 (읽어보면 도움 됨)

### Docker가 뭐야?
- **가상 컴퓨터** 같은 거야
- 우리 프로그램들이 **독립된 공간**에서 실행됨
- **설치 복잡함 해결**: "내 컴퓨터에선 돼는데?" 문제 해결

### 우리가 사용하는 도구들:
- **Kafka**: 데이터를 실시간으로 전달하는 메신저
- **Spark**: 큰 데이터를 빠르게 처리하는 계산기
- **Airflow**: 작업 스케줄을 관리하는 매니저
- **MinIO**: 파일을 저장하는 창고

## 🤝 팀 협업 가이드

### Git 사용법
```bash
# 최신 코드 받기
git pull origin main

# 내 작업 저장
git add .
git commit -m "분석 결과 추가"
git push origin main
```

### 파일 수정 규칙
- **분석가**: `notebooks/`, `dbt/models/` 폴더만 수정
- **엔지니어**: `src/`, `dags/` 폴더 수정
- **.env 파일**: 팀 리더 승인 후 수정

## 📞 도움 요청

### 1순위: 팀 리더
### 2순위: 팀 채팅방
### 3순위: 구글링 😄

## ✅ 첫날 할 일 체크리스트

- [ ] Docker Desktop 설치 및 실행
- [ ] 프로젝트 클론
- [ ] GCP 키 파일 설정
- [ ] `docker-compose up -d` 실행
- [ ] 모든 웹페이지 접속 확인
- [ ] Jupyter에서 "Hello World" 실행
- [ ] 팀 채팅방에 "환경 구축 완료!" 메시지

**환경 구축에 문제가 있으면 바로 말해! 혼자 끙끙대지 말고** 🙋‍♀️

---
**⭐ 팁: 이 README는 계속 업데이트될 예정이니까 자주 확인해!**