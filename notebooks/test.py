import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

print("함수 정의 시작...")

# CAMEO 이벤트 코드 매핑 (한글 + 영어 원문)
cameo_actions = {
    "010": "공식 성명을 발표했다 (issued a public statement)",
    "020": "긴급 호소를 했다 (made an urgent appeal)",
    "030": "협력 의지를 강력히 표명했다 (expressed strong intent to cooperate with)",
    "040": "협의를 가졌다 (held consultations with)",
    "050": "외교적 협력을 했다 (engaged in diplomatic cooperation with)",
    "060": "물질적 지원을 제공했다 (provided material cooperation to)",
    "070": "원조를 확대했다 (extended aid to)",
    "080": "요구에 굴복했다 (yielded to demands from)",
    "090": "조사를 시작했다 (launched an investigation into)",
    "100": "요구사항을 제기했다 (made demands to)",
    "110": "불만을 표명했다 (voiced disapproval of)",
    "120": "제안을 거부했다 (rejected proposals from)",
    "130": "위협을 가했다 (issued threats against)",
    "140": "시위를 조직했다 (organized protests against)",
    "150": "군사력을 시위했다 (demonstrated military force against)",
    "160": "관계를 단절했다 (severed relations with)",
    "170": "강압을 가했다 (applied coercion to)",
    "180": "공격을 개시했다 (launched assault on)",
    "190": "무력 충돌을 벌였다 (engaged in armed conflict with)",
    "200": "대규모 폭력을 자행했다 (perpetrated mass violence against)",
}


def extract_rich_themes(themes_str):
    """테마에서 의미있는 키워드 추출"""
    if not themes_str:
        return []

    themes = []
    theme_mapping = {
        "WB_": "",
        "ECON_": "경제 (economic) ",
        "TAX_": "세금 (taxation) ",
        "TRADE": "무역 (trade)",
        "CRISISLEX_": "위기 (crisis) ",
        "NATURAL_DISASTER_": "자연재해 (natural disaster) ",
        "FNCACT": "금융 활동 (financial activities)",
        "EPU_": "경제정책 불확실성 (economic policy uncertainty)",
    }

    for theme in themes_str.split(";"):
        if theme.strip():
            theme_name = theme.split(",")[0].strip()
            if any(prefix in theme_name for prefix in theme_mapping.keys()):
                clean_name = theme_name
                for prefix, replacement in theme_mapping.items():
                    clean_name = clean_name.replace(prefix, replacement)
                clean_name = clean_name.replace("_", " ").strip().lower()
                if clean_name and clean_name not in themes:
                    themes.append(clean_name)

    return themes[:3]


def extract_key_persons(persons_str):
    """인물 정보 추출"""
    if not persons_str:
        return []

    persons = []
    for person in persons_str.split(";")[:3]:
        if person.strip():
            name = person.strip().title()
            if len(name) > 2:
                persons.append(name)
    return persons


def extract_key_organizations(orgs_str):
    """조직 정보 추출"""
    if not orgs_str:
        return []

    orgs = []
    for org in orgs_str.split(";")[:2]:
        if org.strip():
            org_name = org.strip().title()
            if len(org_name) > 3:
                orgs.append(org_name)
    return orgs


def extract_amounts(amounts_str):
    """금액/수량 정보 추출"""
    if not amounts_str:
        return []

    amounts = []
    for amount in amounts_str.split(";")[:2]:
        if amount.strip() and re.search(r"\d", amount):
            amounts.append(amount.strip())
    return amounts


def create_simple_story(row):
    """간단한 스토리"""
    actor1 = row.get("actor1_name") or row.get("actor1_code") or "Unknown entity"
    actor2 = row.get("actor2_name") or row.get("actor2_code")
    event_code = str(row.get("event_code", "")).zfill(3)
    location = (
        row.get("action_geo_fullname", "").split(",")[0]
        if row.get("action_geo_fullname")
        else "unknown location"
    )
    tone = row.get("avg_tone", 0)

    action = cameo_actions.get(
        event_code, f"{event_code} 액션을 수행했다 (performed action {event_code})"
    )
    tone_desc = (
        "긍정적으로 (positively)"
        if tone > 2
        else "부정적으로 (negatively)" if tone < -2 else "중립적으로 (neutrally)"
    )

    if actor2 and actor2.strip():
        return f"{actor1}이(가) {location}에서 {actor2}에게 {action} ({tone_desc})"
    else:
        return f"{actor1}이(가) {location}에서 {action} ({tone_desc})"


def create_rich_story(row):
    """향상된 버전 """

    # 기본 정보
    actor1 = row.get("actor1_name") or row.get("actor1_code") or "Unknown entity"
    actor2 = row.get("actor2_name") or row.get("actor2_code")
    event_code = str(row.get("event_code", "")).zfill(3)
    location = (
        row.get("action_geo_fullname", "").split(",")[0]
        if row.get("action_geo_fullname")
        else "unknown location"
    )

    # 인물 정보 추가
    persons = extract_key_persons(row.get("v2_persons", ""))
    if persons and persons[0].lower() not in actor1.lower():
        actor1 = (
            f"{persons[0]} from {actor1}" if len(persons[0]) > len(actor1) else actor1
        )

    # 액션 
    action = cameo_actions.get(
        event_code, f"{event_code} 액션을 취했다 (took action {event_code})"
    )

    # 톤 분석 
    tone = row.get("avg_tone", 0)
    if tone > 5:
        tone_desc = "매우 긍정적인 방식으로 (in a highly positive manner)"
    elif tone > 2:
        tone_desc = "긍정적인 감정으로 (with positive sentiment)"
    elif tone < -5:
        tone_desc = "강한 부정적 감정으로 (with strong negative sentiment)"
    elif tone < -2:
        tone_desc = "긴장 상황 속에서 (amid tensions)"
    else:
        tone_desc = "중립적인 톤으로 (in a neutral tone)"

    # 컨텍스트 정보
    themes = extract_rich_themes(row.get("v2_enhanced_themes", ""))
    context = (
        f" {', '.join(themes)}에 관해 (concerning {', '.join(themes)})"
        if themes
        else ""
    )

    orgs = extract_key_organizations(row.get("v2_organizations", ""))
    org_context = (
        f" {', '.join(orgs)}와 관련하여 (involving {', '.join(orgs)})" if orgs else ""
    )

    amounts = extract_amounts(row.get("amounts", ""))
    amount_context = (
        f" {', '.join(amounts)} 규모로 (worth {', '.join(amounts)})" if amounts else ""
    )

    # 메타 정보
    num_articles = row.get("num_articles", 0)
    source = row.get("mention_source_name", "")
    source_context = (
        f" ({num_articles}개 기사에서 보도됨"
        + (f", {source} 포함" if source else "")
        + f" / reported across {num_articles} articles"
        + (f" including {source}" if source else "")
        + ")"
    )

    # 중요도 표시
    goldstein = row.get("goldstein_scale", 0)
    importance = ""
    if abs(goldstein) > 8:
        importance = "🔥 주요 사건 (MAJOR EVENT): "
    elif abs(goldstein) > 5:
        importance = "⚠️ 중요 사건 (SIGNIFICANT): "
    elif abs(goldstein) > 3:
        importance = "📰 주목할 만한 사건 (NOTABLE): "

    # 최종 조합
    if actor2 and actor2.strip():
        story = f"{importance}{actor1}이(가) {location}에서 {actor2}에게 {tone_desc} {action}{context}{org_context}{amount_context}{source_context}"
    else:
        story = f"{importance}{actor1}이(가) {location}에서 {tone_desc} {action}{context}{org_context}{amount_context}{source_context}"

    return story


def search_event_by_id(event_id):
    """특정 이벤트 ID로 검색해서 스토리 생성"""
    import sys

    sys.path.append("/app")
    from src.utils.spark_builder import get_spark_session

    print(f"🔍 이벤트 ID {event_id} 검색 중...")

    # Spark 세션 생성
    spark = get_spark_session("GDELT_Event_Search", "spark://spark-master:7077")
    print("✅ Spark 세션 생성 완료")

    # 데이터 로드
    joined_path = "s3a://warehouse/silver/test_gdelt_joined/"
    final_silver_df = spark.read.format("delta").load(joined_path)

    # 특정 이벤트 검색
    target_event = final_silver_df.filter(
        final_silver_df.global_event_id == event_id
    ).collect()

    if not target_event:
        print(f"❌ 이벤트 ID {event_id}를 찾을 수 없습니다.")
        spark.stop()
        return

    row_dict = target_event[0].asDict()

    print("=" * 80)
    print(f"🎯 이벤트 ID {event_id} 발견!")
    print("=" * 80)

    print(f"📅 Date: {row_dict.get('event_date')}")
    print(f"🏛️ Actor1: {row_dict.get('actor1_name')} ({row_dict.get('actor1_code')})")
    print(f"🏛️ Actor2: {row_dict.get('actor2_name')} ({row_dict.get('actor2_code')})")
    print(f"🎯 Event Code: {row_dict.get('event_code')}")
    print(f"📍 Location: {row_dict.get('action_geo_fullname')}")
    print(f"😊 Tone: {row_dict.get('avg_tone', 0):.2f}")
    print(f"💎 Goldstein: {row_dict.get('goldstein_scale', 0)}")
    print(f"📰 Articles: {row_dict.get('num_articles', 0)}")
    print(f"🌐 Source: {row_dict.get('mention_source_name', '')}")

    # 상세 데이터
    print(f"\n📊 상세 데이터:")
    print(f"👥 Persons: {row_dict.get('v2_persons', '')}")
    print(f"🏢 Organizations: {row_dict.get('v2_organizations', '')}")
    print(f"💰 Amounts: {row_dict.get('amounts', '')}")
    print(f"🏷️ Themes: {row_dict.get('v2_enhanced_themes', '')}")

    # 스토리 생성
    simple = create_simple_story(row_dict)
    rich = create_rich_story(row_dict)

    print(f"\n📖 간단 스토리:")
    print(f"   {simple}")

    print(f"\n🌟 풍부한 스토리:")
    print(f"   {rich}")

    print("=" * 80)
    spark.stop()
    return row_dict


def test_story_generation():
    """Spark 세션 생성해서 실제 데이터로 테스트"""
    import sys

    sys.path.append("/app")
    from src.utils.spark_builder import get_spark_session

    print("🚀 GDELT 스토리 생성 테스트 시작!")

    # Spark 세션 생성
    spark = get_spark_session("GDELT_Story_Test", "spark://spark-master:7077")
    print("✅ Spark 세션 생성 완료")

    # 데이터 로드
    joined_path = "s3a://warehouse/silver/test_gdelt_joined/"
    final_silver_df = spark.read.format("delta").load(joined_path)
    print(f"✅ 데이터 로드 완료: {final_silver_df.count():,}개 레코드")

    print("\n🔸 실제 GDELT 데이터로 스토리 생성 테스트:")
    print("=" * 80)

    # 샘플 데이터 3개 추출
    sample_rows = final_silver_df.limit(3).collect()

    for i, row in enumerate(sample_rows, 1):
        row_dict = row.asDict()

        print(f"\n--- {i}번째 이벤트 상세 분석 ---")
        print(f"🆔 Event ID: {row_dict.get('global_event_id')}")
        print(f"📅 Date: {row_dict.get('event_date')}")
        print(
            f"🏛️ Actor1: {row_dict.get('actor1_name')} ({row_dict.get('actor1_code')})"
        )
        print(
            f"🏛️ Actor2: {row_dict.get('actor2_name')} ({row_dict.get('actor2_code')})"
        )
        print(f"🎯 Event Code: {row_dict.get('event_code')}")
        print(f"📍 Location: {row_dict.get('action_geo_fullname')}")
        print(f"😊 Tone: {row_dict.get('avg_tone', 0):.2f}")
        print(f"💎 Goldstein: {row_dict.get('goldstein_scale', 0)}")
        print(f"📰 Articles: {row_dict.get('num_articles', 0)}")

        # 원본 데이터 일부 확인
        print(f"👥 Persons: {str(row_dict.get('v2_persons', ''))[:80]}...")
        print(f"🏢 Organizations: {str(row_dict.get('v2_organizations', ''))[:80]}...")
        print(f"💰 Amounts: {str(row_dict.get('amounts', ''))[:80]}...")
        print(f"🏷️ Themes: {str(row_dict.get('v2_enhanced_themes', ''))[:80]}...")

        # 스토리 비교
        simple = create_simple_story(row_dict)
        rich = create_rich_story(row_dict)

        print(f"\n📖 간단 스토리:")
        print(f"   {simple}")

        print(f"\n🌟 풍부한 스토리:")
        print(f"   {rich}")

        print("=" * 80)

    print("\n✅ 스토리 생성 테스트 완료!")
    spark.stop()


print("✅ 함수 정의 완료!")
print("📋 사용 가능한 함수:")
print("   - create_simple_story(row): 간단한 스토리")
print("   - create_rich_story(row): 풍부한 스토리")
print("   - test_story_generation(): 실제 데이터로 테스트")
print("   - search_event_by_id(event_id): 특정 이벤트 검색")

# 직접 실행할 때 특정 이벤트 검색
if __name__ == "__main__":
    # search_event_by_id(1263162631)  # 원하는 이벤트 ID
    test_story_generation()  # 또는 일반 테스트
