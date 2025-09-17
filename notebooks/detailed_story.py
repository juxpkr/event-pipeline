import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

print("함수 정의 시작...")

# -------------------------------------------------------------------
# 1. CAMEO 이벤트 코드 매핑 (한글 + 영어)
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# 2. 테마 매핑 (확장 버전)
# -------------------------------------------------------------------
theme_mapping = {
    # 국제기구/정책
    "WB_": "세계은행 주제 (World Bank theme)",
    "UN_": "유엔 관련 (United Nations)",
    "EU_": "유럽연합 관련 (European Union)",

    # 경제/재정
    "ECON_": "경제 (economic)",
    "EPU_": "경제정책 불확실성 (economic policy uncertainty)",
    "FINANCE": "금융/재정 (finance)",
    "FNCACT": "금융 활동 (financial activities)",
    "TAX_": "세금 (taxation)",
    "TRADE": "무역 (trade)",
    "INVEST": "투자 (investment)",
    "SANCTIONS": "제재 (sanctions)",

    # 사회/정치 불안
    "UNREST_": "사회 불안/저항 (unrest)",
    "PROTEST": "시위/항의 (protest)",
    "STRIKE": "파업/노동쟁의 (strike)",
    "COUP": "쿠데타 (coup d'état)",

    # 갈등/안보
    "CONFLICT": "분쟁/갈등 (conflict)",
    "MILITARY": "군사 관련 (military)",
    "SECURITY": "안보/방위 (security)",
    "TERROR": "테러 관련 (terrorism)",
    "VIOLENCE": "폭력 사건 (violence)",
    "WAR": "전쟁 (war)",

    # 인권/법치
    "HUMAN_RIGHTS": "인권 (human rights)",
    "GENOCIDE": "집단학살 (genocide)",
    "JUSTICE": "사법/정의 (justice)",
    "LAW": "법률/규제 (law)",
    "SOVEREIGNTY": "주권 문제 (sovereignty)",

    # 재난/위기
    "CRISISLEX_": "위기 (crisis)",
    "NATURAL_DISASTER_": "자연재해 (natural disaster)",
    "DISASTER": "재해 (disaster)",
    "FLOOD": "홍수 (flood)",
    "EARTHQUAKE": "지진 (earthquake)",
    "FIRE": "화재 (fire)",
    "EPIDEMIC": "전염병/질병 확산 (epidemic)",

    # 원조/구호
    "AID_": "원조/인도주의 지원 (aid/humanitarian)",
    "FOOD_SECURITY": "식량 안보 (food security)",
    "REFUGEE": "난민/이주민 (refugees/migration)",
    "HUMANITARIAN": "인도주의 (humanitarian aid)",

    # 환경/자원
    "CLIMATE": "기후/환경 (climate/environment)",
    "ENERGY": "에너지 (energy)",
    "OIL": "석유/가스 (oil/gas)",
    "WATER": "물 자원 (water resources)",
    "FOREST": "산림/자연자원 (forests/natural resources)",

    # 미디어/정보
    "MEDIA": "미디어/언론 (media)",
    "INTERNET": "인터넷/디지털 (internet/digital)",
    "ICT": "정보통신기술 (ICT)",
    "SOCIAL_MEDIA": "소셜 미디어 (social media)",

    # 기타
    "ELECTION": "선거/정치 과정 (elections)",
    "CORRUPTION": "부패 (corruption)",
    "TRANSPARENCY": "투명성 (transparency)",
    "CRIME": "범죄 (crime)",
    "DRUG": "마약/불법거래 (drugs/illicit trade)",
    "ORGANIZED_CRIME": "조직범죄 (organized crime)",
}
# -------------------------------------------------------------------
# 3. 필터링 유틸 함수
# -------------------------------------------------------------------
def is_valid_text(value: str) -> bool:
    """숫자 배열, 짧은 문자열, 의미 없는 값 필터링"""
    if not value:
        return False
    value = value.strip()

    # 숫자/소수점/콤마만 반복되는 경우 (예: -2.6,4.3,7.9 ...)
    if re.fullmatch(r"[-\d\.\,\s]+", value):
        return False

    # 너무 짧은 경우 제외
    if len(value) <= 2:
        return False

    # 의미 없는 코드성 값 (예: 0, 000, 12345)
    if re.fullmatch(r"\d{1,5}", value):
        return False

    return True


# -------------------------------------------------------------------
# 4. 보조 추출 함수
# -------------------------------------------------------------------
def extract_rich_themes(themes_str):
    if not themes_str:
        return []
    themes = []
    for theme in themes_str.split(";"):
        theme_name = theme.split(",")[0].strip()
        if is_valid_text(theme_name):  # 숫자 배열 필터링 적용
            for prefix, replacement in theme_mapping.items():
                if prefix in theme_name:
                    clean_name = theme_name.replace(prefix, replacement).replace("_", " ").strip()
                    if clean_name and clean_name not in themes:
                        themes.append(clean_name)
    return themes[:3]



def extract_key_persons(persons_str):
    if not persons_str:
        return []
    return [p.strip().title() for p in persons_str.split(";") if len(p.strip()) > 2][:3]


def extract_key_organizations(orgs_str):
    if not orgs_str:
        return []
    return [o.strip().title() for o in orgs_str.split(";") if len(o.strip()) > 3][:2]


def extract_amounts(amounts_str):
    if not amounts_str:
        return []
    return [a.strip() for a in amounts_str.split(";") if re.search(r"\d", a)][:2]

# -------------------------------------------------------------------
# 5. 스토리 생성 함수
# -------------------------------------------------------------------
def create_simple_story(row):
    actor1 = row.get("actor1_name") or row.get("actor1_code") or "Unknown entity"
    actor2 = row.get("actor2_name") or row.get("actor2_code")
    event_code = str(row.get("event_code", "")).zfill(3)
    location = (row.get("action_geo_fullname", "").split(",")[0]
                if row.get("action_geo_fullname") else "unknown location")
    tone = row.get("avg_tone", 0)

    action = cameo_actions.get(event_code, f"{event_code} 액션을 수행했다 (performed action {event_code})")
    tone_desc = "긍정적으로 (positively)" if tone > 2 else "부정적으로 (negatively)" if tone < -2 else "중립적으로 (neutrally)"

    if actor2 and actor2.strip():
        return f"{actor1}이(가) {location}에서 {actor2}에게 {action} ({tone_desc})"
    return f"{actor1}이(가) {location}에서 {action} ({tone_desc})"


def create_rich_story(row):
    actor1 = row.get("actor1_name") or row.get("actor1_code") or "Unknown entity"
    actor2 = row.get("actor2_name") or row.get("actor2_code")
    event_code = str(row.get("event_code", "")).zfill(3)
    location = (row.get("action_geo_fullname", "").split(",")[0]
                if row.get("action_geo_fullname") else "unknown location")

    persons = extract_key_persons(row.get("v2_persons", ""))
    if persons and persons[0].lower() not in actor1.lower():
        actor1 = f"{persons[0]} from {actor1}" if len(persons[0]) > len(actor1) else actor1

    action = cameo_actions.get(event_code, f"{event_code} 액션을 취했다 (took action {event_code})")

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

    themes = extract_rich_themes(row.get("v2_enhanced_themes", ""))
    context = f" {', '.join(themes)}에 관해 (concerning {', '.join(themes)})" if themes else ""

    orgs = extract_key_organizations(row.get("v2_organizations", ""))
    org_context = f" {', '.join(orgs)}와 관련하여 (involving {', '.join(orgs)})" if orgs else ""

    amounts = extract_amounts(row.get("amounts", ""))
    amount_context = f" {', '.join(amounts)} 규모로 (worth {', '.join(amounts)})" if amounts else ""

    num_articles = row.get("num_articles", 0)
    source = row.get("mention_source_name", "")
    source_context = f" ({num_articles}개 기사에서 보도됨" + (f", {source} 포함" if source else "") \
                     + f" / reported across {num_articles} articles" \
                     + (f" including {source}" if source else "") + ")"

    goldstein = row.get("goldstein_scale", 0)
    importance = "주요 사건 (MAJOR EVENT): " if abs(goldstein) > 8 \
        else "중요 사건 (SIGNIFICANT): " if abs(goldstein) > 5 \
        else "주목할 만한 사건 (NOTABLE): " if abs(goldstein) > 3 else ""

    if actor2 and actor2.strip():
        return f"{importance}{actor1}이(가) {location}에서 {actor2}에게 {tone_desc} {action}{context}{org_context}{amount_context}{source_context}"
    return f"{importance}{actor1}이(가) {location}에서 {tone_desc} {action}{context}{org_context}{amount_context}{source_context}"


def create_headline_story(row):
    actor1 = row.get("actor1_name") or row.get("actor1_country_code") or "Unknown"
    actor2 = row.get("actor2_name") or row.get("actor2_country_code") or ""
    event = str(row.get("event_code", "")).zfill(3)

    event_mapping = {
        "010": "issued statement",
        "020": "made urgent appeal",
        "040": "held consultations",
        "080": "yielded to demands",
        "090": "launched investigation",
        "130": "issued threats",
        "180": "launched assault",
        "190": "armed conflict",
        "200": "mass violence"
    }
    event_desc = event_mapping.get(event, "took action")

    if actor2:
        return f"{actor1} {event_desc} with {actor2}"
    return f"{actor1} {event_desc}"


def create_event_summary(row):
    root = str(row.get("event_root_code", ""))
    summary_map = {
        "01": "Statement",
        "02": "Appeal",
        "03": "Cooperation",
        "04": "Consultation",
        "05": "Diplomatic",
        "13": "Threat",
        "14": "Protest",
        "18": "Assault",
        "19": "Conflict",
        "20": "Violence"
    }
    return summary_map.get(root, "Other")


def create_tone_story(row):
    base_story = create_headline_story(row)
    tone = row.get("avg_tone", 0)

    if tone > 2:
        tone_desc = "with a positive tone"
    elif tone < -2:
        tone_desc = "with a negative tone"
    else:
        tone_desc = "with a neutral tone"

    return f"{base_story} ({tone_desc})"

# -------------------------------------------------------------------
# 6. 유틸 함수 (검색/테스트)
# -------------------------------------------------------------------
def search_event_by_id(event_id):
    import sys
    sys.path.append("/app")
    from src.utils.spark_builder import get_spark_session

    print(f"이벤트 ID {event_id} 검색 중...")

    spark = get_spark_session("GDELT_Event_Search", "spark://spark-master:7077")
    joined_path = "s3a://warehouse/silver/test_gdelt_joined/"
    final_silver_df = spark.read.format("delta").load(joined_path)

    target_event = final_silver_df.filter(final_silver_df.global_event_id == event_id).collect()
    if not target_event:
        print(f"이벤트 ID {event_id}를 찾을 수 없습니다.")
        spark.stop()
        return

    row_dict = target_event[0].asDict()
    print(f"Date: {row_dict.get('event_date')}")
    print(f"Actor1: {row_dict.get('actor1_name')} ({row_dict.get('actor1_code')})")
    print(f"Actor2: {row_dict.get('actor2_name')} ({row_dict.get('actor2_code')})")
    print(f"Event Code: {row_dict.get('event_code')}")
    print(f"Location: {row_dict.get('action_geo_fullname')}")
    print(f"Tone: {row_dict.get('avg_tone', 0):.2f}")
    print(f"Goldstein: {row_dict.get('goldstein_scale', 0)}")
    print(f"Articles: {row_dict.get('num_articles', 0)}")
    print(f"Source: {row_dict.get('mention_source_name', '')}")

    print("\nSimple Story:", create_simple_story(row_dict))
    print("Rich Story:", create_rich_story(row_dict))
    print("Headline Story:", create_headline_story(row_dict))
    print("Event Summary:", create_event_summary(row_dict))
    print("Tone Story:", create_tone_story(row_dict))

    spark.stop()
    return row_dict


def test_story_generation():
    import sys
    sys.path.append("/app")
    from src.utils.spark_builder import get_spark_session

    print("GDELT 스토리 생성 테스트 시작")
    spark = get_spark_session("GDELT_Story_Test", "spark://spark-master:7077")

    joined_path = "s3a://warehouse/silver/test_gdelt_joined/"
    final_silver_df = spark.read.format("delta").load(joined_path)

    sample_rows = final_silver_df.limit(3).collect()
    for i, row in enumerate(sample_rows, 1):
        row_dict = row.asDict()
        print(f"\n--- {i}번째 이벤트 ---")
        print(f"Event ID: {row_dict.get('global_event_id')}")
        print(f"Date: {row_dict.get('event_date')}")
        print(f"Actor1: {row_dict.get('actor1_name')} ({row_dict.get('actor1_code')})")
        print(f"Actor2: {row_dict.get('actor2_name')} ({row_dict.get('actor2_code')})")
        print(f"Event Code: {row_dict.get('event_code')}")
        print(f"Location: {row_dict.get('action_geo_fullname')}")
        print(f"Tone: {row_dict.get('avg_tone', 0):.2f}")
        print(f"Goldstein: {row_dict.get('goldstein_scale', 0)}")
        print(f"Articles: {row_dict.get('num_articles', 0)}")

        print("Simple Story:", create_simple_story(row_dict))
        print("Rich Story:", create_rich_story(row_dict))
        print("Headline Story:", create_headline_story(row_dict))
        print("Event Summary:", create_event_summary(row_dict))
        print("Tone Story:", create_tone_story(row_dict))

    spark.stop()


print("✅ 함수 정의 완료")
print("사용 가능한 함수:")
print("   - create_simple_story(row)")
print("   - create_rich_story(row)")
print("   - create_headline_story(row)")
print("   - create_event_summary(row)")
print("   - create_tone_story(row)")
print("   - search_event_by_id(event_id)")
print("   - test_story_generation()")

if __name__ == "__main__":
    test_story_generation()
