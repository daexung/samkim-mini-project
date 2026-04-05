import os
import re
import json
import time
import boto3
import pandas as pd

from datetime import datetime
from typing import Dict, List, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError


# ============================================================
# 1. 환경변수 로드
# - 로컬에서는 .env 사용 가능
# - Lambda에서는 CloudFormation / Lambda 환경변수 사용
# ============================================================
def load_env() -> Dict[str, str]:
    if os.path.exists(".env"):
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

    env = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_NAME": os.getenv("DB_NAME"),
        "S3_BUCKET": os.getenv("S3_BUCKET"),
    }

    missing = [k for k, v in env.items() if not v]
    if missing:
        raise EnvironmentError(f"필수 환경 변수가 없습니다: {', '.join(missing)}")

    return env


ENV = load_env()


# ============================================================
# 2. 공통 유틸
# ============================================================
def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def get_engine():
    url = URL.create(
        "mysql+pymysql",
        username=ENV["DB_USER"],
        password=ENV["DB_PASSWORD"],
        host=ENV["DB_HOST"],
        database=ENV["DB_NAME"],
        query={"charset": "utf8mb4"},
    )

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=300,
    )


def get_engine_with_retry(retries: int = 3, delay: int = 2):
    last_exception = None

    for attempt in range(1, retries + 1):
        try:
            log(f"[INFO] DB 엔진 생성 시도 {attempt}/{retries}")
            engine = get_engine()

            # 실제 연결 확인
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            log("[INFO] DB 연결 확인 완료")
            return engine

        except Exception as e:
            last_exception = e
            log(f"[WARN] DB 연결 실패 ({attempt}/{retries}): {str(e)}")

            if attempt < retries:
                log(f"[INFO] {delay}초 후 DB 재시도")
                time.sleep(delay)

    log("[ERROR] 모든 DB 연결 재시도 실패")
    raise last_exception


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    if value is None or pd.isna(value):
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def calc_preference_score(occupancy: float, sold_out_rate: float, speed_score: float) -> float:
    occupancy = max(0.0, occupancy)
    sold_out_rate = max(0.0, sold_out_rate)
    speed_score = max(0.0, speed_score)

    score = (occupancy * 0.5) + (sold_out_rate * 0.3) + (speed_score * 0.2)
    return round(score / 10, 1)


# ============================================================
# 3. 시간대 분석
# - 0으로 나누기 방지:
#   max_capacity = 0 이면 점유율 계산에서 제외
#   COUNT(*) = 0 이면 0 처리
# ============================================================
def analyze_time_preference(engine) -> List[Dict[str, Any]]:
    query = """
        SELECT 
            TIME_FORMAT(s.reservation_time, '%%H시') AS hour,
            ROUND(
                AVG(
                    CASE
                        WHEN s.max_capacity > 0
                        THEN ((s.max_capacity - s.remaining_capacity) / s.max_capacity) * 100
                        ELSE NULL
                    END
                ), 1
            ) AS avg_occupancy,
            ROUND(
                CASE
                    WHEN COUNT(*) > 0
                    THEN SUM(CASE WHEN s.status = 'SOLD_OUT' THEN 1 ELSE 0 END) / COUNT(*) * 100
                    ELSE 0
                END, 1
            ) AS sold_out_rate,
            ROUND(
                AVG(
                    CASE
                        WHEN s.status = 'SOLD_OUT'
                             AND TIMESTAMPDIFF(MINUTE, s.created_at, s.updated_at) IS NOT NULL
                        THEN GREATEST(
                            0,
                            (1 - (TIMESTAMPDIFF(MINUTE, s.created_at, s.updated_at) / (9 * 60))) * 100
                        )
                        ELSE 0
                    END
                ), 1
            ) AS speed_score
        FROM reservation_slots s
        GROUP BY s.reservation_time
        ORDER BY s.reservation_time
    """
    df = pd.read_sql(query, engine)

    result = []
    for _, row in df.iterrows():
        avg_occupancy = safe_float(row.get("avg_occupancy"))
        sold_out_rate = safe_float(row.get("sold_out_rate"))
        speed_score = safe_float(row.get("speed_score"))

        result.append({
            "hour": row.get("hour"),
            "avg_occupancy": avg_occupancy,
            "sold_out_rate": sold_out_rate,
            "preference_score": calc_preference_score(avg_occupancy, sold_out_rate, speed_score)
        })
    return result


# ============================================================
# 4. 요일 분석
# - 0으로 나누기 방지 동일 적용
# ============================================================
def analyze_day_preference(engine) -> List[Dict[str, Any]]:
    query = """
        SELECT 
            DAYNAME(s.reservation_date) AS weekday,
            DAYOFWEEK(s.reservation_date) AS dow_num,
            ROUND(
                AVG(
                    CASE
                        WHEN s.max_capacity > 0
                        THEN ((s.max_capacity - s.remaining_capacity) / s.max_capacity) * 100
                        ELSE NULL
                    END
                ), 1
            ) AS avg_occupancy,
            ROUND(
                CASE
                    WHEN COUNT(*) > 0
                    THEN SUM(CASE WHEN s.status = 'SOLD_OUT' THEN 1 ELSE 0 END) / COUNT(*) * 100
                    ELSE 0
                END, 1
            ) AS sold_out_rate,
            ROUND(
                AVG(
                    CASE
                        WHEN s.status = 'SOLD_OUT'
                             AND TIMESTAMPDIFF(MINUTE, s.created_at, s.updated_at) IS NOT NULL
                        THEN GREATEST(
                            0,
                            (1 - (TIMESTAMPDIFF(MINUTE, s.created_at, s.updated_at) / (9 * 60))) * 100
                        )
                        ELSE 0
                    END
                ), 1
            ) AS speed_score
        FROM reservation_slots s
        GROUP BY weekday, dow_num
        ORDER BY (dow_num + 5) %% 7
    """
    df = pd.read_sql(query, engine)

    day_map = {
        "Monday": "월",
        "Tuesday": "화",
        "Wednesday": "수",
        "Thursday": "목",
        "Friday": "금",
        "Saturday": "토",
        "Sunday": "일",
    }

    result = []
    for _, row in df.iterrows():
        avg_occupancy = safe_float(row.get("avg_occupancy"))
        sold_out_rate = safe_float(row.get("sold_out_rate"))
        speed_score = safe_float(row.get("speed_score"))

        result.append({
            "weekday": day_map.get(row.get("weekday"), row.get("weekday")),
            "avg_occupancy": avg_occupancy,
            "sold_out_rate": sold_out_rate,
            "preference_score": calc_preference_score(avg_occupancy, sold_out_rate, speed_score)
        })
    return result


# ============================================================
# 5. 월별 방문자 분석
# ============================================================
def analyze_monthly(engine) -> List[Dict[str, Any]]:
    query = """
        SELECT 
            DATE_FORMAT(reservation_date, '%%Y-%%m') AS month,
            COALESCE(CAST(SUM(party_size) AS UNSIGNED), 0) AS total_visitors
        FROM reservations
        WHERE status = 'SUCCESS'
        GROUP BY month
        ORDER BY month
    """
    df = pd.read_sql(query, engine)

    if not df.empty:
        df["total_visitors"] = df["total_visitors"].fillna(0).astype(int)

    return df.to_dict(orient="records")


# ============================================================
# 6. 리포트 분석
# ============================================================
def analyze_report(engine) -> Dict[str, Any]:
    with engine.connect() as conn:
        conn.execute(text("SET NAMES utf8mb4"))
        conn.execute(text("SET CHARACTER SET utf8mb4"))
        conn.execute(text("SET character_set_connection = utf8mb4"))

    age_query = """
        SELECT 
            u.age_group,
            ROUND(
                CASE
                    WHEN (SELECT COUNT(*) FROM users) > 0
                    THEN COUNT(*) / (SELECT COUNT(*) FROM users) * 100
                    ELSE 0
                END, 1
            ) AS ratio
        FROM users u
        GROUP BY u.age_group
        ORDER BY u.age_group
    """
    df_age = pd.read_sql(age_query, engine)

    def normalize_age_group(x):
        x = str(x).strip()

        if re.fullmatch(r"\d{2}대", x):
            return x

        m = re.search(r"(\d{2})", x)
        if m:
            age = m.group(1)
            if age.endswith("0"):
                return f"{age}대"

        return x

    if not df_age.empty:
        df_age["age_group"] = df_age["age_group"].apply(normalize_age_group)
        df_age["ratio"] = df_age["ratio"].fillna(0).astype(float)
        df_age = (
            df_age.groupby("age_group", as_index=False)["ratio"]
            .sum()
            .sort_values("age_group")
            .reset_index(drop=True)
        )
        df_age["ratio"] = df_age["ratio"].round(1)

    gender_query = """
        SELECT 
            u.gender,
            ROUND(
                CASE
                    WHEN (SELECT COUNT(*) FROM users) > 0
                    THEN COUNT(*) / (SELECT COUNT(*) FROM users) * 100
                    ELSE 0
                END, 1
            ) AS ratio
        FROM users u
        GROUP BY u.gender
    """
    df_gender = pd.read_sql(gender_query, engine)

    def normalize_gender(x):
        x = str(x).strip().upper()
        if x in ["M", "MALE", "남", "남성"]:
            return "남성"
        if x in ["F", "FEMALE", "여", "여성"]:
            return "여성"
        return x

    if not df_gender.empty:
        df_gender["gender"] = df_gender["gender"].apply(normalize_gender)
        df_gender["ratio"] = df_gender["ratio"].fillna(0).astype(float)
        df_gender = (
            df_gender.groupby("gender", as_index=False)["ratio"]
            .sum()
            .reset_index(drop=True)
        )
        df_gender["ratio"] = df_gender["ratio"].round(1)

    party_query = """
        SELECT 
            party_size,
            COUNT(*) AS count,
            ROUND(
                CASE
                    WHEN SUM(COUNT(*)) OVER() > 0
                    THEN COUNT(*) / SUM(COUNT(*)) OVER() * 100
                    ELSE 0
                END, 1
            ) AS ratio
        FROM reservations
        WHERE status = 'SUCCESS'
        GROUP BY party_size
        ORDER BY count DESC
    """
    df_party = pd.read_sql(party_query, engine)

    if not df_party.empty:
        df_party["party_size"] = df_party["party_size"].fillna(0).astype(int)
        df_party["count"] = df_party["count"].fillna(0).astype(int)
        df_party["ratio"] = df_party["ratio"].fillna(0).astype(float)

    avg_query = """
        SELECT ROUND(COALESCE(AVG(party_size), 0), 1) AS avg_party_size
        FROM reservations
        WHERE status = 'SUCCESS'
    """
    df_avg = pd.read_sql(avg_query, engine)
    avg_party = (
        safe_float(df_avg["avg_party_size"].iloc[0])
        if not df_avg.empty
        else 0.0
    )

    if not df_age.empty:
        top_idx = df_age["ratio"].idxmax()
        top_age = str(df_age.loc[top_idx, "age_group"]).strip()
    else:
        top_age = "고객"

    female_ratio = (
        safe_float(df_gender[df_gender["gender"] == "여성"]["ratio"].iloc[0])
        if not df_gender[df_gender["gender"] == "여성"].empty
        else 0.0
    )
    male_ratio = (
        safe_float(df_gender[df_gender["gender"] == "남성"]["ratio"].iloc[0])
        if not df_gender[df_gender["gender"] == "남성"].empty
        else 0.0
    )

    if abs(male_ratio - female_ratio) < 1.0:
        gender_text = "남녀 고객이 고르게 방문하고 있어요."
    elif female_ratio > male_ratio:
        gender_text = "여성 고객 비율이 높은 편이에요."
    else:
        gender_text = "남성 고객 비율이 높은 편이에요."

    if not df_party.empty:
        most_size = safe_int(df_party.iloc[0]["party_size"])
        party_text = f"가장 많은 예약은 {most_size}인 방문이에요."
    else:
        party_text = "다양한 인원대로 방문하고 있어요."

    insight = f"{top_age} 고객의 방문이 가장 많고, {gender_text} {party_text}"

    return {
        "age_group": df_age.to_dict(orient="records"),
        "gender": df_gender.to_dict(orient="records"),
        "party_size_distribution": df_party.to_dict(orient="records"),
        "avg_party_size": avg_party,
        "insight": insight
    }


# ============================================================
# 7. 공통 실행 로직
# ============================================================
def run_analysis() -> Dict[str, Any]:
    engine = None
    try:
        log("[INFO] DB 엔진 생성 시작")
        engine = get_engine_with_retry(retries=3, delay=2)
        log("[INFO] DB 엔진 생성 완료")

        log("[INFO] 시간대 분석 시작")
        time_slot = analyze_time_preference(engine)
        log("[INFO] 시간대 분석 완료")

        log("[INFO] 요일 분석 시작")
        day_slot = analyze_day_preference(engine)
        log("[INFO] 요일 분석 완료")

        log("[INFO] 월별 분석 시작")
        monthly = analyze_monthly(engine)
        log("[INFO] 월별 분석 완료")

        log("[INFO] 리포트 분석 시작")
        report = analyze_report(engine)
        log("[INFO] 리포트 분석 완료")

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "time_slot": time_slot,
            "day_slot": day_slot,
            "monthly": monthly,
            "report": report
        }

    finally:
        if engine is not None:
            engine.dispose()
            log("[INFO] DB 엔진 종료 완료")


# ============================================================
# 8. 저장 함수
# ============================================================
def save_to_s3(data: Dict[str, Any], filename: str) -> None:
    try:
        log("[INFO] S3 클라이언트 생성")
        s3 = boto3.client("s3")

        json_str = json.dumps(data, ensure_ascii=False, indent=2)

        s3.put_object(
            Bucket=ENV["S3_BUCKET"],
            Key=f"analysis/{filename}",
            Body=json_str.encode("utf-8"),
            ContentType="application/json; charset=utf-8"
        )

        log(f"[INFO] S3 저장 완료: analysis/{filename}")

    except Exception as e:
        log(f"[ERROR] S3 업로드 실패: {str(e)}")
        raise


def save_to_local(data: Dict[str, Any], filename: str) -> None:
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log(f"[INFO] 로컬 저장 완료: {filename}")
    except Exception as e:
        log(f"[ERROR] 로컬 저장 실패: {str(e)}")
        raise


# ============================================================
# 9. Lambda 엔트리포인트
# ============================================================
def handler(event, context):
    log("[START] Lambda execution started")

    try:
        result = run_analysis()
        filename = f"analysis_{datetime.now().strftime('%Y-%m-%d')}.json"

        log("[INFO] S3 저장 시작")
        save_to_s3(result, filename)

        log("[END] Lambda execution finished successfully")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json; charset=utf-8"},
            "body": json.dumps(
                {
                    "message": "분석 완료",
                    "filename": filename
                },
                ensure_ascii=False
            )
        }

    except EnvironmentError as e:
        log(f"[ERROR] 환경변수 오류: {str(e)}")
        raise
    except SQLAlchemyError as e:
        log(f"[ERROR] DB 오류: {str(e)}")
        raise
    except Exception as e:
        log(f"[ERROR] Lambda execution failed: {str(e)}")
        raise


# ============================================================
# 10. 로컬 실행
# ============================================================
if __name__ == "__main__":
    try:
        result = run_analysis()
        filename = f"analysis_{datetime.now().strftime('%Y-%m-%d')}.json"
        save_to_local(result, filename)
        print(result["report"]["insight"])
    except Exception as e:
        log(f"[ERROR] 로컬 실행 실패: {str(e)}")
        raise