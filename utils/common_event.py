from datetime import date, datetime
from decimal import Decimal

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType, TimestampType, DecimalType
)

# -------------------------
# Spark schema
# -------------------------
COMMON_EVENT_SCHEMA = StructType([
    StructField("trade_dt", DateType(), True),
    StructField("rec_type", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("event_tm", TimestampType(), True),
    StructField("event_seq_nb", IntegerType(), True),
    StructField("arrival_tm", TimestampType(), True),
    StructField("trade_pr", DecimalType(38, 18), True),
    StructField("bid_pr", DecimalType(38, 18), True),
    StructField("bid_size", IntegerType(), True),
    StructField("ask_pr", DecimalType(38, 18), True),
    StructField("ask_size", IntegerType(), True),
    StructField("partition", StringType(), True),
])

# -------------------------
# Helpers for casting
# -------------------------
def parse_date(s: str) -> date:
    return date.fromisoformat(s)

def parse_ts(s: str) -> datetime:
    # Handles "2020-08-05 09:30:00.0" and "2020-08-05 09:34:51.505"
    return datetime.fromisoformat(s)

def parse_dec(s: str) -> Decimal:
    return Decimal(s)

# -------------------------
# Common event constructor
# (returns a dict matching schema field names)
# -------------------------
def common_event(
    trade_dt,
    rec_type,
    symbol,
    exchange,
    event_tm,
    event_seq_nb,
    arrival_tm,
    trade_pr,
    bid_pr,
    bid_size,
    ask_pr,
    ask_size,
    partition,
):
    return {
        "trade_dt": trade_dt,
        "rec_type": rec_type,
        "symbol": symbol,
        "exchange": exchange,
        "event_tm": event_tm,
        "event_seq_nb": event_seq_nb,
        "arrival_tm": arrival_tm,
        "trade_pr": trade_pr,
        "bid_pr": bid_pr,
        "bid_size": bid_size,
        "ask_pr": ask_pr,
        "ask_size": ask_size,
        "partition": partition,
    }

def bad_record_event() -> dict:
    return common_event(
        trade_dt=None,
        rec_type="B",
        symbol=None,
        exchange=None,
        event_tm=None,
        event_seq_nb=None,
        arrival_tm=None,
        trade_pr=None,
        bid_pr=None,
        bid_size=None,
        ask_pr=None,
        ask_size=None,
        partition="B",
    )
