"""
Docstring for utils.json_parser

The parser will read the JSON text file line by line and transform it into either trade or quote
records based on the “event_type” (or similar) field value.

It will also handle corrupted records, allowing the program to continue processing the good records
without crashing. These bad records will be put aside into a special partition ("B").

Expected input: one JSON object per line (JSONL), e.g.
{"trade_dt":"2020-08-05","file_tm":"2020-08-05 09:30:00.000","event_type":"Q",...}
"""
import json

from utils.common_event import (
    common_event,
    bad_record_event,
    parse_date,
    parse_ts,
    parse_dec,
)

def parse_quote_json(obj: dict) -> dict:
    # Quote JSON example keys:
    # trade_dt, file_tm, event_type(Q), symbol, event_tm, event_seq_nb, exchange,
    # bid_pr, bid_size, ask_pr, ask_size
    return common_event(
        trade_dt=parse_date(obj["trade_dt"]),
        rec_type=obj.get("event_type") or obj.get("rec_type") or "Q",
        symbol=obj["symbol"],
        exchange=obj["exchange"],
        event_tm=parse_ts(obj["event_tm"]),
        event_seq_nb=int(obj["event_seq_nb"]),
        arrival_tm=parse_ts(obj["file_tm"]),   # using file_tm as arrival time (same idea as CSV)
        trade_pr=None,
        bid_pr=parse_dec(str(obj["bid_pr"])),
        bid_size=int(obj["bid_size"]),
        ask_pr=parse_dec(str(obj["ask_pr"])),
        ask_size=int(obj["ask_size"]),
        partition="Q",
    )

def parse_trade_json(obj: dict) -> dict:
    # Trade JSON example keys (trade_size may exist but is not in common schema):
    # trade_dt, file_tm, event_type(T), symbol, event_tm, event_seq_nb, exchange, trade_pr, trade_size?
    return common_event(
        trade_dt=parse_date(obj["trade_dt"]),
        rec_type=obj.get("event_type") or obj.get("rec_type") or "T",
        symbol=obj["symbol"],
        exchange=obj["exchange"],
        event_tm=parse_ts(obj["event_tm"]),
        event_seq_nb=int(obj["event_seq_nb"]),
        arrival_tm=parse_ts(obj["file_tm"]),
        trade_pr=parse_dec(str(obj["trade_pr"])),
        bid_pr=None,
        bid_size=None,
        ask_pr=None,
        ask_size=None,
        partition="T",
    )

def parse_json(line: str) -> dict:
    try:
        obj = json.loads(line)

        # Most guides call this field event_type in JSON; fallback to rec_type if present
        rtype = obj.get("event_type") or obj.get("rec_type")

        if rtype == "Q":
            return parse_quote_json(obj)
        elif rtype == "T":
            return parse_trade_json(obj)
        else:
            return bad_record_event()

    except Exception:
        return bad_record_event()
