"""
Docstring for utils.csv_parser

The parser will read the text file line by line and transform it into either trade or quote records based on
“rec_type” column value. The parser will also handle corrupted records, allowing the program to
continue processing the good records without crashing. These bad records will be put aside into
a special partition.

"""
from utils.common_event import (
    common_event,
    bad_record_event,
    parse_date,
    parse_ts,
    parse_dec,
)

def parse_quote_record(record: list) -> dict:
    # Quote line example:
    # 0 trade_dt
    # 1 file_tm (your sample shows "2020-08-05 09:30:00.0" here)
    # 2 rec_type (Q)
    # 3 symbol
    # 4 event_tm
    # 5 event_seq_nb
    # 6 exchange
    # 7 bid_pr
    # 8 bid_size
    # 9 ask_pr
    # 10 ask_size
    return common_event(
        trade_dt=parse_date(record[0]),
        rec_type=record[2],
        symbol=record[3],
        exchange=record[6],
        event_tm=parse_ts(record[4]),
        event_seq_nb=int(record[5]),
        arrival_tm=parse_ts(record[1]),
        trade_pr=None,
        bid_pr=parse_dec(record[7]),
        bid_size=int(record[8]),
        ask_pr=parse_dec(record[9]),
        ask_size=int(record[10]),
        partition="Q",
    )

def parse_trade_record(record: list) -> dict:
    # Trade line example:
    # 0 trade_dt
    # 1 file_tm
    # 2 rec_type (T)
    # 3 symbol
    # 4 event_tm
    # 5 event_seq_nb
    # 6 exchange
    # 7 trade_pr
    # 8 trade_size (not in common schema -> ignored)

    return common_event(
        trade_dt=parse_date(record[0]),
        rec_type=record[2],
        symbol=record[3],
        exchange=record[6],
        event_tm=parse_ts(record[4]),
        event_seq_nb=int(record[5]),
        arrival_tm=parse_ts(record[1]),
        trade_pr=parse_dec(record[7]),
        bid_pr=None,
        bid_size=None,
        ask_pr=None,
        ask_size=None,
        partition="T",
    )

def parse_csv(line: str) -> dict:
    record_type_pos = 2
    try:
        record = line.split(",")

        if len(record) <= record_type_pos:
            return bad_record_event()

        rtype = record[record_type_pos]

        if rtype == "Q":
            return parse_quote_record(record)
        elif rtype == "T":
            return parse_trade_record(record)
        else:
            return bad_record_event()

    except Exception:
        return bad_record_event()
