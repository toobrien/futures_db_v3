from contract_settings  import CONTRACT_SETTINGS
from datetime           import datetime
from enum               import IntEnum
from json               import dump, loads
from databento          import Historical
from pandas             import DataFrame
from typing             import Dict, List
from time               import time


# python daily_db.py 


DATE_FMT    = "%Y-%m-%d"
DB_PATH     = "./.futs"
DF_COLS     = [ 
                "contract_id",
                "exchange",
                "name",
                "month",
                "year",
                "date",
                "open",
                "high",
                "low",
                "settle",
                "oi",
                "dte"
            ]
MONTHS      = {
                1: "F",
                2: "G",
                3: "H",
                4: "J",
                5: "K",
                6: "M",
                7: "N",
                8: "Q",
                9: "U",
                10: "V",
                11: "X",
                12: "Z"
            }


class rec(IntEnum):

    contract_id     = 0
    exchange        = 1
    name            = 1
    month           = 2
    year            = 3
    date            = 4
    open            = 5
    high            = 6
    low             = 7
    settle          = 8
    volume          = 9
    oi              = 10
    dte             = 11


def format_recs(recs: List[List]):

    for r in recs:

        sym         = r[rec.name]
        exp         = expirations[sym].split("-")
        month       = MONTHS[int(exp[1])]
        year        = int(exp[0])
        sym         = sym[:-2] if not sym[-2].isdigit() else sym[:-3] # symMY or symMYY possible
        exchange    = None # TODO: get from config
        contract_id = f"{exchange}_{sym}{month}{year}"

        r[rec.contract_id]  = contract_id
        r[rec.exchange]     = exchange
        r[rec.name]         = sym
        r[rec.month]        = month
        r[rec.year]         = year

    return recs


def to_df(
    date: str,
    recs: Dict[str, List[float]]
):

    recs = [
        [
            None,
            None,
            sym,
            None,
            None,
            date, 
            *rec 
        ]
        for sym, rec in recs.items()
    ]

    recs = format_recs(recs)

    df = DataFrame(recs, columns = DF_COLS)

    df.sort_values(by = [ "name", "dte" ])

    return df


if __name__ == "__main__":

    t0 = time()

    client          = Historical()
    rng             = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    config_fd       = open("./config.json", "r+") # TODO: refactor config
    config          = loads(config_fd.read())
    expirations_fd  = open("./expirations.json", "r+") # TODO: refactor config
    expirations     = loads(expirations_fd.read())
    start           = config["daily_db_checkpoint"]
    end             = rng["end_date"]
    syms            = config["daily_db_futs"]
    args            = {
                        "dataset":      "GLBX.MDP3",
                        "symbols":      syms,
                        "schema":       "statistics",
                        "stype_in":     "parent",
                        "start":        start,
                        "end":          end
                    }
    to_write        = {}

    cost = client.metadata.get_cost(**args)
    size = client.metadata.get_billable_size(**args)

    print(f"{start} - {end}")
    print(f"cost:       {cost:0.4f}")
    print(f"size:       {size} ({size / 1073741824:0.2f} GB)")

    stats = client.timeseries.get_range(**args)
    stats = stats.to_df()
    stats = stats[[ "symbol", "ts_event", "stat_type", "price", "quantity" ]]
    
    stats["date"] = stats["ts_event"].dt.date

    dates = stats["date"].dt.date.unique()

    for date in dates:

        to_write[date] = {}

    # record ohlcv and oi per symbol, date
        
    for row in stats:

        date        = row["date"]
        symbol      = row["symbol"]
        batch       = to_write[date]
        statistic   = row["stat_type"]

        # skip spreads, etc.

        if ":" in symbol or "-" in symbol or " " in symbol:

            continue

        # https://docs.databento.com/knowledge-base/new-users/fields-by-schema/statistics-statistics
        
        if symbol not in batch:

            batch[symbol] = [ None, None, None, None, None, None, None ]

        sym_rec = batch[symbol]

        if statistic == 1:

            sym_rec[rec.open] = row["price"]

        elif statistic == 3:

            sym_rec[rec.settle] = row["price"]
        
        elif statistic == 4:

            sym_rec[rec.low] = row["price"]

        elif statistic == 5:

            sym_rec[rec.high] = row["price"]
        
        elif statistic == 6:

            sym_rec[rec.volume] = row["quantity"]

        elif statistic == 9:

            sym_rec[rec.open_interest] = row["quantity"]

    # calc dte
    
    args["schema"]      = "definition"
    args["stype_in"]    = "raw_symbol"
    args["symbols"]     = None

    for row in stats:

        date    = row["date"]
        symbol  = row["symbol"]
            
        if symbol not in to_write[date]:

            # spread or other non-tracked symbol

            continue

        if symbol not in expirations:

            # no expiration recorded, need to consult definition schema

            args["symbols"] = [ symbol ]        
            dfns            = client.timeseries.get_range(**args)
            dfns            = dfns.to_df()

            # assume expiration is uniform across definition records

            expiry              = dfns.iloc[0]["expiration"].dt.date
            expirations[symbol] = expiry
        
        sym_rec = to_write[date][symbol]
        expiry  = expirations[symbol]
        dte     = (datetime.strptime(expiry, DATE_FMT) - datetime.strptime(date, DATE_FMT)).days

        sym_rec[rec.dte] = dte

    # batch by date, format records, and write to parquet
        
    for date, recs in to_write.items():

        df = to_df(date, recs)

        df.to_parquet(path = f"{DB_PATH}/{date}.parquet")
    
    # write config, expirations

    config["daily_db_checkpoint"] = end

    for to_write in [ 
        (config, config_fd),
        (expirations, expirations_fd)
    ]:

        json    = to_write[0]
        fd      = to_write[1]

        fd.seek(0)
        dump(json, fd)
        fd.truncate()
        fd.close()
    
    print(f"elapsed:    {time() - t0:0.1f}s")