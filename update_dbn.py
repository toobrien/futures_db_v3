from contract_settings      import CONTRACT_SETTINGS
from datetime               import datetime
from enum                   import IntEnum
from json                   import dump, loads
from databento              import Historical
from pandas                 import concat, DataFrame
from typing                 import Dict, List
from time                   import time


# python daily_db.py 


# NOT COMPLETE OR USED


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
                "volume",
                "oi",
                "dte"
            ]
EXCHANGES   = { 
                settings["globex"] : settings["exchange"]
                for _, settings in CONTRACT_SETTINGS.items()
                if "globex" in settings
            }
GB          = 1073741824
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
VERBOSE     = False
PROMPT      = True


class rec(IntEnum):

    contract_id     = 0
    exchange        = 1
    name            = 2
    month           = 3
    year            = 4
    date            = 5
    open            = 6
    high            = 7
    low             = 8
    settle          = 9
    volume          = 10
    oi              = 11
    dte             = 12

OFFSET = -6


def format_recs(recs: List[List]):

    for r in recs:

        sym         = r[rec.name]
        exp         = expirations[sym].split("-")
        month       = MONTHS[int(exp[1])]
        year        = int(exp[0])
        sym         = sym[:-2] if not sym[-2].isdigit() else sym[:-3] # symMY or symMYY possible
        exchange    = EXCHANGES[sym]
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
    recs = [ r for r in recs if recs[rec.settle] ]

    if recs:

        df = DataFrame(recs, columns = DF_COLS)
        df.sort_values(by = [ "name", "dte" ])

        return df
    
    else: 
    
        # no settlement for date

        return None


if __name__ == "__main__":

    t0 = time()

    client          = Historical()
    rng             = client.metadata.get_dataset_range(dataset = "GLBX.MDP3")
    config_fd       = open("./config_dbn.json", "r+")
    config          = loads(config_fd.read())
    expirations_fd  = open("./expirations.json", "r+")
    expirations     = loads(expirations_fd.read())
    start           = config["daily_db_checkpoint"]
    end             = rng["end_date"]
    today           = datetime.today().strftime(DATE_FMT)
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

    t_cost = client.metadata.get_cost(**args)
    t_size = client.metadata.get_billable_size(**args)

    print(f"{start} - {end}")
    print(f"s_cost:         {t_cost:0.4f}")
    print(f"s_size:         {t_size} ({t_size / GB:0.2f} GB)")

    if PROMPT:

        go = input("continue? [y/n]: ").lower()

        if go == "n":

            print("aborted")

            exit()

    dfs = []

    for sym in syms:

        args["symbols"] = [ sym ]

        dfs.append(client.timeseries.get_range(**args).to_df())

    stats = concat(dfs, ignore_index = True)
    stats = stats[[ "symbol", "ts_event", "stat_type", "price", "quantity" ]]
    
    stats["date"] = stats["ts_event"].dt.date
    stats["date"] = stats["date"].astype(str)

    dates = stats["date"].unique()

    for date in dates:

        to_write[str(date)] = {}

    # record ohlcv and oi per symbol, date
        
    for _, row in stats.iterrows():

        date        = str(row["date"])
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

            sym_rec[rec.open + OFFSET] = row["price"]

        elif statistic == 3:

            sym_rec[rec.settle + OFFSET] = row["price"]
        
        elif statistic == 4:

            sym_rec[rec.low + OFFSET] = row["price"]

        elif statistic == 5:

            sym_rec[rec.high + OFFSET] = row["price"]
        
        elif statistic == 6:

            sym_rec[rec.volume + OFFSET] = row["quantity"]

        elif statistic == 9:

            sym_rec[rec.oi + OFFSET] = row["quantity"]

    # calc dte
    
    args["schema"]      = "definition"
    args["stype_in"]    = "raw_symbol"
    args["symbols"]     = None

    for _, row in stats.iterrows():

        date    = row["date"]
        symbol  = row["symbol"]
            
        if symbol not in to_write[date]:

            # spread or other non-tracked symbol

            continue

        if symbol not in expirations:

            # no expiration recorded, need to consult definition schema

            args["symbols"] = [ symbol ]
            dfns            = client.timeseries.get_range(**args)

            if VERBOSE:

                cost = client.metadata.get_cost(**args)
                size = client.metadata.get_billable_size(**args)

                t_cost += cost
                t_size += size
                
                print(f"{symbol}\t${cost:0.4f}\t{size}\t{size / GB:0.2f} GB")
            
            dfns            = dfns.to_df()

            # assume expiration is uniform across definition records

            expiry              = str(dfns.iloc[0]["expiration"].date())
            expirations[symbol] = expiry
        
        sym_rec = to_write[date][symbol]
        expiry  = expirations[symbol]
        dte     = (datetime.strptime(expiry, DATE_FMT) - datetime.strptime(date, DATE_FMT)).days

        sym_rec[rec.dte + OFFSET] = dte

    # batch by date, format records, and write to parquet
        
    for date, recs in to_write.items():

        df = to_df(date, recs)

        if df:

            df.to_parquet(path = f"{DB_PATH}/{date}.parquet")
    
    # write config, expirations

    config["daily_db_checkpoint"] = today

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
    
    print(f"t_cost:         {t_cost:0.4f}")
    print(f"t_size:         {t_size} ({t_size / GB:0.2f} GB)")
    print(f"checkpoint:     {today}")
    print(f"elapsed:        {time() - t0:0.1f}s")