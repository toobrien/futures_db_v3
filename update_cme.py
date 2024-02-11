from    config              import  CONFIG
from    contract_settings   import  CONTRACT_SETTINGS
from    csv                 import  reader
from    datetime            import  datetime
from    ftplib              import  FTP
from    os                  import  environ
from    os.path             import  exists
import  polars              as      pl
from    requests            import  get
from    time                import  time
from    typing              import  List


DATE_FMT            = "%Y-%m-%d"
ENABLED_FUTS        = {
                        settings["globex"] : settings
                        for _, settings in CONTRACT_SETTINGS.items()
                        if "globex" in settings
                    }
ENABLED_OPTS        = {
                        definition["opts"]: {
                            "globex":   definition["globex"], 
                            "exchange": definition["exchange"]
                        }
                        for _, definition in CONTRACT_SETTINGS.items()
                        if "opts" in definition
                    }
MONTHS              = {
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


# cme record format

# 0  BizDt          current date        YYYY-MM-DD
# 1  Sym            globex symbol       ZN
# 2  ID             clearing symbol     21
# 3  StrkPx         strike price        float
# 4  SecTyp         security type       FUT, OOF, COMBO, OOC
# 5  MMY            exp y+m             YYYYMM
# 6  MatDt          ?                   YYYY-MM-DD          
# 7  PutCall        put or call         1 or 0
# 8  Exch           exchange            NYMEX
# 9  Desc           always empty        "" 
# 10 LastTrdDt      last trade date     YYYY-MM-DD
# 11 BidPrice       ?                   float
# 12 OpeningPrice   open                float
# 13 SettlePrice    settle              float
# 14 SettleDelta    delta settle?       float
# 15 HighLimit      high bid/offer?     float
# 16 LowLimit       low bid/offer?      float
# 17 DHighPrice     high                float
# 18 DLowPrice      low                 float
# 19 HighBid        ?                   float    
# 20 LowBid         ?                   float
# 21 PrevDayVol     volume              int
# 22 PrevDayOI      OI                  int
# 23 FixingPrice    ?                   float
# 24 UndlyExch      underlying exchange NYMEX
# 25 UndlyID        underlying clearing 21
# 26 UndlySecTyp    underlying sectype  FUT, OOF, COMBO, OOC
# 27 UndlyMMY       exp y+m             YYYYMM
# 28 BankBusDay     ?                   YYYY-MM-DD


EXPECTED_COLS = [
    "BizDt",
    "Sym",
    "ID",
    "StrkPx",
    "SecTyp",
    "MMY",
    "MatDt",
    "PutCall",
    "Exch",
    "Desc",
    "LastTrdDt",
    "BidPrice",
    "OpeningPrice",
    "SettlePrice",
    "SettleDelta",
    "HighLimit",
    "LowLimit",
    "DHighPrice",
    "DLowPrice",
    "HighBid",
    "LowBid",
    "PrevDayVol",
    "PrevDayOI",
    "FixingPrice",
    "UndlyExch",
    "UndlyID",
    "UndlySecTyp",
    "UndlyMMY",
    "BankBusDay"
]


def get_futs_cols(rows: List):

    id_col          = []
    exchange_col    = []
    symbol_col      = []
    month_col       = []
    year_col        = []
    date_col        = []
    open_col        = []
    high_col        = []
    low_col         = []
    settle_col      = []
    vol_col         = []
    oi_col          = []
    dte_col         = []

    rows = reader(rows)
    
    next(rows)  # skip header

    for row in rows:

        if row[4] != "FUT" or row[1] not in ENABLED_FUTS:

            continue

        symbol      = row[1]
        exchange    = ENABLED_FUTS[symbol]["exchange"]
        delivery    = row[5]
        year        = delivery[0:4]
        month       = None

        # format varies: yyyymm vs yyyymmdd

        if len(delivery) == 6:

            month = MONTHS[int(delivery[4:])]

        else:

            month = MONTHS[int(delivery[4:6])]

        scale = ENABLED_FUTS[symbol]["scale"]

        id          = f"{exchange}_{symbol}{month}{year}"
        date        = row[0]
        settle      = float(row[13]) * scale if row[13] != "" else None
        open        = float(row[12]) * scale if row[12] != "" else None
        high        = float(row[17]) * scale if row[17] != "" else None
        low         = float(row[18]) * scale if row[18] != "" else None
        vol         = int(row[21])           if row[21] != "" else None
        oi          = int(row[22])           if row[22] != "" else None
        exp_date    = row[10]
        dte         = (datetime.strptime(exp_date, DATE_FMT) - datetime.strptime(date, DATE_FMT)).days

        id_col.append(id)
        exchange_col.append(exchange)
        symbol_col.append(symbol)
        month_col.append(month)
        year_col.append(year)
        date_col.append(date)
        open_col.append(open)
        high_col.append(high)
        low_col.append(low)
        settle_col.append(settle)
        vol_col.append(vol)
        oi_col.append(oi)
        dte_col.append(dte)

    df = pl.from_dict(
        {
            "contract_id":  id_col,
            "exchange":     exchange_col,
            "name":         symbol_col,
            "month":        month_col,
            "year":         year_col,
            "date":         date_col,
            "open":         open_col,
            "high":         high_col,
            "low":          low_col,
            "settle":       settle_col,
            "volume":       vol_col,
            "oi":           oi_col,
            "dte":          dte_col
        },
        schema = CONFIG["futs_schema"]
    )

    return df


def get_opts_cols(rows: List):

    date_col                = []
    name_col                = []
    strike_col              = []
    expiry_col              = []
    call_col                = []
    last_traded_col         = []
    settle_col              = []
    settle_delta_col        = []
    high_limit_col          = []
    low_limit_col           = []
    high_bid_col            = []
    low_bid_col             = []
    previous_volume_col     = []
    previous_interest_col   = []
    underlying_symbol_col   = []
    underlying_exchange_col = []
    underlying_id_col       = []

    rows = reader(rows)
    
    next(rows)  # skip header

    for row in rows:

        underlying_symbol   = row[25]
        con_type            = row[4]

        if underlying_symbol in ENABLED_OPTS and con_type == "OOF":

            underlying_exchange = ENABLED_OPTS[underlying_symbol]["exchange"]
            underlying_symbol   = ENABLED_OPTS[underlying_symbol]["globex"]

            date                = row[0]
            name                = row[1]
            strike              = float(row[3])
            expiry              = row[6]
            call                = int(row[7])
            last_traded         = row[10]
            settle              = float(row[13]) if row[13] != "" else None
            settle_delta        = float(row[14]) if row[14] != "" else None
            high_limit          = float(row[15]) if row[15] != "" else None
            low_limit           = float(row[16]) if row[16] != "" else None
            high_bid            = float(row[19]) if row[19] != "" else None
            low_bid             = float(row[20]) if row[20] != "" else None
            previous_volume     = int(row[21])   if row[21] != "" else None
            previous_interest   = int(row[22])   if row[22] != "" else None
            underlying_month    = MONTHS[int(row[27][-2:])]
            underlying_year     = row[27][0:4]
            underlying_id       = f"{underlying_exchange}_{underlying_symbol}{underlying_month}{underlying_year}"

            date_col.append(date)
            name_col.append(name)
            strike_col.append(strike)
            expiry_col.append(expiry)
            call_col.append(call)
            last_traded_col.append(last_traded)
            settle_col.append(settle)
            settle_delta_col.append(settle_delta)
            high_limit_col.append(high_limit)
            low_limit_col.append(low_limit)
            high_bid_col.append(high_bid)
            low_bid_col.append(low_bid)
            previous_volume_col.append(previous_volume)
            previous_interest_col.append(previous_interest)
            underlying_symbol_col.append(underlying_symbol)
            underlying_exchange_col.append(underlying_exchange)
            underlying_id_col.append(underlying_id)
            
    df = pl.from_dict(
        {
            "date":                 date_col,
            "name":                 name_col,
            "strike":               strike_col,
            "expiry":               expiry_col,
            "call":                 call_col,
            "last_traded":          last_traded_col,
            "settle":               settle_col,
            "settle_delta":         settle_delta_col,
            "high_limit":           high_limit_col,
            "low_limit":            low_limit_col,
            "high_bid":             high_bid_col,
            "low_bid":              low_bid_col,
            "previous_volume":      previous_volume_col,
            "previous_interest":    previous_interest_col,
            "underlying_symbol":    underlying_symbol_col,
            "underlying_exchange":  underlying_exchange_col,
            "underlying_id":        underlying_id_col
        },
        schema = CONFIG["opts_schema"]
    )

    return df


def update(date: str):

    t0 = time()
    
    date    = date.replace("-", "")
    dm_url  = CONFIG["datamine_url"]
    dm_cfgs = CONFIG["datamine_configs"]
    dm_user = environ[CONFIG["datamine_id"]]
    dm_pass = environ[CONFIG["datamine_pass"]]
    creds   = (dm_user, dm_pass) 
    futs_fn = f"{CONFIG['futs_path']}/{date}.parquet"
    opts_fn = f"{CONFIG['opts_path']}/{date}.parquet"
    futs_df = pl.read_parquet(futs_fn) if exists(futs_fn) else pl.DataFrame()
    opts_df = pl.read_parquet(opts_fn) if exists(opts_fn) else pl.DataFrame()
    urls    = [
                dm_url.format(date, *cfg)
                for cfg in dm_cfgs
            ]

    for url in urls:
        
        t1 = time()

        res = get(url, auth = creds)

        print(f"{'GET':30}{f'{url} {res.status_code}':100}{time() - t1:0.1f}s")

        if res.status_code != 200:

            continue

        rows = res.text.split()

        if rows[0].split(",") != EXPECTED_COLS:

            print(f"error: unexpected column format in {url}")

        t2 = time()

        df = get_futs_cols(rows)

        if not futs_df.is_empty():
        
            futs_df.extend(df)
        
        else:

            futs_df = df

        print(f"{'update_cme.insert_fut_rows':30}{url:100}{time() - t2:0.1f}s")
        
        t3 = time()

        df = get_opts_cols(rows)

        if not opts_df.is_empty():

            opts_df.extend(df)
        
        else:

            opts_df = df

        print(f"{'update_cme.insert_opt_rows':30s}{url:100}{time() - t3:0.1f}s")

        pass

    t5 = time()

    futs_df = futs_df.unique(maintain_order = True).sort([ "contract_id", "date" ])
    futs_df.write_parquet(futs_fn)

    print(f"{'update_cme:write_futs_db':30s}{url:100}{time() - t5:0.1f}s")

    t6 = time()

    opts_df = opts_df.unique(maintain_order = True).sort([ "date", "name", "expiry", "strike" ])
    opts_df.write_parquet(opts_fn)

    print(f"{'update_cme:write_opts_db':30s}{url:100}{time() - t6:0.1f}s")

    print(f"{'update_cme.update':30}{time() - t0:0.1f}s")