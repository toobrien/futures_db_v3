from    bisect  import  bisect_left, bisect_right
from    os      import  listdir
import  polars  as      pl
from    time    import  time


# absolute paths for use in other modules

CFG = {
    "futc": ( "/Users/taylor/trading/daily/data/futc", "" ),
    "futs": ( "/Users/taylor/trading/daily/data/futs", "name"               ),
    "opts": ( "/Users/taylor/trading/daily/data/opts", "underlying_symbol"  )
}


def get_futc(
    symbol: str,
    start:  str = "1900-01-01",
    end:    str = "2999-01-01"
):
    
    path = CFG["futc"][0]

    df = pl.read_parquet(
        f"{path}/{symbol}.parquet"
    ).filter(
        (pl.col("date") >= start) &
        (pl.col("date") <= end)
    )
    
    return df


def cat_df(
    type:   str,
    symbol: str,
    start:  str, 
    end:    str
):

    path    = CFG[type][0]
    key     = CFG[type][1]
    fns     = sorted(listdir(path))[1:] 
    fns     = fns[bisect_left(fns, start) : bisect_right(fns, end)][1:] # skip .gitignore

    if fns:

        dfs = [ 
                pl.read_parquet(f"{path}/{fn}").filter(pl.col(key) == symbol)
                for fn in fns
            ]
        df  = pl.concat(dfs, how = "vertical")

    else:
        
        df = None

    return df


if __name__ == "__main__":

    t0      = time()
    df      = cat_df("futs", "ZW", "2022-01-01", "2024-01-01")
    t1      = time()
    print(f"read fut: {time() - t0:0.1f}")
    df_2    = cat_df("opts", "ZW", "2022-01-01", "2024-01-01")
    print(f"read opt: {time() - t1:0.1f}")