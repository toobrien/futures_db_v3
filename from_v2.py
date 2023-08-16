from    config  import  CONFIG
import  polars  as      pl
from    sys     import  argv
from    time    import  time

pl.Config.set_tbl_cols(17)
pl.Config.set_tbl_rows(1000)

V2_FUTS_FILE   = "./futs.parquet"
V2_OPTS_FILE   = "./opts.parquet"

V3_FUTS_PATH   = CONFIG["futs_path"]
V3_OPTS_PATH   = CONFIG["opts_path"]


def convert(infile: str, outpath: str):

    df      = pl.read_parquet(infile)
    groups  = df.groupby(["date"])

    for group in groups:

        date    = group[0][0]
        df      = group[1]
        fn      = f"{outpath}/{date}.parquet"

        df.write_parquet(fn)

if __name__ == "__main__":

    t0 = time()

    if "futs" in argv:

        convert(V2_FUTS_FILE, V3_FUTS_PATH)

        print(f"futs converted in {time() - t0:0.1f}s")

        t1 = time()
    
    if "opts" in argv:

        convert(V2_OPTS_FILE, V3_OPTS_PATH)

        print(f"opts converted in {time() - t1:0.1f}s")

    print(f"elapsed: {time() - t0:0.1f}s")