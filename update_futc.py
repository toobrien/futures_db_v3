from config import CONFIG
from os     import listdir
from polars import concat, read_parquet
from time   import time


def update():

    t0 = time()

    futs_path = CONFIG["futs_path"]
    futc_path = CONFIG["futc_path"]

    fns = sorted(listdir(futs_path))[2:]
    fns = [ f"{futs_path}/{fn}" for fn in fns ]

    dfs     = [ read_parquet(fn) for fn in fns ]
    df      = concat(dfs)
    groups  = df.group_by([ "name" ], maintain_order = True)

    for name, df in groups:

        df.write_parquet(f"{futc_path}/{name[0]}.parquet")

    print(f"{'update_futc.update':30}{time() - t0:0.1f}s")


if __name__ == "__main__":

    update()