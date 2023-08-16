A third version of the [`futures_db`](https://www.github.com/toobrien/futures_db) repo. The `v2` was quite slow to update, as everything was stored in a single parquet file. This version stores a separate file per day for both futures and futures options. The smaller spot price, WASDE, etc. databases are unchanged.

To upgrade from `v2`, simply run `from_v2.py` after ensuring that the `V2_FUTS_FILE` and `V2_OPTS_FILE` variables at the top of the file point to the appropriate files. To upgrade from `v1`, first run `from_v1.py` after ensuring that the connection string at the top of the file is pointing at your SQLITE database.

I have also included `cat_df.py` as an example of how to concatenate the files into a single dataframe. This file is now used with some of my other repositories, such as `spreads`.

