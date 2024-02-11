A third version of the [`futures_db`](https://www.github.com/toobrien/futures_db) repo. The `v2` was quite slow to update, as everything was stored in a single parquet file. This version stores a separate file per day for both futures and futures options. The smaller spot price, WASDE, etc. databases are unchanged.

To upgrade from `v2`, simply run `from_v2.py` after ensuring that the `V2_FUTS_FILE` and `V2_OPTS_FILE` variables at the top of the file point to the appropriate files. To upgrade from `v1`, first run `from_v1.py` after ensuring that the connection string at the top of the file is pointing at your SQLITE database.

I have also included `cat_df.py` as an example of how to concatenate the files into a single dataframe. This file is now used with some of my other repositories, such as `spreads`.

IMPORTANT:

Since April 1st, 2024, the CME FTP will no longer be active. The settlement data that was once available on the FTP now exists in the "STLBASICPLS" data set on CME DataMine. This repository has been updated to pull from DataMine.

You will need to register for the CBOT, CME, COMEX, and NYMEX datasets here: https://datamine.cmegroup.com/#/datasets/STLBASICPLS

You will also need sign up for DataMine API access to receive an API id and key. See instructions here: https://www.cmegroup.com/market-data/datamine-api.html

Once you have an API id and key, store them in your environment and edit the `datamine_id` and `datamine_pass` keys from `config.py` with the relevant variable names.

Note that datamine is updated at midnight CST; therefore, running `update.py` with no date argument will default to querying the API for yesterday's settlements. To override this behavior, you can simply pass today's date in YYYY-MM-DD format as the first argument.