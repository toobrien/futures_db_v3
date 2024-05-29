from    datetime        import datetime, timedelta
import  update_cboe
import  update_cme
import  update_spot
import  update_wasde
import  update_futc
from    sys             import argv
from    time            import time

# example usage:
#
#   - python update.py
#   - python update.py 2024-02-12
#
#   The first form updates the database with the yesterday's settlements (available after midnight CST).
#   The second form attempts to update the database with a given day's settlement values.


if __name__ == "__main__":

    t0      = time()
    date    = datetime.strftime(datetime.today() - timedelta(days = 1), "%Y-%m-%d")
    new     = True

    if len(argv) > 1:

        date    = argv[1]
        new     = False

    update_cboe.update(date)
    update_cme.update(date)
    update_spot.update()
    update_wasde.update()
    update_futc.update()

    print(f"{'update':30s}{date:30s}{time() - t0:0.1f}")