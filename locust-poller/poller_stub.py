#  Stub file to call locust_poller.py
#  BJB 5/7/24
from bbutil import Util
import sys

# --------------------------------------------------------- #
#       MAIN
# --------------------------------------------------------- #
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    from locust_loader import MetricsLocust
    widget = MetricsLocust("BradyByrd")
    if "update" in ARGS:
        bb.message_box("Performing Update")
        result = widget._bulkupdate()
    else:
        bb.message_box("Performing Initial Inserts")
        result = widget._bulkinsert()
    