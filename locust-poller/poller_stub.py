#  Stub file to call locust_poller.py
#  BJB 5/7/24

from locust_poller import MetricsLocust

# --------------------------------------------------------- #
#       MAIN
# --------------------------------------------------------- #
if __name__ == "__main__":
    widget = MetricsLocust("BradyByrd")
    result = widget._bulkinsert()
    