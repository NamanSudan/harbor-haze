"""Run tests in UTC: test rows use naive datetimes, which Python converts with the local time zone."""

import os
import time

os.environ["TZ"] = "UTC"
time.tzset()
