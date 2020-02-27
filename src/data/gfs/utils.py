"""

Introduction to GRIB2 Files

https://www.ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/intro_grib2.pdf
"""
import re
import pandas as pd

from dataclasses import dataclass, field

pat = re.compile(
    (
        r"(?P<message_num>\d+)"
        r"(\.(?P<submessage_num>\d+))?"
        r":(?P<location>\d+):"
        r"d=(?P<reference_time>\d+)"
        r":(?P<variable_name>\w+)"
        r":(?P<z>[^:]+)"
        r":(?P<dtime>[^:]+):"
    )
)


class InvalidGRIBInventory(Exception):
    pass


def parse_grib2_inventory_slug(s):
    match = pat.match(s)
    if match is None:
        raise InvalidGRIBInventory(f"{s} is not a valid GRIB2 inventory.")
    return match.groupdict()
