#!/usr/bin/env python
"""IB auto login

"""

import argparse
import mod_ib

parser = argparse.ArgumentParser(description='attempts to auto login into IB and stay logged in')
parser.add_argument('-timeout', type=int, help='timeout in seconds to connect to TWS')
parser.add_argument('-sleep', type=float, help='time in seconds between connection checks')
args = parser.parse_args()

mod_ib.TWS(auto_login=True, timeout=args.timeout, sleep=args.sleep)
