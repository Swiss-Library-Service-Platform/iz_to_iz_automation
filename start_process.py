# This script is used to start the workflow. It takes the size of the dataset as an argument.
#
# To start the workflow with a small dataset, run the following command:
# python start_process.py -size SMALL
#
# To start the workflow with a large dataset, run the following command:
# python start_process.py -size LARGE

import sys
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from speibiutils.workflow import start

if sys.argv[1] == '-size' and sys.argv[2] in ['SMALL', 'LARGE']:
    start(size=sys.argv[2])