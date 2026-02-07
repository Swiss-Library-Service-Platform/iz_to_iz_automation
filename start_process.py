# This script is used to start the workflow. It takes the size of the dataset as an argument.
#
# To start the workflow with a small dataset, run the following command:
# python start_process.py -size SMALL
#
# To start the workflow with a large dataset, run the following command:
# python start_process.py -size LARGE

import sys
import os
import dotenv
from pymongo import MongoClient

dotenv.load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from speibiutils.workflow import start

if sys.argv[1] == '-size' and sys.argv[2] in ['SMALL', 'LARGE']:

    row = start(size=sys.argv[2])
    if sys.argv[2] == 'LARGE':
        # Add report to MongoDB
        client = MongoClient(os.getenv('MONGODB_URI'))
        db = client['automated_processes']
        collection = db['VKSS_Einlagerung']

        collection.insert_one(row)

        client.close()
else:
    print('Usage: python start_process.py -size [SMALL|LARGE]')
    sys.exit(1)
