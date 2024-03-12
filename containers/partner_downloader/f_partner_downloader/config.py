import os
from datetime import datetime

DATA_BUCKET_NAME = os.environ["DATA_BUCKET_NAME"]
HOUSING_ANYWHERE_URL = os.environ["HOUSING_ANYWHERE_URL"]
UNIPLACES_URL = os.environ["UNIPLACES_URL"]

now = datetime.now()
FORMATTED_DATE = now.strftime("%d-%m-%Y")
