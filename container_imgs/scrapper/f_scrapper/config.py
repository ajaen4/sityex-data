import os
from datetime import datetime

DATA_BUCKET_NAME = os.environ["DATA_BUCKET_NAME"]

now = datetime.now()
# FORMATTED_DATE = now.strftime("%d-%m-%Y")
FORMATTED_DATE = "11-10-2023"
