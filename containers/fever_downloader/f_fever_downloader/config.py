import os
from datetime import datetime

DATA_BUCKET_NAME = os.environ["DATA_BUCKET_NAME"]
FEVER_GDRIVE_FILE_ID = os.environ["FEVER_GDRIVE_FILE_ID"]

now = datetime.now()
FORMATTED_DATE = now.strftime("%d-%m-%Y")
