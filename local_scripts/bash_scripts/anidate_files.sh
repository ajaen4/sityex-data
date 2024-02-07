#!/bin/bash

BUCKET_NAME="sityex-public-documents"
PARENT_FOLDER="documents"

# List all objects in the bucket
aws s3 ls s3://$BUCKET_NAME/ --recursive | while read -r line; do
    # Extract the file name
    FILENAME=$(echo $line | awk '{print $4}')
    
    # Skip if line is empty
    if [ -z "$FILENAME" ]; then
        continue
    fi

    # Copy the file to the new location within the same bucket
    NEW_KEY="${PARENT_FOLDER}/${FILENAME}"
    aws s3 cp s3://$BUCKET_NAME/$FILENAME s3://$BUCKET_NAME/$NEW_KEY
    echo "Moved $FILENAME to $NEW_KEY"

    # Remove the original file after copying
    echo "Deleting $FILENAME"
    aws s3 rm s3://$BUCKET_NAME/$FILENAME
done
