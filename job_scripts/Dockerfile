# Start from the AWS Glue base image
FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

# Switch to root to install additional libraries
USER root

# Install polars and deep_translator
RUN pip3 install polars deep-translator

# Switch back to the default user
USER glue_user
