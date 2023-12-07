.PHONY : install clean run

install:
	pip install -r requirements.txt
	pre-commit install

run_local_spark:
	docker run -it -v ~/.aws:/home/glue_user/.aws -v /Users/albertojaen/Desktop/glue_workspace:/home/glue_user/workspace/ -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 --name glue_pyspark amazon/aws-glue-libs:glue_libs_4.0.0_image_01 pyspark
