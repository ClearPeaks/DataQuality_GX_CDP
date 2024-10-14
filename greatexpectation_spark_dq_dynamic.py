import great_expectations as gx
from pyspark.sql import SparkSession
from great_expectations.checkpoint import Checkpoint
import sys
import datetime
import os
import json
from pyspark import Row
from pyspark.sql.functions import to_timestamp, from_json, col


def getRulesDF(spark):
    # Check if the command-line argument is provided and not empty
    if len(sys.argv) > 1 and sys.argv[1]:
        # If an argument is provided, include the WHERE clause
        query = f"""
        SELECT r.MODULE, r.DATABASE, r.TABLE, r.COLUMN, r.RULE_ID, r.PARAMETERS
        FROM datawash.gx_rules r
        WHERE r.RULE_GROUP = '{sys.argv[1]}'
        """
    else:
        # If no argument is provided, execute the query without the WHERE clause
        query = """
        SELECT r.MODULE, r.DATABASE, r.TABLE, r.COLUMN, r.RULE_ID, r.PARAMETERS
        FROM datawash.gx_rules r
        """

    return spark.sql(query)


# Initialize Great Expectations context
print("-----------------------Initialize great_expectations--------------------------")
context = gx.get_context()

# Define data source
datasource = context.sources.add_or_update_spark("my_spark_datasource")
name = "my_df_asset"
data_asset = datasource.add_dataframe_asset(name=name)

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("hive_datasource") \
    .config("spark.sql.DataWashImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Define date and timestamp for file naming
date = datetime.datetime.now().strftime("%Y%m%d")
timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

# Define input parameters
dbname = "dqtest"

# Fetch the rules
print("-----------------------rules dataframe creation started--------------------------")
rulesDf = getRulesDF(spark)
print("-----------------------rules dataframe creation end--------------------------")
rows = rulesDf.collect()

for row in rows:
    module = row['MODULE']
    database = row['DATABASE']
    table = row['TABLE']
    column = row['COLUMN']
    rule_id = row['RULE_ID']
    parameters = row['PARAMETERS']

    try:
        print("-----------------------fetch result from source table--------------------------")
        datasourceDF = spark.sql(f"SELECT {column} FROM {database}.{table}")
        datasourceDF.show()

        # Create batch request for Great Expectations
        print("-----------------------create batch request started--------------------------")
        batch_request = data_asset.build_batch_request(dataframe=datasourceDF)

        # Define and update expectation suite
        expectation_suite_name = f"analyse_{database}.{table}"
        context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

        # Create validator
        print("-----------------------create validator started--------------------------")
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=expectation_suite_name,
        )

        print("-----------------------expectation's call started--------------------------")
        # Dynamically apply expectation based on the module value
        # The expectations which required extra parameter, this expectation should configure separately in additional if block
        # for example validator.expect_column_values_to_be_between(column="age", min_value=18, max_value=32),
        # require 2 additional parameter min and max
        if module == 'expect_column_values_to_be_between' and parameters:
            # Parse JSON from parameters column and extract min and max
            parsed_json = json.loads(parameters)
            min_value = parsed_json.get("min")
            max_value = parsed_json.get("max")

            # Apply the expectation with min and max values
            validator.expect_column_values_to_be_between(column, min_value=min_value, max_value=max_value)
        else:
            # Apply other expectations based on the module value
            # The expectations which doesn't require extra parameter, will get executed in the else block dynamically
            # The getattr() function returns the value of the module attribute from the validator object.
            # where module is expectation e.g: expect_column_values_to_not_be_null
            expectation_method = getattr(validator, module)
            expectation_method(column)

        # Save expectation suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("-----------------------expectation's call end--------------------------")

        # Define checkpoint
        print("-----------------------GX checkpoint creation started--------------------------")
        my_checkpoint_name = "spark_checkpoint"
        checkpoint = Checkpoint(
            name=my_checkpoint_name,
            run_name_template=f"{timestamp}-{table}",
            data_context=context,
            batch_request=batch_request,
            expectation_suite_name=expectation_suite_name,
            action_list=[
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"}
                },
            ],
        )

        # Add or update checkpoint
        context.add_or_update_checkpoint(checkpoint=checkpoint)

        # Run checkpoint
        checkpoint_result = checkpoint.run()
        PATH = f"great_expectation/{date}"
        if not os.path.exists(PATH):
            os.makedirs(PATH)

        # Serialize checkpoint result
        checkpoint_result_serializable = checkpoint_result.to_json_dict()

        # Write validation result to file
        print("-----------------------Write validation result to file--------------------------")
        with open(f"{PATH}/{table}_validation_check_{timestamp}.json", 'w') as f:
            json.dump(checkpoint_result_serializable, f, indent=4)

        # Read the file
        print("-----------------------Read the file validation result--------------------------")
        with open(f"{PATH}/{table}_validation_check_{timestamp}.json", 'r') as file:
            data = json.load(file)

        # Extract dynamic key from "run_results"
        run_results = data["run_results"]
        dynamic_key = list(run_results.keys())[0]
        validation_results = run_results[dynamic_key]["validation_result"]["results"]
        run_time = data["run_id"]["run_time"]

        # Extract required fields
        rows = []
        for result in validation_results:
            success = result["success"]
            expectation_type = result["expectation_config"]["expectation_type"]
            element_count = result["result"]["element_count"]
            column_name = result["expectation_config"]["kwargs"]["column"]
            unexpected_count = result["result"]["unexpected_count"]
            unexpected_percent = result["result"].get("unexpected_percent", 0.0)

            rows.append(Row(run_time=run_time, success=success, expectation_type=expectation_type,
                            element_count=element_count, column_name=column_name,
                            unexpected_count=unexpected_count, unexpected_percent=unexpected_percent,
                            tablename=database + '.' + table))

        # Create DataFrame
        df = spark.createDataFrame(rows)
        df = df.withColumn("run_time", to_timestamp(df["run_time"]))
        df.show()

        # Write DataFrame to Hive table
        df.write.mode('append').format("hive").saveAsTable("datawash.greatexpectation_result")

    except Exception as e:
        print(f"An error occurred processing table {table}: {e}", file=sys.stderr)

# Stop SparkSession
spark.stop()
