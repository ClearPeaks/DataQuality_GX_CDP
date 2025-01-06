#test commit
import great_expectations as gx
from pyspark.sql import SparkSession
from great_expectations.checkpoint import Checkpoint
import sys
import datetime
import os
import json
from pyspark import Row
from pyspark.sql.functions import to_timestamp, from_json, col, when, lit
from pyspark.sql import functions as F

# Initialize Great Expectations context
context = gx.get_context()

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

def getRulesDF(spark):
    if len(sys.argv) > 1 and sys.argv[1]:
        query = f"""
        SELECT r.MODULE, r.DATABASE, r.TABLE, r.COLUMN, r.RULE_ID,r.RULE_TYPE, r.PARAMETERS, CONCAT(r.DATABASE, '.', r.TABLE) AS DATABASE_TABLE
        FROM datawash.gx_rules r
        WHERE r.RULE_GROUP = '{sys.argv[1]}' and r.ENABLE=1
        """
    else:
        query = """
        SELECT r.MODULE, r.DATABASE, r.TABLE, r.COLUMN, r.RULE_ID,r.RULE_TYPE, r.PARAMETERS, CONCAT(r.DATABASE, '.', r.TABLE) AS database_table
        FROM datawash.gx_rules r where r.ENABLE=1
        """
    return spark.sql(query)

def expect_column_values_to_be_between(validator, column, parameters):
    parsed_json = json.loads(parameters)
    min_value = parsed_json.get("min")
    max_value = parsed_json.get("max")
    validator.expect_column_values_to_be_between(column, min_value=min_value, max_value=max_value)

def expect_column_values_to_be_greater_than(validator, column, parameters):
    parsed_json = json.loads(parameters)
    min_value = parsed_json.get("min")
    validator.expect_column_values_to_be_between(column, min_value=min_value)

def expect_column_value_lengths_to_be_between(validator, column, parameters):
    parsed_json = json.loads(parameters)
    min_value = parsed_json.get("min")
    max_value = parsed_json.get("max")
    validator.expect_column_value_lengths_to_be_between(column, min_value=min_value, max_value=max_value)

def expect_column_values_to_be_in_set(validator, column, parameters):
    parsed_json = json.loads(parameters)
    value_set = parsed_json.get("value_set", [])
    validator.expect_column_values_to_be_in_set(column, value_set=value_set)

def expect_column_values_to_not_be_in_set(validator, column, parameters):
    parsed_json = json.loads(parameters)
    value_set = parsed_json.get("value_set", [])
    validator.expect_column_values_to_not_be_in_set(column, value_set=value_set)

def expect_column_values_to_match_regex(validatexpectation_suite_nameor, column, parameters):
    parameters = parameters.strip().strip("'")
    if parameters.startswith("\\"):
        parameters = parameters[1:]
    parsed_json = json.loads(parameters)
    regex_pattern = parsed_json.get("regex_pattern")
    validator.expect_column_values_to_match_regex(column, regex=regex_pattern)

def expect_column_values_to_not_match_regex(validator, column, parameters):
    parameters = parameters.strip().strip("'")
    if parameters.startswith("\\"):
        parameters = parameters[1:]
    parsed_json = json.loads(parameters)
    regex_pattern = parsed_json.get("regex_pattern")
    validator.expect_column_values_to_match_regex(column, regex=regex_pattern)

def expect_calculated_age_to_be_greater_than(validator, column, parameters, sampleDF):
    parameters = parameters.strip().strip("'")
    if parameters.startswith("\\"):
        parameters = parameters[1:]
    parsed_json = json.loads(parameters)
    min_age = parsed_json.get("min_age", 16)
    validator.expect_column_values_to_be_between(column="age", min_value=min_age)

def expect_custom_rule_id_type(validator, id_type_column, customer_id_column, sampleDF):
    # 1. Check if id_type is 'national_id', customer_id should be numeric and non-null
    national_id_df = sampleDF.filter(F.col(id_type_column) == 'national_id')
    if not national_id_df.rdd.isEmpty():
        validator.expect_column_values_to_not_be_null(customer_id_column)
        validator.expect_column_values_to_match_regex(customer_id_column, r'^\d+$')

    # 2. Check if id_type is 'passport_id', customer_id should be alphanumeric and non-null
    passport_id_df = sampleDF.filter(F.col(id_type_column) == 'passport_id')
    if not passport_id_df.rdd.isEmpty():
        validator.expect_column_values_to_not_be_null(customer_id_column)
        validator.expect_column_values_to_match_regex(customer_id_column, r'^[a-zA-Z0-9]+$')

    # 3. Filter out records where id_type is 'anonymous_id' (no validation needed for these rows)
    filtered_df = sampleDF.filter(F.col(id_type_column) != 'anonymous_id')

    # 4. Ensure id_type column is not blank
    validator.expect_column_values_to_not_be_null(id_type_column)
    validator.expect_column_values_to_not_match_regex(id_type_column, r'^\s*$')

    return filtered_df

def expect_column_values_to_be_valid_country(validator, column):
    valid_countries = [
        "Afghanistan", "Bahrain", "Belgium", "Benin", "Bouvet Island", "Brunei",
        "Burkina Faso", "Cameroon", "Congo", "Cuba", "Eritrea", "Finland", "France",
        "Greece", "Italy", "Malawi", "Namibia", "Netherlands", "New Zealand", "Nigeria",
        "Portugal", "Serbia", "Somalia", "Swaziland", "Tanzania", "Thailand", "Uganda",
        "Zimbabwe", "Angola", "Antigua and Barbuda", "China", "Denmark", "Egypt",
        "Georgia, Republic of", "Germany", "Niger", "Philippines", "South Africa",
        "Sri Lanka", "Sudan", "Switzerland", "Turkey", "Turks and Caicos Islands",
        "United Kingdom", "United States", "Argentina", "Bonaire", "Bosnia and Herzegovina",
        "Canada", "East Timor", "Gabon", "India", "Korea", "Mozambique", "Russia",
        "Rwanda", "Spain", "Sweden", "Turkmenistan", "United States Minor Outlying Islands",
        "Zambia", "Aruba", "Australia", "Bahamas", "Bangladesh", "Botswana", "Brazil",
        "British Indian Ocean Territory", "Burundi", "Chad", "Dem. Republic of Congo",
        "Ghana", "Guinea", "Israel", "Japan", "Kenya", "Lesotho", "Liberia", "Madagascar",
        "Pakistan", "Romania", "Sierra Leone", "Tuvalu"
    ]
    validator.expect_column_values_to_be_in_set(column, value_set=valid_countries)


# Fetch the rules
rulesDf = getRulesDF(spark)

# Get distinct database_table values from rulesDf
distinct_tables = rulesDf.select("DATABASE_TABLE").distinct().collect()

#rows = rulesDf.collect()
# Loop through each distinct database_table
for table in distinct_tables:

    try:
        table_value = table['DATABASE_TABLE']
        print(f"Processing table: {table_value}")

        # Filter rulesDf for rows related to the current table
        filtered_rulesDf = rulesDf.filter(rulesDf['DATABASE_TABLE'] == table_value)

        # Extract distinct column values as a comma-separated string
        columns = filtered_rulesDf.select("COLUMN").distinct().rdd.flatMap(lambda x: x).collect()
        columns_str = ','.join(columns)  # Create comma-separated column string

        #Create the Data Asset
        # Define data source
        datasource = context.sources.add_or_update_spark("BTC_GATE_DATASOURCE")
        name = f"{table_value}"
        data_asset = datasource.add_dataframe_asset(name=name)
        sampleDF = spark.sql(f"SELECT {columns_str},customer_id FROM {table_value}")
        #sampleDF.show()

        # Filter custom rules to modify data asset
        filtered_rulesDf = rulesDf.filter(rulesDf['RULE_TYPE'] == 'custom')

        for row in filtered_rulesDf.collect():
            module = row['MODULE']
            database = row['DATABASE']
            table = row['TABLE']
            column = row['COLUMN']
            rule_id = row['RULE_ID']
            parameters = row['PARAMETERS']
            database_table = row['DATABASE_TABLE']

            if module == 'expect_calculated_age_to_be_greater_than':
                parameters = parameters.strip().strip("'")
                if parameters.startswith("\\"):
                    parameters = parameters[1:]
                parsed_json = json.loads(parameters)
                min_age = parsed_json.get("min_age", 16)
                date_regex = parsed_json.get("date_regex", r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T.*$")

                # Define the static value as today's date
                today_date = datetime.datetime.now().strftime("%Y-%m-%d")

                # Modify the sampleDF to set dob_date based on the regex match
                sampleDF = sampleDF.withColumn(
                    "dob_date",
                    F.when(F.col(column).rlike(date_regex), F.to_date(F.col(column).substr(1, 10), "yyyy-MM-dd"))
                    .otherwise(lit(today_date))  # Set static value if regex does not match
                )
                sampleDF = sampleDF.withColumn(
                    "age", F.floor(F.datediff(F.current_date(), F.col("dob_date")) / 365.25)
                )


            elif module == 'expect_custom_rule_id_type':
                # Add new columns based on the value of id_type
                sampleDF = sampleDF.withColumn(
                    "national_id",
                    when(F.col("id_type") == "national_id", F.col("customer_id"))
                    .otherwise(lit(None))  # Set to None if not applicable
                ).withColumn(
                    "passport_id",
                    when(F.col("id_type") == "passport_id", F.col("customer_id"))
                    .otherwise(lit(None))  # Set to None if not applicable
                ).withColumn(
                    "anonymous_id",
                    when(F.col("id_type") == "anonymous_id", lit("123456789"))
                    .otherwise(lit(None))  # Set to None if not applicable
                )

        # Create batch request for Great Expectations
        batch_request = data_asset.build_batch_request(dataframe=sampleDF)
        # expectation_suite_name = f"analyse_{database}.{table}"
        expectation_suite_name = "GATE_DQ_RULES"
        context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
        validator = context.get_validator(batch_request=batch_request, expectation_suite_name=expectation_suite_name)

        # Filter rulesDf back for rows related to the current table
        filtered_rulesDf = rulesDf.filter(rulesDf['DATABASE_TABLE'] == table_value)

        # Traverse through each row of the filtered DataFrame
        for row in filtered_rulesDf.collect():
            module = row['MODULE']
            database = row['DATABASE']
            table = row['TABLE']
            column = row['COLUMN']
            rule_id = row['RULE_ID']
            parameters = row['PARAMETERS']
            database_table = row['DATABASE_TABLE']

            #if module == 'expect_custom_rule_id_type':
            #    sampleDF = spark.sql(f"SELECT {column},customer_id FROM {database}.{table}")
            #else:
            #    sampleDF = spark.sql(f"SELECT {column} FROM {database}.{table}")

            # Dynamically call the appropriate function based on the module value
            if module == 'expect_column_values_to_be_between':
                expect_column_values_to_be_between(validator, column, parameters)
            elif module == 'expect_column_values_to_be_greater_than':
                expect_column_values_to_be_greater_than(validator, column, parameters)
            elif module == 'expect_column_value_lengths_to_be_between':
                expect_column_value_lengths_to_be_between(validator, column, parameters)
            elif module == 'expect_column_values_to_be_in_set':
                expect_column_values_to_be_in_set(validator, column, parameters)
            elif module == 'expect_column_values_to_not_be_in_set':
                expect_column_values_to_not_be_in_set(validator, column, parameters)
            elif module == 'expect_column_values_to_match_regex':
                expect_column_values_to_match_regex(validator, column, parameters)
            elif module == 'expect_calculated_age_to_be_greater_than':
                expect_calculated_age_to_be_greater_than(validator, column, parameters, sampleDF)
            elif module == 'expect_custom_rule_id_type':
                # 1. Filter out records where id_type is 'anonymous_id' (no validation needed for these rows)
                #filtered_df = sampleDF.filter(F.col(column) != 'anonymous_id')
                print(f"adding row_condition id_type==national_id")

                # 2. Check if id_type is 'national_id', customer_id should be numeric and non-null
                validator.expect_column_values_to_not_be_null('national_id',condition_parser="spark",row_condition='id_type=="national_id"')
                validator.expect_column_values_to_match_regex('national_id', r'^\d+$',condition_parser="spark",row_condition='id_type=="national_id"')

                # 3. Check if id_type is 'passport_id', customer_id should be alphanumeric and non-null
                validator.expect_column_values_to_not_be_null('passport_id',condition_parser="spark",row_condition='id_type=="passport_id"')
                validator.expect_column_values_to_match_regex('passport_id', r'^[a-zA-Z0-9]+$',condition_parser="spark",row_condition='id_type=="passport_id"')

                # 4. Ensure id_type column is not blank
                validator.expect_column_values_to_not_be_null(column)
                validator.expect_column_values_to_not_match_regex(column, r'^\s*$')

                #sampleDF = expect_custom_rule_id_type(validator, column, 'customer_id', sampleDF)
            elif module == 'expect_column_values_to_be_valid_country':
                expect_column_values_to_be_valid_country(validator, column)
            elif module == 'expect_phonenumber_with_country':
                # 1. Validate if the phone number follows the correct format (E.164 format for international numbers)
                phone_number_regex = r'^(\+)?\d+$'
                validator.expect_column_values_to_match_regex(column, regex=phone_number_regex,condition_parser="spark",row_condition=f'{column} is not null AND {column} != ""')
                # 2. Ensure uniqueness across customers
                validator.expect_column_values_to_be_unique(column,condition_parser="spark",row_condition=f'{column} is not null AND {column} != ""')
                # 3. Ensure it follows international phone format (E.164 or similar)
                validator.expect_column_values_to_match_regex(column, regex=r'^\+[1-9]\d{1,14}$',condition_parser="spark",row_condition=f'{column} is not null AND {column} != ""')
            elif module == 'expect_string_lengthmax50_with_null_allowed':
                validator.expect_column_values_to_match_regex(column, regex=r'^[A-Za-z ]+$',condition_parser="spark",row_condition=f'{column} is not null AND {column} != ""')
                validator.expect_column_value_lengths_to_be_between(column, min_value=2, max_value=50,condition_parser="spark",row_condition=f'{column} is not null AND {column} != ""')
            elif module == 'expect_email_with_null_allowed':
                validator.expect_column_values_to_match_regex(column, regex=r'^[\w\.-]+@[\w\.-]+\.\w+$',condition_parser="spark",row_condition=f'{column} is not null AND {column} != ""')
                validator.expect_column_values_to_be_unique(column,condition_parser="spark",row_condition=f'{column} is not null AND {column} != ""')
            else:
                # Apply other expectations based on the module value
                # The expectations which doesn't require extra parameter, will get executed in the else block dynamically
                # The getattr() function returns the value of the module attribute from the validator object.
                # where module is expectation e.g: expect_column_values_to_not_be_null
                expectation_method = getattr(validator, module)
                expectation_method(column)

        validator.save_expectation_suite(discard_failed_expectations=False)

       # Define checkpoint
        my_checkpoint_name = "btc_spark_checkpoint"
        checkpoint = Checkpoint(
            name=my_checkpoint_name,
            run_name_template=f"{timestamp}-{table}",
            data_context=context,
            batch_request=batch_request,
            expectation_suite_name=expectation_suite_name,
            action_list=[
                {"name": "store_validation_result", "action": {"class_name": "StoreValidationResultAction"}},
                {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}}
            ],
        )
        print(f"checkpoint : {checkpoint}")
        context.add_or_update_checkpoint(checkpoint=checkpoint)
        checkpoint_result = checkpoint.run()
        print(f"checkpoint_result : {checkpoint_result}")
        PATH = f"BTC_DQ_POC/{date}"
        if not os.path.exists(PATH):
            os.makedirs(PATH)
        checkpoint_result_serializable = checkpoint_result.to_json_dict()

        with open(f"{PATH}/{table}_validation_check_{timestamp}.json", 'w') as f:
            json.dump(checkpoint_result_serializable, f, indent=4)

        with open(f"{PATH}/{table}_validation_check_{timestamp}.json", 'r') as file:
            data = json.load(file)

        run_results = data["run_results"]
        dynamic_key = list(run_results.keys())[0]
        validation_results = run_results[dynamic_key]["validation_result"]["results"]
        run_time = data["run_id"]["run_time"]

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
                            tablename=table_value))

        df = spark.createDataFrame(rows)
        df = df.withColumn("run_time", to_timestamp(df["run_time"]))
        df.show()

        df.write.mode('append').format("hive").saveAsTable("datawash.greatexpectation_result")
    except Exception as e:
        print(f"An error occurred processing table {table_value}: {e}", file=sys.stderr)

# Stop SparkSession
spark.stop()
