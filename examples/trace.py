# -*- coding: utf-8 -*-
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
import logging
import os
import time

# Import Spanner library
# Make sure you have installed it: pip install google-cloud-spanner opentelemetry-exporter-gcp-trace opentelemetry-sdk opentelemetry-api
try:
    import google.cloud.spanner as spanner
    from google.cloud.spanner_v1 import Transaction # Explicit import can be helpful
except ImportError:
    print("Please install required libraries: pip install google-cloud-spanner opentelemetry-exporter-gcp-trace opentelemetry-sdk opentelemetry-api")
    exit()

from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
# from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry import trace
from opentelemetry.propagate import set_global_textmap
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Setup common variables that'll be used between Spanner and traces.
# project_id = os.environ.get('SPANNER_PROJECT_ID', 'test-project')
# project_id = 'span-cloud-testing'


# Configure logging
logging.basicConfig(level=logging.INFO)

# --- Configuration ---
# IMPORTANT: Replace with your actual Project ID, Instance ID, and Database ID
# project_id = os.environ.get('SPANNER_PROJECT_ID', 'your-gcp-project-id') # Use environment variable or hardcode
project_id = "span-cloud-testing" # Using the value from the original code
instance_id = "manu-demo1-mr-nam6" # Using the value from the original code
database_id = "demo" # Using the value from the original code

# IMPORTANT: Ensure this matches your Spanner table schema EXACTLY (case-sensitive)
TABLE_NAME = "TEST"
ID_COLUMN = "Id"
DATA_COLUMN = "Data"
TEST_ID_VALUE = 1899273 # The ID used in the transaction
def spanner_with_cloud_trace():
    # [START spanner_opentelemetry_traces_cloudtrace_usage]
    # Setup OpenTelemetry, trace and Cloud Trace exporter.
    tracer_provider = TracerProvider(sampler=ALWAYS_ON)
    trace_exporter = CloudTraceSpanExporter(project_id=project_id)
    tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
    set_global_textmap(TraceContextTextMapPropagator())

# Setup the Cloud Spanner Client.
    spanner_client = spanner.Client(
        project_id,
        observability_options=dict(tracer_provider=tracer_provider, enable_extended_tracing=True, enable_end_to_end_tracing=True),
    )
    
    # [END spanner_opentelemetry_traces_cloudtrace_usage]
    return spanner_client

def read_and_update_data(database):
    """
    Runs a transaction that inserts, reads, and updates a row.

    Args:
        database: The Spanner database object.
    """
    logging.info(f"Starting transaction for ID: {TEST_ID_VALUE}")

    def _transaction_body(transaction: Transaction):
        """The actual operations performed within the transaction."""
        # 1. Insert a new row
        # Ensure table/column names match schema exactly (case-sensitive)
        insert_sql = f"INSERT INTO {TABLE_NAME} ({ID_COLUMN}, {DATA_COLUMN}) VALUES (@id, @val)"
        params = {"id": TEST_ID_VALUE, "val": "initial_value"}
        param_types = {"id": spanner.param_types.INT64, "val": spanner.param_types.STRING}

        # Use execute_update for DML statements (INSERT, UPDATE, DELETE)
        # It returns the number of affected rows.
        try:
            row_count_insert = transaction.execute_update(
                insert_sql, params=params, param_types=param_types
            )
            logging.info(f"INSERT statement affected {row_count_insert} row(s).")
        except Exception as e:
            logging.error(f"Error during INSERT: {e}")
            # If insert fails (e.g., row already exists), the transaction will likely roll back.
            # Consider adding logic here if needed (e.g., try UPDATE instead).
            raise # Re-raise to ensure transaction rollback

        # 2. Read the row just inserted (within the same transaction)
        # Ensure table/column names match schema exactly (case-sensitive)
        select_sql = f"SELECT {ID_COLUMN}, {DATA_COLUMN} FROM {TABLE_NAME} WHERE {ID_COLUMN} = @id"
        # Use execute_sql for SELECT statements. It returns a ResultSet.
        results = transaction.execute_sql(
            select_sql, params=params, param_types={"id": spanner.param_types.INT64}
        )

        # Iterate through the results (should be exactly one row in this case)
        read_rows = list(results) # Consume the iterator
        if not read_rows:
            logging.warning(f"SELECT statement did not find row with ID {TEST_ID_VALUE} immediately after insert (unexpected).")
            # This shouldn't happen within a transaction if INSERT succeeded
            raise Exception(f"Failed to read row {TEST_ID_VALUE} after inserting it.")
        else:
            logging.info(f"SELECT statement returned {len(read_rows)} row(s). First row data: {read_rows[0]}")
            # You can access columns by index (e.g., read_rows[0][0]) or name if selected explicitly


        # 3. Update the row
        # Ensure table/column names match schema exactly (case-sensitive)
        update_sql = f"UPDATE {TABLE_NAME} SET {DATA_COLUMN} = @new_val WHERE {ID_COLUMN} = @id"
        update_params = {"id": TEST_ID_VALUE, "new_val": "updated_value"}
        update_param_types = {"id": spanner.param_types.INT64, "new_val": spanner.param_types.STRING}

        row_count_update = transaction.execute_update(
            update_sql, params=update_params, param_types=update_param_types
        )
        logging.info(f"UPDATE statement affected {row_count_update} row(s).")
        if row_count_update == 0:
            logging.warning(f"UPDATE statement did not affect any rows for ID {TEST_ID_VALUE} (unexpected).")
            # This also shouldn't happen if the previous steps succeeded
            raise Exception(f"Failed to update row {TEST_ID_VALUE}.")

    # database.run_in_transaction handles retries for transient aborts.
    # If _transaction_body raises an exception, the transaction is rolled back.
    database.run_in_transaction(_transaction_body)
    logging.info(f"Transaction for ID {TEST_ID_VALUE} committed successfully.")


def main():
    # Setup OpenTelemetry, trace and Cloud Trace exporter.
    tracer_provider = TracerProvider(sampler=ALWAYS_ON)
    trace_exporter = CloudTraceSpanExporter(project_id=project_id)
    tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))

    set_global_textmap(TraceContextTextMapPropagator())

    # Setup the Cloud Spanner Client.
    spanner_client = spanner.Client(
        project_id,
        observability_options=dict(tracer_provider=tracer_provider, enable_extended_tracing=True, enable_end_to_end_tracing=True),
    )
    # Setup the Cloud Spanner Client.
    # Change to "spanner_client = spanner_with_otlp" to use OTLP exporter
    # spanner_client = spanner_with_cloud_trace()
    instance = spanner_client.instance('gargsurbhi-testing')
    database = instance.database('test-db')
    
    # Set W3C Trace Context as the global propagator for end to end tracing.
    # set_global_textmap(TraceContextTextMapPropagator())

    # Retrieve a tracer from our custom tracer provider.
    tracer = tracer_provider.get_tracer('MyApp')

    # Now run our queries
    with tracer.start_as_current_span('QuerySimpleSelect'):
        with database.snapshot() as snapshot:
            info_schema = snapshot.execute_sql(
                'SELECT 1')
            for row in info_schema:
                print(row)

        with tracer.start_as_current_span('ReadWriteTransaction'):
            try:
                read_and_update_data(database)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    main()
