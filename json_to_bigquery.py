# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""`data_ingestion.py` is a Dataflow pipeline which reads a file and writes its
contents to a BigQuery table.
This example does not do any transformation on the data.
"""

from __future__ import absolute_import
from apache_beam.io.gcp.internal.clients import bigquery
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to read and the output table to write.
    # This is the final stage of the pipeline, where we define the destination
    # of the data. In this case we are writing to BigQuery.
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        # This example file contains a total of only 10 lines.
        # Useful for developing on a small set of data.
        default='gs://python-dataflow-example/data_files/head_usa_names.csv')

    # This defaults to the lake dataset in your BigQuery project. You'll have
    # to create the lake dataset yourself using this command:
    # bq mk lake
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='lake.usa_names')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as p:
    
      table_schema = bigquery.TableSchema()

      order_schema = bigquery.TableFieldSchema()
      order_schema.name = 'date'
      order_schema.type = 'datetime'
      order_schema.name = 'order_id'
      order_schema.type = 'string'
      table_schema.fields.append(order_schema)
        customer = bigquery.TableFieldSchema()
        customer.name = 'customer'
        customer.type = 'record'
        customer.mode = 'nullable'
          customer_state = bigquery.TableFieldSchema()
          customer_state.name = 'customer_state'
          customer_state.type = 'string'
          customer_state.mode = 'nullable'
          customer.fields.append(customer_state)
          customer_city = bigquery.TableFieldSchema()
          customer_city.name = 'customer_city'
          customer_city.type = 'string'
          customer_city.mode = 'nullable'
          customer.fields.append(customer_city)
          customer_unique_id = bigquery.TableFieldSchema()
          customer_unique_id.name = 'customer_unique_id'
          customer_unique_id.type = 'string'
          customer_unique_id.mode = 'nullable'
          customer.fields.append(customer_unique_id)
          customer_zip_code_prefix = bigquery.TableFieldSchema()
          customer_zip_code_prefix.name = 'customer_zip_code_prefix'
          customer_zip_code_prefix.type = 'integer'
          customer_zip_code_prefix.mode = 'nullable'
          customer.fields.append(customer_zip_code_prefix)
          customer_id = bigquery.TableFieldSchema()
          customer_id.name = 'customer_id'
          customer_id.type = 'integer'
          customer_id.mode = 'nullable'
          customer.fields.append(customer_id)
          table_schema.fields.append(customer)
        orderItem = bigquery.TableFieldSchema()
        orderItem.name = 'orderItem'
        orderItem.type = 'record'
        orderItem.mode = 'nullable'
          seller_city = bigquery.TableFieldSchema()
          seller_city.name = 'seller_city'
          seller_city.type = 'string'
          seller_city.mode = 'nullable'
          order_item.fields.append(seller_city)
          freight_value = bigquery.TableFieldSchema()
          freight_value.name = 'freight_value'
          freight_value.type = 'float'
          freight_value.mode = 'nullable'
          order_item.fields.append(freight_value)
          seller_zip_code_prefix = bigquery.TableFieldSchema()
          seller_zip_code_prefix.name = 'seller_zip_code_prefix'
          seller_zip_code_prefix.type = 'integer'
          seller_zip_code_prefix.mode = 'nullable'
          order_item.fields.append(seller_zip_code_prefix)
          shipping_limit_date = bigquery.TableFieldSchema()
          shipping_limit_date.name = 'shipping_limit_date'
          shipping_limit_date.type = 'datetime'
          shipping_limit_date.mode = 'nullable'
          order_item.fields.append(shipping_limit_date)
          seller_id = bigquery.TableFieldSchema()
          seller_id.name = 'seller_id'
          seller_id.type = 'string'
          seller_id.mode = 'nullable'
          order_item.fields.append(seller_id)
          product_id = bigquery.TableFieldSchema()
          product_id.name = 'product_id'
          product_id.type = 'string'
          product_id.mode = 'nullable'
          order_item.fields.append(product_id)
          order_item_id = bigquery.TableFieldSchema()
          order_item_id.name = 'order_item_id'
          order_item_id.type = 'integer'
          order_item_id.mode = 'nullable'
          order_item.fields.append(order_item_id)
          seller_state = bigquery.TableFieldSchema()
          seller_state.name = 'seller_state'
          seller_state.type = 'string'
          seller_state.mode = 'nullable'
          order_item.fields.append(seller_state)
          price = bigquery.TableFieldSchema()
          price.name = 'price'
          price.type = 'float'
          price.mode = 'nullable'
          order_item.fields.append(price)
          order_id = bigquery.TableFieldSchema()
          order_id.name = 'order_id'
          order_id.type = 'string'
          order_id.mode = 'nullable'
          order_item.fields.append(order_id)
          table_schema.fields.append(orderItem)

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     # Read the file. This is the source of the pipeline. All further
     # processing starts with lines read from the file. We use the input
     # argument from the command line. We also skip the first line which is a
     # header row.
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written. This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String To BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType
             schema='state:STRING,gender:STRING,year:STRING,name:STRING,'
             'number:STRING,created_date:STRING',
             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()




