import argparse
import logging

from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.transforms.sql import SqlTransform
import apache_beam as beam
from apache_beam import transforms
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import _DelimSplitter, read_csv

class collectcolumns(beam.DoFn):
    def process(self, element):
        result = [
            (element['AGENCY'], element['TRANSACTION_AMOUNT'])
        ]
        return result

class filtercolumns(beam.DoFn):
  """Parse each line of input text into words."""

  def process(self, element):
    
    TRANSACTION_ID,AGENCY,TRANSACTION_DATE,TRANSACTION_AMOUNT,VENDOR_NAME,VENDOR_STATE_PROVINCE,MCC_DESCRIPTION = element.split(",")
    li=[{
            'AGENCY': AGENCY.strip(),
            'TRANSACTION_DATE': TRANSACTION_DATE.strip()[0:10],
            'TRANSACTION_AMOUNT': float(TRANSACTION_AMOUNT)
        }]
    return li


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the our pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='C:/path/suryavamsi/generated_batch_data.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default='gs://<your project>/out',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


  def write_format(*kwargs):
    out=''
    for i in kwargs:
      out +=i[0] + ' : ' + str(i[1]) + '\n'
    return out

  def sum_all(element):
    (agency, row) = element
    return (agency, sum(row))


  with beam.Pipeline(options=pipeline_options) as p:

    data_header = ['TRANSACTION_ID','AGENCY','TRANSACTION_DATE','TRANSACTION_AMOUNT','VENDOR_NAME','VENDOR_STATE_PROVINCE','MCC_DESCRIPTION']

    # Read the text file[pattern] into a PCollection.
    lines = (p | 'Read' >> ReadFromText(known_args.input, skip_header_lines=1)
               | 'Parse' >> (beam.ParDo(filtercolumns())))
    transforming = (lines | (beam.ParDo(collectcolumns()))
                          | 'Group' >> (beam.GroupByKey())
                          | 'sum' >> (beam.Map(sum_all))
                          | 'Largest 5 values' >> beam.combiners.Top.Largest(5))
    (transforming 
    | 'Write in Format' >> beam.MapTuple(write_format)
                 |WriteToText(known_args.output))
  
    table_id = 'test_table'
    dataset_id = 'suryavamsi'
    project_id = '<your project name>'
    table_schema = ('AGENCY:STRING,TRANSACTION_DATE:STRING,TRANSACTION_AMOUNT:NUMERIC')

    # BQ_source = beam.io.BigQuerySource(query = "SELECT 'SURYAVAMSI' as surya", use_standard_sql=True)
    # BQ_data = (p | 'execute select query' >> beam.io.Read(BQ_source)
    #              | 'overwrite the data in file' >> WriteToText(known_args.output))

    lines | beam.io.WriteToBigQuery(
    table = table_id,
    dataset=dataset_id,
    project=project_id,
    schema=table_schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
               
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
