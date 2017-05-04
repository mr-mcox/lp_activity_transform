import apache_beam as beam
from apache_beam.utils.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
import sys


project = 'business-intelligence-dev'
output_table = 'matthew_sandbox.lp_actions_transform'
input_query = """SELECT account.id as account_id
                    ,views 
                    FROM [lead-pages:leadpages.DailyAggregatedViews]
                    WHERE account.id IN
                    (SELECT __key__.id as accnt_id
                    FROM [lead-pages:leadpages.Account_cleansed] LIMIT 100)"""

options = PipelineOptions(flags=sys.argv)

# For Cloud execution, set the Cloud Platform project, job_name,
# staging location, temp_location and specify DataflowRunner.
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project
google_cloud_options.job_name = 'lp-analysis'
google_cloud_options.staging_location = 'gs://lp_activity_transform/staging'
google_cloud_options.temp_location = 'gs://lp_activity_transform/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

p = beam.Pipeline(options=options)
(p
    | 'read' >> beam.Read(beam.io.BigQuerySource(query=input_query))
    | 'cast ints' >> beam.Map(lambda row: (row['account_id'], int(row['views'])))
    | beam.CombinePerKey(sum)
    | 'format for gbq' >> beam.Map(lambda (k, v): {'account_id':k, 'total_views':v})
    | 'save' >> beam.Write(beam.io.BigQuerySink(
        output_table,
        schema='account_id:INTEGER, total_views:INTEGER',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
p.run()
