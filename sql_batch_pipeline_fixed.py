import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyodbc
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import gcsfs
import json
import logging
import os
import uuid

from google.cloud import secretmanager


def get_sql_config(secret_id, project_id):
    logging.info(f"Accessing secret '{secret_id}' from default project context")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret_payload = response.payload.data.decode("UTF-8")
    logging.info("Secret retrieved successfully.")
    return json.loads(secret_payload)


def build_connection_string(config):
    logging.info("Building SQL Server connection string...")
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},1433;"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )


class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--gcp_project', required=True)
        parser.add_argument('--table_name', required=True)
        parser.add_argument('--batch_size', type=int, default=50000)
        parser.add_argument('--output_path', required=True)
        parser.add_argument('--secret_id', required=True)
        parser.add_argument('--order_column', required=True)
        parser.add_argument('--chunk_size', type=int, default=50000)


def generate_batch_queries(table_name, batch_size, order_column, connection_string):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    queries = []
    for start in range(1, total_rows + 1, batch_size):
        end = start + batch_size - 1
        query = f"""
SELECT * FROM (
                SELECT cast([AddressId] as nvarchar(max)) as [AddressId]
				  ,cast([CityId]           	as nvarchar(max)) as [CityId]
				  ,cast([StateId]			as nvarchar(max)) as [StateId]
				  ,cast([ZipId]				as nvarchar(max)) as [ZipId]
				  ,cast([CountryId]			as nvarchar(max)) as [CountryId]
				  ,cast([Address1]			as nvarchar(max)) as [Address1]
				  ,cast([Address2]			as nvarchar(max)) as [Address2]
				  ,cast([CustomerCode]		as nvarchar(max)) as [CustomerCode]
				  ,cast([Latitude]			as nvarchar(max)) as [Latitude]
				  ,cast([Longitude]			as nvarchar(max)) as [Longitude]
				  ,cast([GeoCode]			as nvarchar(max)) as [GeoCode]
				  ,cast([Geocoded]			as nvarchar(max)) as [Geocoded]
				  ,cast([CreatedBy]			as nvarchar(max)) as [CreatedBy]
				  ,cast([CreatedDate]		as nvarchar(max)) as [CreatedDate]
				  ,cast([UpdatedBy]			as nvarchar(max)) as [UpdatedBy]
				  ,cast([UpdatedDate]		as nvarchar(max)) as [UpdatedDate]
				  ,cast([S2CellId]			as nvarchar(max)) as [S2CellId], 
				  ROW_NUMBER() OVER (ORDER BY {order_column}) AS rn
							FROM {table_name}
            ) AS numbered
            WHERE rn BETWEEN {start} AND {end}
        """
        queries.append(query.strip())
    return queries


class StreamedChunkedReader(beam.DoFn):
    def __init__(self, connection_string, chunk_size):
        self.connection_string = connection_string
        self.chunk_size = chunk_size
        self.conn = None

    def start_bundle(self):
        self.conn = pyodbc.connect(self.connection_string)

    def process(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        chunk = []
        for row in cursor:
            chunk.append(dict(zip(columns, row)))
            if len(chunk) >= self.chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
        cursor.close()

    def finish_bundle(self):
        if self.conn:
            self.conn.close()


class WriteParquetPerBatch(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path
        self.fs = gcsfs.GCSFileSystem()

    def process(self, elements):
        if not elements:
            return

        batch_id = uuid.uuid4().hex[:8]
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(self.output_path, f"{timestamp}_part-{batch_id}.parquet")

        try:
            table = pa.Table.from_pylist(elements)
            with self.fs.open(output_file, 'wb') as f:
                pq.write_table(table, f, compression='snappy')
            logging.info(f"Wrote {len(elements)} rows to {output_file}")
            yield output_file
        except Exception as e:
            logging.error(f"Failed to write Parquet: {e}")
            raise


def run_pipeline():
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project='rxo-dataeng-datalake-np',
        region='us-central1',
        temp_location='gs://rxo-dataeng-datalake-np-dataflow/temp',
        staging_location='gs://rxo-dataeng-datalake-np-dataflow/staging',
        save_main_session=True,
        service_account_email='ds-dataflow-dataeng-gsa@rxo-dataeng-datalake-np.iam.gserviceaccount.com',
        subnetwork='https://www.googleapis.com/compute/v1/projects/nxo-corp-infra/regions/us-central1/subnetworks/rxo-dataeng-datalake-np-uscentral1',
        use_public_ips=False,
        worker_harness_container_image='us-central1-docker.pkg.dev/rxo-dataeng-datalake-np/dataflow-flex-template/sql-parquet-to-gcs-batched:latest'
    )
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    config = get_sql_config(custom_options.secret_id, custom_options.gcp_project)
    connection_string = build_connection_string(config)

    queries = generate_batch_queries(
        table_name=custom_options.table_name,
        batch_size=custom_options.batch_size,
        order_column=custom_options.order_column,
        connection_string=connection_string
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Create Queries" >> beam.Create(queries)
            | "Read & Chunk SQL" >> beam.ParDo(StreamedChunkedReader(connection_string, custom_options.chunk_size))
            | "Write Parquet" >> beam.ParDo(WriteParquetPerBatch(custom_options.output_path))
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run_pipeline()
