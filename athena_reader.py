import pandas as pd
import logging as log
import awswrangler as wr
import boto3
from botocore.config import Config


class Athena:

    def __init__(self, session: boto3.Session):
        self.session = session

    def read(self, database: str, file_path: str = None, query: str = None) -> pd.DataFrame:
        log.info(f'-----------< read query >-----------')
        log.info(f'Database: {database}')
        _query = open(file_path).read() if file_path else query
        df = wr.athena.read_sql_query(
            _query,
            database=database,
            workgroup='sales-ops',
            boto3_session=self.session,
        )
        log.info(f'DataFrame: {df.shape}')
        log.info(f'-------------< done >--------------')
        return df


_botocore_config = Config(
    region_name='us-east-1',
    max_pool_connections=50,
    retries={'max_attempts': 10, 'mode': 'standard'},
    read_timeout=120,
    connect_timeout=30,
)
session = boto3.Session(region_name='us-east-1')
session._session.set_default_client_config(_botocore_config)
athena = Athena(session)
