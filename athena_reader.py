import pandas as pd
import logging as log
import awswrangler as wr
import boto3


class Athena:

    def __init__(self, session: boto3.Session):
        self.session = session

    def read(self, database: str, file_path: str = None, query: str = None) -> pd.DataFrame:
        log.info(f'-----------< read query >-----------')
        log.info(f'Database: {database}')
        try:
            _query = open(file_path).read() if file_path else query
            df = wr.athena.read_sql_query(
                _query,
                database=database,
                workgroup='sales-ops',
                boto3_session=self.session
            )
            log.info(f'DataFrame: {df.shape}')
            log.info(f'-------------< done >--------------')
            return df
        except Exception as e:
            log.error(f"Something went wrong executing query Exception: {e}")


session = boto3.Session(region_name='us-east-1')
athena = Athena(session)
