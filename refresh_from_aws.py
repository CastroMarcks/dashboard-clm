"""Refresh incremental: busca apenas dias novos do Athena e concatena com dados.csv existente."""
import json
import time
import traceback
import pandas as pd
import awswrangler as wr
import boto3
from botocore.config import Config
from queries import query_bsa_sfmc_clm_journeyhistory
from process_data import process

CSV_PATH = 'dados.csv'
DATA_JSON = 'data.json'
MAX_TENTATIVAS = 3


def _dia_max_atual():
    try:
        with open(DATA_JSON, encoding='utf-8') as f:
            d = json.load(f)
        return d.get('filtros', {}).get('dia_max')
    except Exception:
        return None


def _athena_query(sql):
    config = Config(
        region_name='us-east-1',
        max_pool_connections=5,   # reduz paralelismo para evitar drop de conexao
        retries={'max_attempts': 5, 'mode': 'standard'},
        read_timeout=300,
        connect_timeout=30,
    )
    session = boto3.Session(region_name='us-east-1')
    session._session.set_default_client_config(config)
    return wr.athena.read_sql_query(
        sql,
        database='datalake',
        workgroup='sales-ops',
        boto3_session=session,
        ctas_approach=False,    # download unico em CSV — mais lento mas muito mais estavel
    )


def main():
    t0 = time.time()

    dia_max = _dia_max_atual()
    if dia_max:
        print(f'[incremental] dia_max atual: {dia_max} — buscando apenas dados mais recentes')
        sql = query_bsa_sfmc_clm_journeyhistory(desde=dia_max)
    else:
        print('[full] sem data.json existente — baixando tudo desde 2026-01-01')
        sql = query_bsa_sfmc_clm_journeyhistory()

    df_novo = None
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            print(f'[aws] tentativa {tentativa}/{MAX_TENTATIVAS}...')
            df_novo = _athena_query(sql)
            print(f'[aws] ok em {time.time()-t0:.1f}s — {len(df_novo):,} linhas novas')
            break
        except Exception as e:
            print(f'[aws] tentativa {tentativa} falhou: {e.__class__.__name__}: {e}')
            if tentativa == MAX_TENTATIVAS:
                traceback.print_exc()
                raise
            time.sleep(10 * tentativa)

    if dia_max and len(df_novo) == 0:
        print('[incremental] nenhuma linha nova — dados ja atualizados.')
        return

    if dia_max:
        print(f'[incremental] lendo {CSV_PATH} existente...')
        df_hist = pd.read_csv(CSV_PATH, dtype=str)
        print(f'[incremental] historico: {len(df_hist):,} linhas')
        df_novo = df_novo.astype(str)
        df_full = pd.concat([df_hist, df_novo], ignore_index=True)
        print(f'[incremental] total apos concat: {len(df_full):,} linhas')
    else:
        df_full = df_novo

    print(f'[csv] salvando {CSV_PATH}...')
    df_full.to_csv(CSV_PATH, index=False)

    print('[process] agregando e gerando data.json...')
    process(df_full)
    print(f'[done] tempo total: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
