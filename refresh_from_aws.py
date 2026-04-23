"""Roda a query do Athena, agrega in-memory e gera data.json. Sem CSV intermediario."""
import time
import traceback
from athena_reader import athena
from queries import query_bsa_sfmc_clm_journeyhistory
from process_data import process

MAX_TENTATIVAS = 3


def _executar_query():
    return athena.read('datalake', query=query_bsa_sfmc_clm_journeyhistory())


def main():
    t0 = time.time()
    df = None
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            print(f'[aws] tentativa {tentativa}/{MAX_TENTATIVAS} — executando query no Athena...')
            df = _executar_query()
            print(f'[aws] query ok em {time.time()-t0:.1f}s — {len(df):,} linhas, {len(df.columns)} colunas')
            break
        except Exception as e:
            print(f'[aws] tentativa {tentativa} falhou: {e.__class__.__name__}: {e}')
            if tentativa == MAX_TENTATIVAS:
                traceback.print_exc()
                raise
            time.sleep(5 * tentativa)

    print('[process] agregando e gerando data.json...')
    process(df)
    print(f'[done] tempo total: {time.time()-t0:.1f}s')


if __name__ == '__main__':
    main()
