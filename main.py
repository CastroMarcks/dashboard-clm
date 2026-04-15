from athena_reader import athena
from input_sandbox import sandbox_inputador
from queries import query_bsa_sfmc_clm_journeyhistory


if __name__ == "__main__":

    # 1. Leitura do Athena
    print("Executando query bsa_sfmc_clm_journeyhistory...")
    df = athena.read('datalake', query=query_bsa_sfmc_clm_journeyhistory())
    print(f"Linhas: {len(df)} | Colunas: {list(df.columns)}")

    # 2. Salvar em CSV
    df.to_csv("bsa_sfmc_clm_journeyhistory.csv", index=False)
    print("Salvo em bsa_sfmc_clm_journeyhistory.csv")
