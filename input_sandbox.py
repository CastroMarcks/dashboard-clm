import awswrangler as wr


def sandbox_inputador(df, nome_da_tabela):
    caminho_no_s3 = f"s3://dataplat-sandbox-datalake-sales-ops/tables/{nome_da_tabela}/"
    nome_do_banco_de_dados = "sandbox_datalake_sales_ops"

    wr.s3.to_parquet(
        df=df,
        path=caminho_no_s3,
        dataset=True,
        database=nome_do_banco_de_dados,
        table=nome_da_tabela,
        mode="overwrite"
        # mode="append"
    )
