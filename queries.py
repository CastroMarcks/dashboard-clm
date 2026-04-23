# Defina aqui as queries que serão executadas no Athena.
# Cada função retorna uma string SQL.

## Nossas tabelas na Sandbox:
# sandbox_datalake_sales_ops.oli_pedidos_incrementais
# sandbox_datalake_sales_ops.oli_estoques
# sandbox_datalake_sales_ops.oli_resultados_campanhas_top3
# sandbox_datalake_sales_ops.oli_resultados_campanhas_sellers_id
# sandbox_datalake_sales_ops.oli_produtos_marketmap
# sandbox_datalake_sales_ops.oli_pedidos_marketmap


def query_bsa_sfmc_clm_journeyhistory():
    return """
    SELECT * FROM "datalake_gold"."bsa_sfmc_clm_journeyhistory"
    WHERE data_envio >= TIMESTAMP '2026-01-01 00:00:00.000'
    """
