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
    SELECT 
    id, contato, data_envio, nome_jornada, nome_atividade, 
    canal, status, id_empresa, cnpj, flag_pontual, 
    objetivo, bu, campanha, metrica
    FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY nome_atividade 
            ORDER BY data_envio DESC
        ) AS linha_num
    FROM "datalake_gold"."bsa_sfmc_clm_journeyhistory"
    )
    WHERE linha_num = 1
    """
