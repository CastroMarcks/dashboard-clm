import pandas as pd
import json


def process(df: pd.DataFrame, out_path: str = 'data.json') -> dict:
    df = df.copy()
    df['data_envio'] = pd.to_datetime(df['data_envio'])
    df['dia'] = df['data_envio'].dt.strftime('%Y-%m-%d')
    df['mes'] = df['data_envio'].dt.strftime('%Y-%m')
    df['dia_semana'] = df['data_envio'].dt.dayofweek
    df['hora'] = df['data_envio'].dt.hour

    bu_map = {'ERP': 'ERP', 'Conta': 'Conta Digital', 'Envios': 'Envios', 'Ecommerce': 'Ecommerce', 'Outros': 'Outros'}
    df['bu'] = df['bu'].map(bu_map).fillna('Outros')
    df = df[df['bu'].isin(['ERP', 'Conta Digital', 'Envios', 'Ecommerce', 'Outros'])].copy()

    CANAIS_VALIDOS = {'sms', 'email', 'whats', 'Grupo Controle'}
    df = df[df['canal'].isin(CANAIS_VALIDOS)].copy()

    # SMS so tem envio (sent), nao tem read/click — leitura calculada so com email+whats
    CANAIS_COM_LEITURA = ['email', 'whats']

    dia_max_dt = df['data_envio'].max()
    dia_30_dt = dia_max_dt - pd.Timedelta(days=30)
    dia_30 = dia_30_dt.strftime('%Y-%m-%d')

    # Separa Grupo Controle: nao e disparo, e holdout pra medir lift
    df_controle = df[df['canal'] == 'Grupo Controle'].copy()
    df = df[df['canal'] != 'Grupo Controle'].copy()

    df_controle['nome_jornada'] = df_controle['nome_jornada'].fillna('').astype(str)
    grupo_controle = []
    for (mes, bu), g in df_controle.groupby(['mes', 'bu']):
        grupo_controle.append({'mes': mes, 'bu': bu, 'n': len(g)})
    controle_total = len(df_controle)

    # Jornadas que tem grupo controle (qualquer momento) — set para lookup rapido
    jornadas_com_controle = set(df_controle.loc[df_controle['nome_jornada'].str.strip() != '', 'nome_jornada'].unique())

    # Jornadas com controle nos ultimos 30 dias (para o flag de cobertura recente)
    df_controle_30 = df_controle[df_controle['data_envio'] >= dia_30_dt]
    jornadas_com_controle_30 = set(df_controle_30.loc[df_controle_30['nome_jornada'].str.strip() != '', 'nome_jornada'].unique())

    # 1. Cross-tab granular para filtros (sem Grupo Controle)
    # nome_jornada/nome_atividade incluidos pra permitir busca textual no front
    df['nome_jornada'] = df['nome_jornada'].fillna('').astype(str)
    df['nome_atividade'] = df['nome_atividade'].fillna('').astype(str)
    rows = df.groupby(['dia', 'mes', 'bu', 'canal', 'objetivo', 'metrica', 'flag_pontual', 'status', 'nome_jornada', 'nome_atividade']).size().reset_index(name='n')
    # Renomeia pra chaves curtas pra reduzir tamanho do JSON
    rows = rows.rename(columns={'nome_jornada': 'j', 'nome_atividade': 'a'})
    cross = rows.to_dict('records')

    # 2. Top jornadas (para tabela)
    top_jornadas = []
    for jornada, g in df.groupby('nome_jornada'):
        if len(g) < 2 or not jornada.strip():
            continue
        n = len(g)
        sc = g['status'].value_counts().to_dict()
        top_jornadas.append({
            'j': jornada[:80], 'n': n,
            'c': g['canal'].mode().iloc[0],
            'b': g['bu'].mode().iloc[0],
            'o': g['objetivo'].mode().iloc[0],
            'm': g['metrica'].mode().iloc[0],
            'f': g['flag_pontual'].mode().iloc[0],
            'mes_min': g['mes'].min(),
            'mes_max': g['mes'].max(),
            'del': sc.get('delivered', 0),
            'rd': sc.get('read', 0),
            'cl': sc.get('click', 0),
            'fl': sc.get('failed', 0),
            'snt': sc.get('sent', 0),
        })
    top_jornadas.sort(key=lambda x: x['n'], reverse=True)

    # 2b. Top jornadas em VOLUME de falhas (failed absoluto, nao taxa)
    # Util pra priorizar saneamento — uma jornada de 1M envios com 5% de falha tem mais
    # impacto absoluto que uma de 1k com 50%. Ordena por failed total desc.
    top_falhas = []
    for jornada, g in df.groupby('nome_jornada'):
        if not jornada.strip():
            continue
        n = len(g)
        if n < 100:
            continue
        sc = g['status'].value_counts().to_dict()
        fl = int(sc.get('failed', 0))
        if fl == 0:
            continue
        top_falhas.append({
            'j': jornada[:80], 'n': n, 'fl': fl,
            'taxa_falha': round(fl / n * 100, 1),
            'b': g['bu'].mode().iloc[0],
            'c': g['canal'].mode().iloc[0],
        })
    top_falhas.sort(key=lambda x: x['fl'], reverse=True)
    top_falhas = top_falhas[:20]

    # 3a. Heatmap dia da semana x mes
    heatmap = {}
    for (dia_sem, mes), g in df.groupby(['dia_semana', 'mes']):
        heatmap[f"{int(dia_sem)}_{mes}"] = len(g)

    # 3b. Melhor janela de envio: dia da semana com maior taxa de leitura por canal
    def taxa_leitura(g):
        t = len(g)
        if t == 0:
            return 0.0
        rd = (g['status'].isin(['read', 'click'])).sum()
        return round(rd / t * 100, 1)

    heatmap_canal = {}
    for canal, gc in df.groupby('canal'):
        key_canal = {}
        for dia_sem, g in gc.groupby('dia_semana'):
            key_canal[f"{int(dia_sem)}"] = {
                'n': len(g),
                'r': taxa_leitura(g),
            }
        heatmap_canal[canal] = key_canal

    # 4. Linha mensal por jornada
    jornada_mensal = []
    for (jornada, mes), g in df.groupby(['nome_jornada', 'mes']):
        if not jornada.strip():
            continue
        sc = g['status'].value_counts().to_dict()
        n = len(g)
        jornada_mensal.append({
            'j': jornada[:80],
            'mes': mes,
            'n': n,
            'del': int(sc.get('delivered', 0)),
            'rd': int(sc.get('read', 0)),
            'cl': int(sc.get('click', 0)),
            'fl': int(sc.get('failed', 0)),
            'snt': int(sc.get('sent', 0)),
        })

    # 5. Agregado mensal geral
    # Status cumulativos: entrega = sent+delivered+read+click (tudo que nao falhou).
    # SMS nao tem leitura — denominador de leitura eh so email+whats.
    # Click soh existe pra email — denominador de click eh so o volume email.
    mes_totais = []
    for mes, g in df.groupby('mes'):
        sc = g['status'].value_counts().to_dict()
        t = len(g)
        t_email = int((g['canal'] == 'email').sum())
        t_lei = int(g['canal'].isin(CANAIS_COM_LEITURA).sum())
        snt = sc.get('sent', 0)
        del_ = sc.get('delivered', 0)
        rd = sc.get('read', 0)
        cl = sc.get('click', 0)
        fl = sc.get('failed', 0)
        mes_totais.append({
            'mes': mes,
            'n': t,
            'entrega': round((snt + del_ + rd + cl) / t * 100, 1) if t else 0.0,
            'leitura': round((rd + cl) / t_lei * 100, 1) if t_lei else 0.0,
            'click': round(cl / t_email * 100, 1) if t_email else 0.0,
            'falha': round(fl / t * 100, 1) if t else 0.0,
        })
    mes_totais.sort(key=lambda x: x['mes'])

    # 5b. Melhor janela de envio: hora x dia_semana por canal (ultimos 30 dias)
    # ETL antigo trunca timestamp em 00:00 — descarta esses rows pra nao envenenar a janela.
    # Subset com hora real e pequeno (~1% do total) mas suficiente pra detectar padrao.
    df_30 = df[df['data_envio'] >= dia_30_dt]
    df_30_h = df_30[df_30['hora'] > 0]
    tem_hora_real = len(df_30_h) > 0

    # Por dia da semana (sempre disponivel)
    janela_canal_dia = {}
    for canal in CANAIS_COM_LEITURA:
        gc = df_30[df_30['canal'] == canal]
        if len(gc) == 0:
            continue
        cells = []
        for ds, g in gc.groupby('dia_semana'):
            n = len(g)
            rd_cl = int(g['status'].isin(['read', 'click']).sum())
            cells.append({
                'd': int(ds),
                'n': n,
                'r': round(rd_cl / n * 100, 1) if n else 0.0,
            })
        janela_canal_dia[canal] = cells

    # Por hora x dia da semana (so rows com hora real)
    janela_canal_hora = {}
    janela_hora_meta = {'cobertura_rows': len(df_30_h), 'total_30d': len(df_30)}
    if tem_hora_real:
        for canal in CANAIS_COM_LEITURA:
            gc = df_30_h[df_30_h['canal'] == canal]
            if len(gc) == 0:
                continue
            cells = []
            for (ds, hr), g in gc.groupby(['dia_semana', 'hora']):
                n = len(g)
                rd_cl = int(g['status'].isin(['read', 'click']).sum())
                cells.append({
                    'd': int(ds),
                    'h': int(hr),
                    'n': n,
                    'r': round(rd_cl / n * 100, 1) if n else 0.0,
                })
            janela_canal_hora[canal] = cells

    # 6b. Jornadas recorrentes SEM grupo controle nos ultimos 30 dias
    df_rec_30 = df_30[(df_30['flag_pontual'] == 'Recorrente') & (df_30['nome_jornada'].str.strip() != '')]
    jornadas_sem_controle = []
    for jornada, g in df_rec_30.groupby('nome_jornada'):
        if jornada in jornadas_com_controle_30:
            continue
        jornadas_sem_controle.append({
            'j': jornada[:80],
            'n': len(g),
            'b': g['bu'].mode().iloc[0],
            'c': g['canal'].mode().iloc[0],
            'o': g['objetivo'].mode().iloc[0] if g['objetivo'].notna().any() else '',
        })
    jornadas_sem_controle.sort(key=lambda x: x['n'], reverse=True)

    # 6. Jornadas ativas por mes
    jornadas_ativas_mes = df.groupby('mes')['nome_jornada'].nunique().to_dict()

    # 7. Listas de valores unicos para filtros
    meses = sorted(df['mes'].unique().tolist())
    dia_min = df['dia'].min()
    dia_max = df['dia'].max()
    bus = sorted(df['bu'].unique().tolist())
    canais = sorted(df['canal'].unique().tolist())
    objetivos = sorted(df['objetivo'].dropna().unique().tolist())
    metricas = sorted(df['metrica'].dropna().unique().tolist())
    campanhas = sorted(df['campanha'].dropna().unique().tolist()) if 'campanha' in df.columns else []

    data = {
        'cross': cross,
        'jornadas': top_jornadas[:50],
        'top_falhas': top_falhas,
        'jornada_mensal': jornada_mensal,
        'mes_totais': mes_totais,
        'jornadas_ativas_mes': jornadas_ativas_mes,
        'heatmap': heatmap,
        'heatmap_canal': heatmap_canal,
        'janela_canal': janela_canal_dia,
        'janela_canal_hora': janela_canal_hora,
        'janela_hora_meta': janela_hora_meta,
        'grupo_controle': grupo_controle,
        'grupo_controle_total': controle_total,
        'jornadas_sem_controle_30d': jornadas_sem_controle,
        'jornadas_com_controle_total': len(jornadas_com_controle),
        'dia_30d': dia_30,
        'observacoes': {
            'grupo_controle': 'Grupo Controle e usado apenas em algumas comunicacoes — nao cobre todas as jornadas.',
            'sms_leitura': 'SMS nao retorna status de leitura — apenas envio. Excluido de taxa de leitura e janela otima.',
            'janela_hora': 'Origem so traz data (sem hora) — janela otima quebrada apenas por dia da semana.' if not tem_hora_real else 'Janela otima por dia da semana e hora.',
        },
        'tem_hora_real': tem_hora_real,
        'filtros': {
            'meses': meses,
            'dia_min': dia_min,
            'dia_max': dia_max,
            'bus': bus,
            'canais': canais,
            'objetivos': objetivos,
            'metricas': metricas,
            'campanhas': campanhas,
        }
    }

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"data.json gerado! ({len(json.dumps(data, ensure_ascii=False))} bytes, {len(cross)} linhas cross-tab)")

    # export_data.json — cross com id_empresa agrupado por mes (para export estatico GitHub Pages)
    export_path = out_path.replace('data.json', 'export_data.json') if 'data.json' in out_path else out_path + '.export'
    _gerar_export_data(df, export_path)

    return data


def _gerar_export_data(df: pd.DataFrame, out_path: str):
    """Gera export_data.bin (gzip) com cross por empresa/jornada/canal/status para export estatico."""
    import gzip
    gz_path = out_path.replace('export_data.json', 'export_data.bin')
    if 'id_empresa' not in df.columns:
        print('[export] id_empresa nao encontrado, export_data.json.gz nao gerado.')
        return
    df = df.copy()
    df['id_empresa'] = df['id_empresa'].fillna('').astype(str)
    df['nome_jornada'] = df['nome_jornada'].fillna('').astype(str)
    # Agrupa por empresa+jornada+canal+status (todo historico, sem granularidade temporal)
    # bu/objetivo como atributo do grupo (mode), nao chave — evita explosao de linhas
    rows_list = []
    for (empresa, jornada, canal, status), g in df.groupby(['id_empresa', 'nome_jornada', 'canal', 'status']):
        rows_list.append({
            'e': empresa,
            'j': jornada[:80],
            'c': canal,
            's': status,
            'b': g['bu'].mode().iloc[0] if len(g) else '',
            'o': str(g['objetivo'].mode().iloc[0]) if g['objetivo'].notna().any() else '',
            'n': len(g),
        })
    raw = json.dumps({'rows': rows_list}, ensure_ascii=False).encode('utf-8')
    with gzip.open(gz_path, 'wb', compresslevel=9) as f:
        f.write(raw)
    import os
    sz = os.path.getsize(gz_path)
    print(f"export_data.bin gerado! ({sz/1024/1024:.1f} MB, {len(rows_list):,} linhas)")


if __name__ == '__main__':
    df = pd.read_csv('dados.csv', low_memory=False)
    process(df)
