"""Gera data_2fa.json com análise before/after do disparo 2FA de 12/05/2026.

Uso:
    python analyze_2fa.py

Entrada:  dados.csv  +  Lead_2fa - Base_Insights_2026-05-05_to_2026-05-13.csv
Saída:    data_2fa.json
"""
import json
import pandas as pd

CSV_DADOS  = 'dados.csv'
CSV_2FA    = 'Lead_2fa - Base_Insights_2026-05-05_to_2026-05-13.csv'
OUT        = 'data_2fa.json'
DISPARO    = '2026-05-12'
CANAL      = 'whats'


def metricas(g):
    t = len(g)
    if t == 0:
        return {'total': 0, 'read': 0, 'entrega': 0, 'falha': 0,
                'tx_lei': 0, 'tx_ent': 0, 'tx_fal': 0}
    rd  = int((g['status'].isin(['read', 'click'])).sum())
    fl  = int((g['status'] == 'failed').sum())
    snt = int((g['status'] == 'sent').sum())
    dl  = int((g['status'] == 'delivered').sum())
    ent = snt + dl + rd
    return {
        'total': int(t),
        'read':  rd,
        'entrega': ent,
        'falha': fl,
        'tx_lei': round(rd / t * 100, 1),
        'tx_ent': round(ent / t * 100, 1),
        'tx_fal': round(fl / t * 100, 1),
    }


def main():
    print('[load] lendo CSVs...')
    df = pd.read_csv(CSV_DADOS, dtype=str)
    lf = pd.read_csv(CSV_2FA)

    lead_ids = set(lf['lead_id'].astype(str))
    print(f'[2fa]  leads na base: {len(lead_ids):,}')

    grupo = df[(df['id_empresa'].isin(lead_ids)) & (df['canal'] == CANAL)].copy()
    grupo['data_envio'] = pd.to_datetime(grupo['data_envio'])
    grupo['dia'] = grupo['data_envio'].dt.strftime('%Y-%m-%d')
    print(f'[2fa]  registros whats do grupo: {len(grupo):,}  ({grupo["dia"].min()} a {grupo["dia"].max()})')

    # Timeline semanal
    grupo['semana'] = grupo['data_envio'].dt.to_period('W-MON').apply(
        lambda r: r.start_time.strftime('%Y-%m-%d'))
    timeline_sem = []
    for sem, g in sorted(grupo.groupby('semana')):
        m = metricas(g)
        m['semana'] = sem
        timeline_sem.append(m)

    # Timeline diária (últimos 60 dias antes do disparo para detalhe)
    cutoff_60 = pd.Timestamp(DISPARO) - pd.Timedelta(days=60)
    grupo_60 = grupo[grupo['data_envio'] >= cutoff_60]
    timeline_dia = []
    for dia, g in sorted(grupo_60.groupby('dia')):
        m = metricas(g)
        m['dia'] = dia
        timeline_dia.append(m)

    # Before/after
    g_before = grupo[grupo['dia'] < DISPARO]
    g_after  = grupo[grupo['dia'] >= DISPARO]

    # Baseline 30d (últimos 30 dias antes do disparo)
    cut30 = (pd.Timestamp(DISPARO) - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
    g_30d = grupo[(grupo['dia'] >= cut30) & (grupo['dia'] < DISPARO)]

    # Confirmações 2FA por dia
    lf['time_dt'] = pd.to_datetime(lf['Time'])
    lf['dia_conf'] = lf['time_dt'].dt.strftime('%Y-%m-%d')
    confs = lf.groupby('dia_conf').size().reset_index(name='n')
    confirmacoes = [{'dia': r['dia_conf'], 'n': int(r['n'])} for _, r in confs.iterrows()]

    # Status das versões 2FA
    col_a = 'A. Uniques of [Onboarding] sucesso confirmar 2FA'
    col_b = 'B. Uniques of [Onboarding] sucesso confirmar 2FA v2'
    col_c = 'C. Uniques of [Onboarding] sucesso confirmar 2FA v2.1'
    versoes = {
        'v1':   int((lf[col_a] == 1).sum()),
        'v2':   int((lf[col_b] == 1).sum()),
        'v2_1': int((lf[col_c] == 1).sum()),
    }

    out = {
        'disparo_date':   DISPARO,
        'total_leads':    len(lead_ids),
        'max_dia_whats':  grupo['dia'].max() if len(grupo) else None,
        'before':         metricas(g_before),
        'before_30d':     metricas(g_30d),
        'after':          metricas(g_after),
        'timeline_semanal': timeline_sem,
        'timeline_diaria':  timeline_dia,
        'confirmacoes':   confirmacoes,
        'versoes_2fa':    versoes,
    }

    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)

    print(f'[ok]   {OUT} gerado')
    print(f'       before total: {out["before"]["total"]:,}  leitura {out["before"]["tx_lei"]}%  falha {out["before"]["tx_fal"]}%')
    print(f'       before 30d:   {out["before_30d"]["total"]:,}  leitura {out["before_30d"]["tx_lei"]}%  falha {out["before_30d"]["tx_fal"]}%')
    print(f'       after:        {out["after"]["total"]:,}  (dados ainda chegando)')


if __name__ == '__main__':
    main()
