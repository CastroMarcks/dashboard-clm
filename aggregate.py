"""Aggregation logic for the CRM dashboard.

All business-logic calculations that previously lived in index.html JavaScript
now live here. server.py calls compute_aggregate() for every /aggregate request.
"""
import math

THRESHOLDS = {
    'entrega': {'good': 80,  'warn': 60},
    'leitura': {'good': 30,  'warn': 10},
    'falha':   {'good': 3,   'warn': 8},
    'click':   {'good': 5,   'warn': 2},
    'weights': {'entrega': 0.40, 'leitura': 0.25, 'falha_inv': 0.25, 'click': 0.10},
}

CANAIS_COM_LEITURA = {'email', 'whats'}
CANAIS_COM_CLICK   = {'email'}


def _r1(n):
    return round(n * 10) / 10


def _num_br(n):
    return f'{int(n):,}'.replace(',', '.')


# ---------------------------------------------------------------------------
# Core aggregation
# ---------------------------------------------------------------------------

def agg(rows):
    total = del_ = rd = cl = fl = snt = total_email = total_lei = 0
    for r in rows:
        n = r['n']
        total += n
        canal = r['canal']
        if canal in CANAIS_COM_CLICK:
            total_email += n
        if canal in CANAIS_COM_LEITURA:
            total_lei += n
        s = r['status']
        if s == 'delivered':
            del_ += n
        elif s == 'read':
            rd += n
        elif s == 'click':
            cl += n
        elif s == 'failed':
            fl += n
        elif s == 'sent':
            snt += n

    return {
        'total':       total,
        'delivered':   del_,
        'read':        rd,
        'click':       cl,
        'failed':      fl,
        'sent':        snt,
        'total_email': total_email,
        'total_lei':   total_lei,
        'taxa_entrega': _r1((snt + del_ + rd + cl) / total * 100) if total else 0.0,
        'taxa_leitura': _r1((rd + cl) / total_lei * 100)          if total_lei else 0.0,
        'taxa_click':   _r1(cl / total_email * 100)               if total_email else 0.0,
        'taxa_falha':   _r1(fl / total * 100)                     if total else 0.0,
    }


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def filter_cross(cross, f):
    data_ini  = f.get('dataIni', '')
    data_fim  = f.get('dataFim', '')
    bu        = f.get('bu', '')
    canal     = f.get('canal', '')
    objetivo  = f.get('objetivo', '')
    metrica   = f.get('metrica', '')
    flag      = f.get('flag', '')
    jornadas  = set(f.get('jornadas',  []))
    atividades = set(f.get('atividades', []))

    out = []
    for r in cross:
        if data_ini  and r['dia']          < data_ini:   continue
        if data_fim  and r['dia']          > data_fim:   continue
        if bu        and r['bu']           != bu:        continue
        if canal     and r['canal']        != canal:     continue
        if objetivo  and r['objetivo']     != objetivo:  continue
        if metrica   and r['metrica']      != metrica:   continue
        if flag      and r['flag_pontual'] != flag:      continue
        if jornadas  and r.get('j', '')    not in jornadas:   continue
        if atividades and r.get('a', '')   not in atividades: continue
        out.append(r)
    return out


def _last_30(rows, dia_30d):
    if not dia_30d:
        return rows
    return [r for r in rows if r['dia'] >= dia_30d]


# ---------------------------------------------------------------------------
# Month-over-month
# ---------------------------------------------------------------------------

def build_mom(rows):
    months = sorted({r['mes'] for r in rows})
    if not months:
        return None
    curr_mes = months[-1]
    prev_mes = months[-2] if len(months) > 1 else None

    curr_rows = [r for r in rows if r['mes'] == curr_mes]
    max_day = max((int(r['dia'].split('-')[2]) for r in curr_rows), default=0)

    curr_agg = agg(curr_rows)
    prev_agg = None
    if prev_mes:
        prev_rows = [r for r in rows
                     if r['mes'] == prev_mes and int(r['dia'].split('-')[2]) <= max_day]
        prev_agg = agg(prev_rows)

    def pp(key):
        if prev_agg is None:
            return None
        return _r1(curr_agg[key] - prev_agg[key])

    def pct_delta(curr_n, prev_n):
        if prev_n is None or prev_n == 0:
            return None
        return _r1((curr_n - prev_n) / prev_n * 100)

    return {
        'curr_mes':       curr_mes,
        'prev_mes':       prev_mes,
        'max_day':        max_day,
        'total_delta_pct': pct_delta(curr_agg['total'], prev_agg['total'] if prev_agg else None),
        'entrega_pp':     pp('taxa_entrega'),
        'leitura_pp':     pp('taxa_leitura'),
        'click_pp':       pp('taxa_click'),
        'falha_pp':       pp('taxa_falha'),
    }


# ---------------------------------------------------------------------------
# Health score
# ---------------------------------------------------------------------------

def health_score(k):
    w = THRESHOLDS['weights']
    e = max(0.0, min(100.0, k['taxa_entrega']))
    l = max(0.0, min(100.0, k['taxa_leitura'] * 2))
    f = max(0.0, min(100.0, 100 - k['taxa_falha'] * 4))
    c = max(0.0, min(100.0, k['taxa_click'] * 10))
    score = round(e * w['entrega'] + l * w['leitura'] + f * w['falha_inv'] + c * w['click'])
    label = 'Saudavel' if score >= 75 else 'Atencao' if score >= 55 else 'Critico'
    color = '#059669' if score >= 75 else '#d97706' if score >= 55 else '#dc2626'
    return {'score': score, 'label': label, 'color': color}


# ---------------------------------------------------------------------------
# Alertas
# ---------------------------------------------------------------------------

def build_alertas(k, rows):
    T = THRESHOLDS
    alerts = []

    if k['taxa_falha'] > T['falha']['warn']:
        alerts.append({'t': 'crit', 'txt': f'<strong>Falha acima do limite:</strong> {k["taxa_falha"]}% (limite: {T["falha"]["warn"]}%). Verifique a base de contatos.'})
    elif k['taxa_falha'] > T['falha']['good']:
        alerts.append({'t': 'warn', 'txt': f'<strong>Falha em alerta:</strong> {k["taxa_falha"]}%. Monitorar proximas semanas.'})

    if k['taxa_leitura'] < T['leitura']['warn']:
        alerts.append({'t': 'crit', 'txt': f'<strong>Leitura baixa:</strong> {k["taxa_leitura"]}%. Revisar conteudo/segmentacao.'})
    elif k['taxa_leitura'] < T['leitura']['good']:
        alerts.append({'t': 'warn', 'txt': f'<strong>Leitura moderada:</strong> {k["taxa_leitura"]}%. Oportunidade de otimizar copy.'})

    if k['taxa_entrega'] < T['entrega']['warn']:
        alerts.append({'t': 'crit', 'txt': f'<strong>Entrega baixa:</strong> {k["taxa_entrega"]}%. Verificar configuracao de canal.'})
    elif k['taxa_entrega'] < T['entrega']['good']:
        alerts.append({'t': 'warn', 'txt': f'<strong>Entrega abaixo do ideal:</strong> {k["taxa_entrega"]}%.'})

    by_bu: dict[str, list] = {}
    for r in rows:
        by_bu.setdefault(r['bu'], []).append(r)
    for bu, bu_rows in by_bu.items():
        a = agg(bu_rows)
        if a['total'] < 10:
            continue
        if a['taxa_falha'] > T['falha']['warn']:
            alerts.append({'t': 'warn', 'txt': f'<strong>BU {bu}:</strong> falha em {a["taxa_falha"]}% ({_num_br(a["total"])} envios).'})

    if not alerts:
        alerts.append({'t': 'ok', 'txt': '<strong>Tudo sob controle.</strong> Nenhum indicador fora dos limites.'})

    return alerts[:6]


# ---------------------------------------------------------------------------
# Group aggregation (for bar charts)
# ---------------------------------------------------------------------------

def _group_by(rows, key):
    groups: dict[str, list] = {}
    for r in rows:
        groups.setdefault(r.get(key, ''), []).append(r)
    return groups


def group_agg(rows, key, metric):
    groups = _group_by(rows, key)
    result = []
    for name, grp in groups.items():
        a = agg(grp)
        result.append({'name': name, 'val': a[metric], 'total': a['total']})
    result.sort(key=lambda x: x['total'], reverse=True)
    return result


# ---------------------------------------------------------------------------
# Jornadas em atencao (2+ meses consecutivos de queda em leitura)
# ---------------------------------------------------------------------------

def get_jornadas_atencao(jornada_mensal, mes_ini='', mes_fim=''):
    por_j: dict[str, list] = {}
    for r in jornada_mensal:
        if mes_ini and r['mes'] < mes_ini:
            continue
        if mes_fim and r['mes'] > mes_fim:
            continue
        por_j.setdefault(r['j'], []).append(r)

    out = []
    for j, rows in por_j.items():
        rows_s = sorted(rows, key=lambda x: x['mes'])
        if len(rows_s) < 3:
            continue
        def tl(r):
            t = r['n']
            return _r1((r['rd'] + r['cl']) / t * 100) if t else 0.0
        n = len(rows_s)
        diff1 = tl(rows_s[n-1]) - tl(rows_s[n-2])
        diff2 = tl(rows_s[n-2]) - tl(rows_s[n-3])
        if diff1 < 0 and diff2 < 0 and (diff1 + diff2) < -5:
            out.append({
                'j':     j,
                'delta': _r1(tl(rows_s[n-1]) - tl(rows_s[n-3])),
                'curr':  tl(rows_s[n-1]),
                'prev':  tl(rows_s[n-3]),
            })
    return out


# ---------------------------------------------------------------------------
# Per-journey MoM delta (for the journeys table)
# ---------------------------------------------------------------------------

def get_jornada_deltas(top_jornadas, filtered, mom):
    if not mom or not mom.get('curr_mes') or not mom.get('prev_mes'):
        return {}
    curr_mes = mom['curr_mes']
    prev_mes = mom['prev_mes']
    max_day  = mom['max_day']

    deltas = {}
    for j in top_jornadas:
        nome = j['j']
        curr_rows = [r for r in filtered if r.get('j') == nome and r['mes'] == curr_mes]
        prev_rows = [r for r in filtered
                     if r.get('j') == nome and r['mes'] == prev_mes
                     and int(r['dia'].split('-')[2]) <= max_day]
        if not curr_rows or not prev_rows:
            continue
        ac, ap = agg(curr_rows), agg(prev_rows)
        deltas[nome] = _r1(ac['taxa_leitura'] - ap['taxa_leitura'])
    return deltas


# ---------------------------------------------------------------------------
# Linear regression + projection
# ---------------------------------------------------------------------------

def project_next(vals):
    n = len(vals)
    if n < 2:
        return None
    sum_x = sum_y = sum_xy = sum_x2 = 0.0
    for i, v in enumerate(vals):
        sum_x  += i
        sum_y  += v
        sum_xy += i * v
        sum_x2 += i * i
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return None
    slope     = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n
    next_val  = intercept + slope * n
    sum_r2    = sum((v - (intercept + slope * i)) ** 2 for i, v in enumerate(vals))
    std       = math.sqrt(sum_r2 / n)
    return {'next': next_val, 'low': next_val - std, 'high': next_val + std, 'slope': slope}


def build_projections(timeline):
    if len(timeline) < 3:
        return None
    recent = timeline[-6:]
    last_mes_parts = recent[-1]['mes'].split('-')
    y, mo = int(last_mes_parts[0]), int(last_mes_parts[1])
    mo += 1
    if mo > 12:
        mo = 1
        y += 1
    next_mes = f'{y}-{mo:02d}'

    return {
        'next_mes': next_mes,
        'volume':   project_next([t['total']        for t in recent]),
        'entrega':  project_next([t['taxa_entrega'] for t in recent]),
        'leitura':  project_next([t['taxa_leitura'] for t in recent]),
        'falha':    project_next([t['taxa_falha']   for t in recent]),
    }


# ---------------------------------------------------------------------------
# Timeline (by month, from filtered cross)
# ---------------------------------------------------------------------------

def build_timeline(filtered):
    by_mes = _group_by(filtered, 'mes')
    timeline = []
    for mes in sorted(by_mes):
        a = agg(by_mes[mes])
        a['mes'] = mes
        timeline.append(a)
    return timeline


# ---------------------------------------------------------------------------
# Canais section
# ---------------------------------------------------------------------------

def build_canais(filtered):
    by_canal = _group_by(filtered, 'canal')
    canal_arr = []
    for c, rows in by_canal.items():
        a = agg(rows)
        if a['total'] < 2:
            continue
        canal_arr.append({'canal': c, 'agg': a})
    canal_arr.sort(key=lambda x: x['agg']['total'], reverse=True)

    # Benchmark
    bench_ent = bench_fal = bench_lei = n_lei = 0.0
    n = len(canal_arr) or 1
    for item in canal_arr:
        bench_ent += item['agg']['taxa_entrega']
        bench_fal += item['agg']['taxa_falha']
        if item['canal'] in CANAIS_COM_LEITURA:
            bench_lei += item['agg']['taxa_leitura']
            n_lei += 1
    bench_ent = _r1(bench_ent / n)
    bench_fal = _r1(bench_fal / n)
    bench_lei = _r1(bench_lei / n_lei) if n_lei else 0.0

    result = []
    for item in canal_arr:
        c, a = item['canal'], item['agg']
        tem_lei = c in CANAIS_COM_LEITURA
        d_ent = _r1(a['taxa_entrega'] - bench_ent)
        d_lei = _r1(a['taxa_leitura'] - bench_lei) if tem_lei else None
        result.append({
            'canal':    c,
            'agg':      a,
            'tem_lei':  tem_lei,
            'tem_cli':  c in CANAIS_COM_CLICK,
            'd_ent':    d_ent,
            'd_lei':    d_lei,
            'bench_ent': bench_ent,
            'bench_lei': bench_lei,
        })

    # BU x canal
    bu_canal: dict[str, list] = {}
    for r in filtered:
        bu_canal.setdefault(f"{r['bu']}||{r['canal']}", []).append(r)
    bc_arr = []
    for key, rows in bu_canal.items():
        bu, canal = key.split('||')
        a = agg(rows)
        if a['total'] < 2:
            continue
        bc_arr.append({'bu': bu, 'canal': canal, 'agg': a, 'tem_lei': canal in CANAIS_COM_LEITURA})
    bc_arr.sort(key=lambda x: x['agg']['total'], reverse=True)

    # Flag
    by_flag = _group_by(filtered, 'flag_pontual')
    flag_arr = []
    for fl, rows in by_flag.items():
        a = agg(rows)
        flag_arr.append({'flag': fl, 'agg': a})

    return {
        'canais':   result,
        'bu_canal': bc_arr,
        'flags':    flag_arr,
    }


# ---------------------------------------------------------------------------
# Insights
# ---------------------------------------------------------------------------

def build_insights(filtered, dia_30d):
    rows30 = _last_30(filtered, dia_30d)
    by_canal = _group_by(rows30, 'canal')
    by_bu    = _group_by(rows30, 'bu')
    by_obj   = _group_by(rows30, 'objetivo')
    by_met   = _group_by(rows30, 'metrica')
    by_flag  = _group_by(rows30, 'flag_pontual')

    T = THRESHOLDS

    def best(groups, metric, direction='desc'):
        arr = []
        for name, rows in groups.items():
            a = agg(rows)
            if a['total'] >= 5:
                arr.append({'name': name, 'agg': a})
        if not arr:
            return None
        arr.sort(key=lambda x: x['agg'][metric], reverse=(direction == 'desc'))
        return arr[0]

    def best_canal_lei():
        arr = [{'name': c, 'agg': agg(rows)}
               for c, rows in by_canal.items()
               if c in CANAIS_COM_LEITURA and agg(rows)['total'] >= 5]
        if not arr:
            return None
        arr.sort(key=lambda x: x['agg']['taxa_leitura'], reverse=True)
        return arr[0]

    bcl   = best_canal_lei()
    wbf   = best(by_bu,  'taxa_falha',   'desc')
    bbe   = best(by_bu,  'taxa_entrega', 'desc')
    bol   = best(by_obj, 'taxa_leitura', 'desc')
    bml   = best(by_met, 'taxa_leitura', 'desc')

    insights = []
    if bcl:
        insights.append({'type': 'success', 'icon': '📱', 'title': f'{bcl["name"]} lidera leitura',
                          'body': f'Canal {bcl["name"]} tem {bcl["agg"]["taxa_leitura"]}% de taxa de leitura com {_num_br(bcl["agg"]["total"])} disparos.', 'tag': 'Destaque', 'tagCls': 'tag-s'})
    if wbf and wbf['agg']['taxa_falha'] > T['falha']['good']:
        insights.append({'type': 'danger', 'icon': '⚠️', 'title': f'{wbf["name"]}: alta falha',
                          'body': f'BU {wbf["name"]} tem {wbf["agg"]["taxa_falha"]}% de falha ({_num_br(wbf["agg"]["failed"])} falhadas). Revisar base de contatos.', 'tag': 'Alerta', 'tagCls': 'tag-d'})
    if bbe:
        insights.append({'type': 'info', 'icon': '✅', 'title': f'{bbe["name"]}: melhor entrega',
                          'body': f'BU {bbe["name"]} lidera com {bbe["agg"]["taxa_entrega"]}% de taxa de entrega.', 'tag': 'Referencia', 'tagCls': 'tag-i'})
    if bol:
        insights.append({'type': 'success', 'icon': '🎯', 'title': f'{bol["name"]} engaja mais',
                          'body': f'Objetivo "{bol["name"]}" tem {bol["agg"]["taxa_leitura"]}% de leitura — maior entre os objetivos filtrados.', 'tag': 'Oportunidade', 'tagCls': 'tag-s'})
    if bml:
        insights.append({'type': 'info', 'icon': '🏆', 'title': f'Metrica: {bml["name"]}',
                          'body': f'Metrica "{bml["name"]}" lidera leitura com {bml["agg"]["taxa_leitura"]}% ({_num_br(bml["agg"]["total"])} atividades).', 'tag': 'Destaque', 'tagCls': 'tag-i'})

    rec = agg(by_flag.get('Recorrente', []))
    pon = agg(by_flag.get('Pontual',    []))
    if rec['total'] >= 5 and pon['total'] >= 5:
        winner = 'Recorrentes engajam mais.' if rec['taxa_leitura'] > pon['taxa_leitura'] else 'Pontuais engajam mais neste corte.'
        insights.append({'type': 'warning', 'icon': '🔄', 'title': 'Recorrente vs Pontual',
                          'body': f'Recorrentes: {rec["taxa_leitura"]}% leitura vs Pontuais: {pon["taxa_leitura"]}%. {winner}', 'tag': 'Comparativo', 'tagCls': 'tag-w'})

    k30 = agg(rows30)
    padroes = []
    sms_a = agg(by_canal.get('sms', []))
    if bcl:
        padroes.append({'ico': '📱', 'txt': f'<strong>{bcl["name"]} e o melhor canal para leitura</strong> com {bcl["agg"]["taxa_leitura"]}% no periodo filtrado.'})
    if sms_a['total'] > 0:
        padroes.append({'ico': '💬', 'txt': f'<strong>SMS garante alcance:</strong> {sms_a["taxa_entrega"]}% de entrega.'})
    if rec['total'] > 0 and rec['taxa_leitura'] > 10:
        padroes.append({'ico': '🔁', 'txt': f'<strong>Jornadas recorrentes:</strong> {rec["taxa_leitura"]}% de leitura. Sequencias educam e retêm.'})
    if k30['taxa_falha'] > T['falha']['warn']:
        padroes.append({'ico': '⚠️', 'txt': f'<strong>Taxa de falha acima de {T["falha"]["warn"]}%:</strong> {k30["taxa_falha"]}% geral. Recomenda-se higienizacao da base.'})
    else:
        padroes.append({'ico': '✅', 'txt': f'<strong>Taxa de falha controlada:</strong> {k30["taxa_falha"]}% esta dentro do aceitavel.'})

    metrica_rank = []
    for name, rows in by_met.items():
        a = agg(rows)
        if a['total'] >= 2:
            metrica_rank.append({'name': name, 'val': a['taxa_leitura'], 'total': a['total']})
    metrica_rank.sort(key=lambda x: x['taxa_leitura'], reverse=True)

    scatter = []
    for name, rows in by_obj.items():
        a = agg(rows)
        if a['total'] >= 5:
            scatter.append({'name': name, 'total': a['total'], 'leit': a['taxa_leitura'],
                            'ent': a['taxa_entrega'], 'fal': a['taxa_falha']})

    return {
        'cards':        insights,
        'padroes':      padroes,
        'metrica_rank': metrica_rank[:8],
        'scatter':      scatter,
    }


# ---------------------------------------------------------------------------
# Journeys section
# ---------------------------------------------------------------------------

def build_jornadas_section(data, filtered, f, mom):
    T = THRESHOLDS
    mes_ini = f.get('mesIni', '')
    mes_fim = f.get('mesFim', '')

    top_jornadas = data.get('jornadas', [])
    jornada_mensal = data.get('jornada_mensal', [])

    # Filter the pre-computed top_jornadas list
    jornadas = []
    for j in top_jornadas:
        if f.get('bu')      and j['b'] != f['bu']:      continue
        if f.get('canal')   and j['c'] != f['canal']:   continue
        if f.get('objetivo') and j['o'] != f.get('objetivo'): continue
        if f.get('metrica') and j['m'] != f.get('metrica'):   continue
        if f.get('flag')    and j['f'] != f.get('flag'):      continue
        if mes_ini and j['mes_max'] < mes_ini: continue
        if mes_fim and j['mes_min'] > mes_fim: continue
        jornadas.append(j)

    atencao_list = get_jornadas_atencao(jornada_mensal, mes_ini, mes_fim)
    atencao_set  = {a['j']: a for a in atencao_list}

    deltas = get_jornada_deltas(jornadas, filtered, mom)

    saudaveis, atencao, criticas = [], [], []
    for j in jornadas:
        t  = j['del'] + j['rd'] + j['cl'] + j['fl'] + j['snt']
        tf = _r1(j['fl'] / t * 100) if t else 0.0
        tem_lei = j['c'] in CANAIS_COM_LEITURA
        tl = _r1((j['rd'] + j['cl']) / t * 100) if (tem_lei and t) else None
        te = _r1((j['snt'] + j['del'] + j['rd'] + j['cl']) / t * 100) if t else 0.0
        health_badge = ('badge-ok' if tf <= T['falha']['good']
                        else 'badge-warn' if tf <= T['falha']['warn'] else 'badge-danger')
        health_label = ('Saudavel' if tf <= T['falha']['good']
                        else 'Atencao' if tf <= T['falha']['warn'] else 'Critico')
        entry = {**j, '_t': t, '_tf': tf, '_tl': tl, '_te': te,
                 '_tem_lei': tem_lei, '_health_badge': health_badge,
                 '_health_label': health_label,
                 '_delta_leitura': deltas.get(j['j'])}
        if tf > T['falha']['warn'] or j['j'] in atencao_set:
            criticas.append(entry)
        elif tf > T['falha']['good'] or (tem_lei and tl is not None and tl < T['leitura']['warn']):
            atencao.append(entry)
        else:
            saudaveis.append(entry)

    return {
        'jornadas':       jornadas[:30],
        'saudaveis':      saudaveis,
        'atencao':        atencao,
        'criticas':       criticas,
        'atencao_set':    atencao_set,
        'top_falhas':     data.get('top_falhas', []),
        'sem_controle':   data.get('jornadas_sem_controle_30d', []),
    }


# ---------------------------------------------------------------------------
# Grupo controle (filtered by mes/bu)
# ---------------------------------------------------------------------------

def build_grupo_controle(grupo_controle_raw, kpis, f):
    mes_ini = f.get('mesIni', '')
    mes_fim = f.get('mesFim', '')
    bu      = f.get('bu', '')

    total = 0
    for r in grupo_controle_raw:
        if mes_ini and r['mes'] < mes_ini: continue
        if mes_fim and r['mes'] > mes_fim: continue
        if bu and r['bu'] != bu: continue
        total += r['n']

    base = kpis['total'] + total
    pct  = _r1(total / base * 100) if base else 0.0
    return {'total': total, 'holdout_pct': pct}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def compute_aggregate(data, filters):
    """Run all aggregations and return a dict ready to JSON-serialize."""
    f = filters
    f['mesIni'] = f.get('dataIni', '')[:7]
    f['mesFim'] = f.get('dataFim', '')[:7]

    cross          = data.get('cross', [])
    dia_30d        = data.get('dia_30d')
    jornada_mensal = data.get('jornada_mensal', [])
    mes_totais     = data.get('mes_totais', [])

    filtered = filter_cross(cross, f)
    rows30   = _last_30(filtered, dia_30d)

    kpis    = agg(filtered)
    kpis30  = agg(rows30)
    mom     = build_mom(filtered)
    health  = health_score(kpis30)
    alertas = build_alertas(kpis30, rows30)

    timeline = build_timeline(filtered)
    proj     = build_projections(timeline)

    canais_data   = build_canais(filtered)
    insights_data = build_insights(filtered, dia_30d)
    jornadas_data = build_jornadas_section(data, filtered, f, mom)
    gc_data       = build_grupo_controle(data.get('grupo_controle', []), kpis, f)

    # Jornadas ativas no periodo
    jorns_ativas_set = set()
    for r in jornada_mensal:
        if f['mesIni'] and r['mes'] < f['mesIni']: continue
        if f['mesFim'] and r['mes'] > f['mesFim']: continue
        jorns_ativas_set.add(r['j'])
    jornadas_ativas = len(jorns_ativas_set)

    # Timeline MoM (for volume tab)
    vol_mom = build_mom(filtered)

    # Group bars data
    por_bu      = group_agg(filtered, 'bu',        'taxa_entrega')
    por_objetivo = group_agg(filtered, 'objetivo', 'taxa_leitura')
    por_metrica  = group_agg(filtered, 'metrica',  'taxa_leitura')
    por_status_raw = {}
    for r in filtered:
        por_status_raw.setdefault(r['status'], 0)
        por_status_raw[r['status']] += r['n']
    por_status = [{'status': s, 'n': n} for s, n in por_status_raw.items()]
    por_status.sort(key=lambda x: x['n'], reverse=True)

    # Saude BU (for timeline tab)
    by_bu_tl = _group_by(filtered, 'bu')
    saude_bu = []
    for bu, rows in by_bu_tl.items():
        a = agg(rows)
        saude_bu.append({'bu': bu, 'agg': a})
    saude_bu.sort(key=lambda x: x['agg']['total'], reverse=True)

    return {
        'kpis':            kpis,
        'mom':             mom,
        'health':          health,
        'alertas':         alertas,
        'por_bu':          por_bu,
        'por_objetivo':    por_objetivo,
        'por_metrica':     por_metrica,
        'por_status':      por_status,
        'canais':          canais_data,
        'jornadas_ativas': jornadas_ativas,
        'jornadas_atencao': jornadas_data['atencao'],
        'jornadas_section': jornadas_data,
        'grupo_controle':  gc_data,
        'timeline':        timeline,
        'projecao':        proj,
        'saude_bu':        saude_bu,
        'insights':        insights_data,
        'dia_30d':         dia_30d,
        'thresholds':      THRESHOLDS,
    }
