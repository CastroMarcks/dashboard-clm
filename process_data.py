import pandas as pd
import json

df = pd.read_csv('dados.csv')
df['data_envio'] = pd.to_datetime(df['data_envio'])
df['mes'] = df['data_envio'].dt.strftime('%Y-%m')
df['hora'] = df['data_envio'].dt.hour
df['dia_semana'] = df['data_envio'].dt.dayofweek  # 0=Seg

# Limpar BUs pequenas
bu_map = {'ERP': 'ERP', 'Conta': 'Conta Digital', 'Envios': 'Envios', 'Loja': 'Loja', 'Ecommerce': 'Ecommerce', 'Outros': 'Outros'}
df['bu_clean'] = df['bu'].map(bu_map).fillna('Outros')

# --- KPIs gerais ---
total = len(df)
status_counts = df['status'].value_counts().to_dict()
delivered = status_counts.get('delivered', 0)
read_count = status_counts.get('read', 0)
click = status_counts.get('click', 0)
sent = status_counts.get('sent', 0)
failed = status_counts.get('failed', 0)

kpis = {
    'total': total,
    'delivered': delivered,
    'read': read_count,
    'click': click,
    'sent': sent,
    'failed': failed,
    'taxa_entrega': round((delivered + read_count + click) / total * 100, 1),
    'taxa_leitura': round((read_count + click) / total * 100, 1),
    'taxa_click': round(click / total * 100, 2),
    'taxa_falha': round(failed / total * 100, 1),
}

# --- Por BU ---
by_bu = []
for bu, g in df.groupby('bu_clean'):
    n = len(g)
    sc = g['status'].value_counts().to_dict()
    by_bu.append({
        'bu': bu, 'total': n,
        'delivered': sc.get('delivered', 0),
        'read': sc.get('read', 0),
        'click': sc.get('click', 0),
        'failed': sc.get('failed', 0),
        'taxa_entrega': round((sc.get('delivered', 0) + sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_leitura': round((sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_falha': round(sc.get('failed', 0) / n * 100, 1),
    })
by_bu.sort(key=lambda x: x['total'], reverse=True)

# --- Por Canal ---
by_canal = []
for canal, g in df.groupby('canal'):
    if len(g) < 3:
        continue
    n = len(g)
    sc = g['status'].value_counts().to_dict()
    by_canal.append({
        'canal': canal, 'total': n,
        'delivered': sc.get('delivered', 0),
        'read': sc.get('read', 0),
        'click': sc.get('click', 0),
        'failed': sc.get('failed', 0),
        'taxa_entrega': round((sc.get('delivered', 0) + sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_leitura': round((sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_falha': round(sc.get('failed', 0) / n * 100, 1),
    })
by_canal.sort(key=lambda x: x['total'], reverse=True)

# --- Por Objetivo ---
by_objetivo = []
for obj, g in df.groupby('objetivo'):
    n = len(g)
    sc = g['status'].value_counts().to_dict()
    by_objetivo.append({
        'objetivo': obj, 'total': n,
        'taxa_entrega': round((sc.get('delivered', 0) + sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_leitura': round((sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_falha': round(sc.get('failed', 0) / n * 100, 1),
    })
by_objetivo.sort(key=lambda x: x['total'], reverse=True)

# --- Por Métrica ---
by_metrica = []
for met, g in df.groupby('metrica'):
    n = len(g)
    sc = g['status'].value_counts().to_dict()
    by_metrica.append({
        'metrica': met, 'total': n,
        'taxa_entrega': round((sc.get('delivered', 0) + sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_leitura': round((sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_falha': round(sc.get('failed', 0) / n * 100, 1),
    })
by_metrica.sort(key=lambda x: x['total'], reverse=True)

# --- Timeline mensal ---
timeline = []
for mes, g in df.groupby('mes'):
    n = len(g)
    sc = g['status'].value_counts().to_dict()
    timeline.append({
        'mes': mes, 'total': n,
        'delivered': sc.get('delivered', 0),
        'read': sc.get('read', 0),
        'click': sc.get('click', 0),
        'failed': sc.get('failed', 0),
        'taxa_entrega': round((sc.get('delivered', 0) + sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_leitura': round((sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_falha': round(sc.get('failed', 0) / n * 100, 1),
    })
timeline.sort(key=lambda x: x['mes'])

# --- Heatmap dia x hora ---
heatmap = {}
for (dia, hora), g in df.groupby(['dia_semana', 'hora']):
    heatmap[f"{int(dia)}_{int(hora)}"] = len(g)

# --- Flag pontual vs recorrente ---
by_flag = []
for flag, g in df.groupby('flag_pontual'):
    n = len(g)
    sc = g['status'].value_counts().to_dict()
    by_flag.append({
        'flag': flag, 'total': n,
        'taxa_entrega': round((sc.get('delivered', 0) + sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_leitura': round((sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_falha': round(sc.get('failed', 0) / n * 100, 1),
    })

# --- BU x Canal cruzado ---
bu_canal = []
for (bu, canal), g in df.groupby(['bu_clean', 'canal']):
    if len(g) < 3:
        continue
    n = len(g)
    sc = g['status'].value_counts().to_dict()
    bu_canal.append({
        'bu': bu, 'canal': canal, 'total': n,
        'taxa_entrega': round((sc.get('delivered', 0) + sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_leitura': round((sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_falha': round(sc.get('failed', 0) / n * 100, 1),
    })

# --- Top jornadas (agrupadas) ---
top_jornadas = []
for jornada, g in df.groupby('nome_jornada'):
    if len(g) < 2 or not jornada.strip():
        continue
    n = len(g)
    sc = g['status'].value_counts().to_dict()
    canais = g['canal'].value_counts().to_dict()
    top_jornadas.append({
        'jornada': jornada[:80], 'total': n,
        'canal_principal': max(canais, key=canais.get),
        'bu': g['bu_clean'].mode().iloc[0],
        'taxa_entrega': round((sc.get('delivered', 0) + sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_leitura': round((sc.get('read', 0) + sc.get('click', 0)) / n * 100, 1),
        'taxa_falha': round(sc.get('failed', 0) / n * 100, 1),
    })
top_jornadas.sort(key=lambda x: x['total'], reverse=True)

data = {
    'kpis': kpis,
    'by_bu': by_bu,
    'by_canal': by_canal,
    'by_objetivo': by_objetivo,
    'by_metrica': by_metrica,
    'timeline': timeline,
    'heatmap': heatmap,
    'by_flag': by_flag,
    'bu_canal': bu_canal,
    'top_jornadas': top_jornadas[:30],
}

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False)

print(f"data.json gerado com sucesso! ({len(json.dumps(data))} bytes)")
