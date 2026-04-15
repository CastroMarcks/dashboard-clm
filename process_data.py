import pandas as pd
import json

df = pd.read_csv('dados.csv')
df['data_envio'] = pd.to_datetime(df['data_envio'])
df['mes'] = df['data_envio'].dt.strftime('%Y-%m')
df['hora'] = df['data_envio'].dt.hour
df['dia_semana'] = df['data_envio'].dt.dayofweek

bu_map = {'ERP': 'ERP', 'Conta': 'Conta Digital', 'Envios': 'Envios', 'Loja': 'Loja', 'Ecommerce': 'Ecommerce', 'Outros': 'Outros'}
df['bu'] = df['bu'].map(bu_map).fillna('Outros')

# 1. Cross-tab granular para filtros
rows = df.groupby(['mes', 'bu', 'canal', 'objetivo', 'metrica', 'flag_pontual', 'status']).size().reset_index(name='n')
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

# 3. Heatmap dia x hora
heatmap = {}
for (dia, hora), g in df.groupby(['dia_semana', 'hora']):
    heatmap[f"{int(dia)}_{int(hora)}"] = len(g)

# 4. Listas de valores unicos para filtros
meses = sorted(df['mes'].unique().tolist())
bus = sorted(df['bu'].unique().tolist())
canais = sorted(df['canal'].unique().tolist())
objetivos = sorted(df['objetivo'].unique().tolist())
metricas = sorted(df['metrica'].unique().tolist())

data = {
    'cross': cross,
    'jornadas': top_jornadas[:50],
    'heatmap': heatmap,
    'filtros': {
        'meses': meses,
        'bus': bus,
        'canais': canais,
        'objetivos': objetivos,
        'metricas': metricas,
    }
}

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False)

print(f"data.json gerado! ({len(json.dumps(data, ensure_ascii=False))} bytes, {len(cross)} linhas cross-tab)")
