"""Dashboard server with auto-refresh and server-side aggregation.

Endpoints:
  GET /               – serves index.html
  GET /data.enc.json  – serves encrypted data (auto-regenerates if stale)
  GET /refresh        – force regeneration of data.json + data.enc.json
  GET /data-status    – JSON metadata about freshness
  GET /aggregate      – run all calculations server-side given filter params
"""
import http.server
import json
import os
import socketserver
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs

BASE   = Path(__file__).parent.resolve()
CSV    = BASE / 'dados.csv'
JSON   = BASE / 'data.json'
SCRIPT = BASE / 'process_data.py'
PORT   = 8080

_lock         = threading.Lock()
_last_refresh = None
_data_cache: dict | None = None   # in-memory cache of data.json

_raw_lock = threading.Lock()
_raw_df   = None   # lazy-loaded pandas DataFrame of raw dados.csv

RAW_COLS = ['id', 'contato', 'lead_id', 'data_envio', 'nome_jornada', 'nome_atividade',
            'canal', 'status', 'id_empresa', 'flag_pontual', 'objetivo',
            'bu', 'campanha', 'metrica', 'date_partition']
BU_MAP   = {'ERP': 'ERP', 'Conta': 'Conta Digital', 'Envios': 'Envios',
            'Ecommerce': 'Ecommerce', 'Outros': 'Outros'}


def _load_raw():
    global _raw_df
    if _raw_df is not None:
        return _raw_df
    with _raw_lock:
        if _raw_df is not None:
            return _raw_df
        if not CSV.exists():
            return None
        print('[raw] carregando dados.csv em memoria...', flush=True)
        try:
            import pandas as pd
            header = pd.read_csv(CSV, nrows=0).columns.tolist()
            usecols = [c for c in RAW_COLS if c in header]
            df = pd.read_csv(CSV, usecols=usecols, low_memory=False, dtype=str)
            df['data_envio'] = pd.to_datetime(df['data_envio'], errors='coerce')
            df['_dia'] = df['data_envio'].dt.strftime('%Y-%m-%d')
            if 'bu' in df.columns:
                df['bu'] = df['bu'].map(BU_MAP).fillna('Outros')
            if 'canal' in df.columns:
                df = df[df['canal'] != 'Grupo Controle'].copy()
            print(f'[raw] {len(df):,} linhas, colunas: {usecols}', flush=True)
            _raw_df = df
        except Exception as e:
            print(f'[raw] FAIL ao carregar: {e}', flush=True)
        return _raw_df


def _load_data():
    global _data_cache
    if JSON.exists():
        _data_cache = json.loads(JSON.read_text(encoding='utf-8'))
    else:
        _data_cache = None
    return _data_cache


def needs_refresh():
    if not JSON.exists():
        return True
    if not CSV.exists():
        return False
    return CSV.stat().st_mtime > JSON.stat().st_mtime


def regenerate():
    global _last_refresh, _data_cache
    with _lock:
        print(f'[refresh] regenerating data.json from {CSV.name}...', flush=True)
        r = subprocess.run(
            [sys.executable, str(SCRIPT)],
            cwd=str(BASE), capture_output=True, text=True,
        )
        _last_refresh = datetime.now()
        if r.returncode == 0:
            print(f'[refresh] ok ({_last_refresh.strftime("%H:%M:%S")})', flush=True)
            _load_data()
        else:
            print(f'[refresh] FAIL: {r.stderr[:300]}', flush=True)
        return r.returncode == 0, r.stdout, r.stderr


def status_payload():
    csv_mtime  = CSV.stat().st_mtime  if CSV.exists()  else None
    json_mtime = JSON.stat().st_mtime if JSON.exists() else None
    csv_size   = CSV.stat().st_size   if CSV.exists()  else 0
    return {
        'csv_exists':    CSV.exists(),
        'json_exists':   JSON.exists(),
        'csv_size_mb':   round(csv_size / 1024 / 1024, 1),
        'csv_modified':  datetime.fromtimestamp(csv_mtime).strftime('%d/%m/%Y %H:%M:%S')  if csv_mtime  else None,
        'data_generated': datetime.fromtimestamp(json_mtime).strftime('%d/%m/%Y %H:%M:%S') if json_mtime else None,
        'needs_refresh': needs_refresh(),
    }


class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        if args and ('/data-status' in str(args[0]) or '/aggregate' in str(args[0]) or '/raw-data' in str(args[0])):
            return
        super().log_message(fmt, *args)

    def _json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path

        if path == '/refresh':
            ok, out, err = regenerate()
            return self._json(200 if ok else 500, {
                'ok': ok, 'stdout': out, 'stderr': err, **status_payload(),
            })

        if path == '/data-status':
            return self._json(200, status_payload())

        if path in ('/data.json', '/data.json/'):
            if needs_refresh():
                regenerate()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            body = JSON.read_bytes()
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == '/aggregate':
            return self._handle_aggregate(parsed)

        if path == '/raw-data':
            return self._handle_raw_data(parsed)

        return super().do_GET()

    def _handle_raw_data(self, parsed):
        import pandas as pd

        df = _load_raw()
        if df is None:
            return self._json(503, {'error': 'dados.csv nao disponivel'})

        qs = parse_qs(parsed.query, keep_blank_values=True)
        def qget(k, d=''):
            return (qs.get(k) or [d])[0]

        mask = pd.Series(True, index=df.index)
        data_ini  = qget('dataIni')
        data_fim  = qget('dataFim')
        bu        = qget('bu')
        canal     = qget('canal')
        objetivo  = qget('objetivo')
        metrica   = qget('metrica')
        flag      = qget('flag')
        status    = qget('status')
        campanha  = qget('campanha')
        jornada   = qget('jornada').strip().lower()
        atividade = qget('atividade').strip().lower()

        if data_ini:
            mask &= df['_dia'] >= data_ini
        if data_fim:
            mask &= df['_dia'] <= data_fim
        if bu and 'bu' in df.columns:
            mask &= df['bu'] == bu
        if canal and 'canal' in df.columns:
            mask &= df['canal'] == canal
        if objetivo and 'objetivo' in df.columns:
            mask &= df['objetivo'] == objetivo
        if metrica and 'metrica' in df.columns:
            mask &= df['metrica'] == metrica
        if flag and 'flag_pontual' in df.columns:
            mask &= df['flag_pontual'] == flag
        if status and 'status' in df.columns:
            mask &= df['status'] == status
        if campanha and 'campanha' in df.columns:
            mask &= df['campanha'] == campanha
        if jornada and 'nome_jornada' in df.columns:
            mask &= df['nome_jornada'].str.lower().str.contains(jornada, na=False, regex=False)
        if atividade and 'nome_atividade' in df.columns:
            mask &= df['nome_atividade'].str.lower().str.contains(atividade, na=False, regex=False)

        filtered = df[mask]
        total = len(filtered)

        display_cols = [c for c in RAW_COLS if c in filtered.columns]

        if qget('export') == '1':
            out = filtered[display_cols].copy()
            out['data_envio'] = out['data_envio'].astype(str)
            csv_bytes = ('﻿' + out.fillna('').to_csv(index=False)).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename="dados_clm.csv"')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Length', str(len(csv_bytes)))
            self.end_headers()
            self.wfile.write(csv_bytes)
            return

        page      = max(0, int(qget('page', '0')))
        page_size = min(max(1, int(qget('pageSize', '500'))), 1000)
        start     = page * page_size
        page_df   = filtered[display_cols].iloc[start:start + page_size].copy()
        page_df['data_envio'] = page_df['data_envio'].astype(str)
        records = page_df.fillna('').to_dict('records')

        return self._json(200, {'total': total, 'page': page,
                                'page_size': page_size, 'records': records})

    def _handle_aggregate(self, parsed):
        from aggregate import compute_aggregate

        global _data_cache
        if _data_cache is None:
            if needs_refresh():
                regenerate()
            else:
                _load_data()
        if _data_cache is None:
            return self._json(503, {'error': 'data.json nao disponivel'})

        qs = parse_qs(parsed.query, keep_blank_values=True)

        def qget(key, default=''):
            vals = qs.get(key, [default])
            return vals[0] if vals else default

        def qlist(key):
            raw = qget(key, '')
            return [v for v in raw.split(',') if v] if raw else []

        filters = {
            'dataIni':   qget('dataIni'),
            'dataFim':   qget('dataFim'),
            'bu':        qget('bu'),
            'canal':     qget('canal'),
            'objetivo':  qget('objetivo'),
            'metrica':   qget('metrica'),
            'flag':      qget('flag'),
            'jornadas':  qlist('jornadas'),
            'atividades': qlist('atividades'),
        }

        try:
            result = compute_aggregate(_data_cache, filters)
            return self._json(200, result)
        except Exception as exc:
            import traceback
            return self._json(500, {'error': str(exc), 'trace': traceback.format_exc()})


class ReusableServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads      = True


if __name__ == '__main__':
    os.chdir(str(BASE))
    print(f'Dashboard server -> http://localhost:{PORT}')
    print(f'  CSV:  {CSV} ({CSV.stat().st_size / 1024 / 1024:.1f} MB)' if CSV.exists() else '  CSV:  (nao encontrado)')
    print(f'  JSON: {JSON}' if JSON.exists() else '  JSON: (sera gerado sob demanda)')
    if needs_refresh():
        print('[startup] data.json desatualizado, regenerando...')
        regenerate()
    else:
        _load_data()
    with ReusableServer(('0.0.0.0', PORT), Handler) as srv:
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            print('\nServidor encerrado.')
