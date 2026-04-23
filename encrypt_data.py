"""Encripta data.json com senha → data.enc.json para publicar em repo publico.

Uso:
    python encrypt_data.py SENHA              # encripta data.json -> data.enc.json
    python encrypt_data.py SENHA arquivo.json # encripta arquivo customizado

Esquema:
    PBKDF2-SHA256 (250k iter) deriva chave de 256 bits a partir da senha + salt.
    AES-256-GCM encripta payload, autentica com tag de 128 bits.
    Todos os parametros vao no JSON de saida (compativel com WebCrypto API).

Sem a senha, o conteudo de data.enc.json e indecifravel — pode ir pra repo publico.
"""
import base64
import hashlib
import json
import os
import sys
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


PBKDF2_ITER = 250_000
KEY_LEN = 32  # 256 bits
SALT_LEN = 16
NONCE_LEN = 12


def derive_key(password: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, PBKDF2_ITER, dklen=KEY_LEN)


def encrypt(plaintext: bytes, password: str) -> dict:
    salt = os.urandom(SALT_LEN)
    nonce = os.urandom(NONCE_LEN)
    key = derive_key(password, salt)
    cipher = AESGCM(key)
    ct = cipher.encrypt(nonce, plaintext, associated_data=None)
    return {
        'v': 1,
        'kdf': {'name': 'PBKDF2', 'hash': 'SHA-256', 'iter': PBKDF2_ITER, 'salt': base64.b64encode(salt).decode()},
        'cipher': {'name': 'AES-GCM', 'nonce': base64.b64encode(nonce).decode()},
        'ct': base64.b64encode(ct).decode(),
    }


def main():
    if len(sys.argv) < 2:
        print('uso: python encrypt_data.py SENHA [arquivo_in.json]')
        sys.exit(1)
    password = sys.argv[1]
    src = sys.argv[2] if len(sys.argv) > 2 else 'data.json'
    out = src.replace('.json', '.enc.json') if src.endswith('.json') else src + '.enc'

    with open(src, 'rb') as f:
        plaintext = f.read()

    enc = encrypt(plaintext, password)
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(enc, f)

    print(f'OK: {src} ({len(plaintext):,} bytes) -> {out} ({len(json.dumps(enc)):,} bytes)')
    print(f'Senha PBKDF2 iters: {PBKDF2_ITER:,}  | algo: AES-256-GCM')


if __name__ == '__main__':
    main()
