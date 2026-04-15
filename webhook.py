from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, jsonify, request
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger('IceXWebhook')

app = Flask(__name__)

MONGO_URI = os.getenv('MONGO_URI', '').strip()
MONGO_DB = os.getenv('MONGO_DB', 'icex_db').strip() or 'icex_db'
MP_ACCESS_TOKEN = os.getenv('MP_ACCESS_TOKEN', '').strip()
MP_WEBHOOK_SECRET = os.getenv('MP_WEBHOOK_SECRET', '').strip()
MP_API_BASE = 'https://api.mercadopago.com'

mongo_client: MongoClient | None = None

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def env_ok() -> bool:
    return bool(MONGO_URI and MP_ACCESS_TOKEN)


def get_db():
    global mongo_client
    if not MONGO_URI:
        raise RuntimeError('MONGO_URI não definido')
    if mongo_client is None:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000, uuidRepresentation='standard')
    return mongo_client[MONGO_DB]


def payments_collection() -> Collection[dict[str, Any]]:
    return get_db()['payments']


def ensure_indexes() -> None:
    col = payments_collection()
    col.create_index([('payment_id', ASCENDING)], unique=True)
    col.create_index([('status', ASCENDING), ('created_at', DESCENDING)])
    col.create_index([('user_id', ASCENDING), ('status', ASCENDING)])
    col.create_index([('approved', ASCENDING), ('processed', ASCENDING)])


def get_payment_id_from_request() -> str | None:
    payload = request.get_json(silent=True) or {}

    candidate = (
        request.args.get('data.id')
        or request.args.get('id')
        or request.args.get('payment_id')
        or request.args.get('data_id')
        or (payload.get('data') or {}).get('id')
        or payload.get('id')
        or payload.get('payment_id')
    )

    if candidate:
        return str(candidate)

    topic = request.args.get('topic') or payload.get('type')
    if topic == 'payment':
        return str((payload.get('data') or {}).get('id') or '') or None

    return None


def fetch_payment(payment_id: str) -> dict[str, Any]:
    if not MP_ACCESS_TOKEN:
        raise RuntimeError('MP_ACCESS_TOKEN não definido')

    url = f'{MP_API_BASE}/v1/payments/{payment_id}'
    headers = {
        'Authorization': f'Bearer {MP_ACCESS_TOKEN}',
        'Accept': 'application/json',
    }
    response = requests.get(url, headers=headers, timeout=25)
    response.raise_for_status()
    return response.json()


def store_payment(payment_id: str, data: dict[str, Any]) -> None:
    col = payments_collection()

    external_reference = data.get('external_reference')
    payer = data.get('payer') or {}
    payer_email = payer.get('email')

    approved = data.get('status') == 'approved'
    processed = bool(data.get('processed', False))

    doc = {
        'payment_id': str(payment_id),
        'user_id': str(external_reference) if external_reference is not None else None,
        'payer_email': payer_email,
        'status': data.get('status', 'unknown'),
        'approved': approved,
        'processed': processed,
        'payload': data,
        'updated_at': utcnow(),
    }

    col.update_one(
        {'payment_id': str(payment_id)},
        {
            '$set': doc,
            '$setOnInsert': {'created_at': utcnow()},
        },
        upsert=True,
    )


def mark_processed(payment_id: str) -> None:
    payments_collection().update_one(
        {'payment_id': str(payment_id)},
        {'$set': {'processed': True, 'processed_at': utcnow(), 'updated_at': utcnow()}},
    )


@app.before_request
def boot_once() -> None:
    if request.endpoint == 'health':
        return
    try:
        ensure_indexes()
    except Exception as exc:
        logger.warning('Falha ao criar índices: %s', exc)


@app.get('/health')
def health():
    return jsonify({'ok': True, 'service': 'icex-webhook', 'ready': env_ok()})


@app.post('/webhook')
def webhook():
    if not MP_ACCESS_TOKEN:
        return jsonify({'ok': False, 'error': 'MP_ACCESS_TOKEN ausente'}), 500
    if not MONGO_URI:
        return jsonify({'ok': False, 'error': 'MONGO_URI ausente'}), 500

    payment_id = get_payment_id_from_request()
    if not payment_id:
        return jsonify({'ok': False, 'error': 'payment_id ausente'}), 400

    try:
        payment = fetch_payment(payment_id)
        store_payment(payment_id, payment)
    except requests.HTTPError as exc:
        logger.exception('Mercado Pago retornou erro ao consultar %s', payment_id)
        return jsonify({'ok': False, 'error': f'Mercado Pago HTTP error: {exc.response.status_code}'}), 502
    except Exception as exc:
        logger.exception('Falha ao processar pagamento %s', payment_id)
        return jsonify({'ok': False, 'error': str(exc)}), 500

    status = payment.get('status', 'unknown')
    response = {
        'ok': True,
        'payment_id': payment_id,
        'status': status,
        'approved': status == 'approved',
    }

    if status == 'approved':
        response['message'] = 'Pagamento aprovado e salvo no MongoDB.'
    elif status in {'pending', 'in_process'}:
        response['message'] = 'Pagamento registrado como pendente.'
    else:
        response['message'] = f'Pagamento registrado com status {status}.'

    return jsonify(response)


@app.post('/webhook/test')
def webhook_test():
    payload = request.get_json(silent=True) or {}
    payment_id = str(payload.get('payment_id') or '').strip()
    if not payment_id:
        return jsonify({'ok': False, 'error': 'payment_id ausente'}), 400
    return webhook()


if __name__ == '__main__':
    port = int(os.getenv('PORT', '10000'))
    app.run(host='0.0.0.0', port=port)
