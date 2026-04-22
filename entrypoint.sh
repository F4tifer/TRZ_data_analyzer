#!/bin/sh
set -e

if [ -f /etc/wireguard/wg0.conf ]; then
    echo "Starting WireGuard..."
    wg-quick up wg0
    echo "WireGuard connected."
else
    echo "WARNING: /etc/wireguard/wg0.conf not found, starting without VPN."
fi

exec uvicorn hybrid_app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
