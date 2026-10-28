web: gunicorn -b 0.0.0.0:${PORT:-10000} --workers 1 --timeout 600 --graceful-timeout 30 app:app
