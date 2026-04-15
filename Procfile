web: gunicorn main:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120
# WHAL-116: cron process — Railway Cron Service runs this daily at 17:05 ET (21:05 UTC)
# Configure in Railway dashboard: New Service → Cron → command: python cron.py → schedule: 5 21 * * 1-5
cron: python cron.py
