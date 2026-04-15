# Whale Tracker Backend — Claude Code Instructions

## 📁 Project Info
- **Local path:** `~/Downloads/whale-tracker-scraper`
- **Deployed on:** Railway at `https://whale-tracker-scraper-production.up.railway.app`
- **Language:** Python / Flask
- **Deploy command:** `railway up`

## 🔗 Services & Credentials
- **Supabase:** `bedurjtazsfbnkisoeee.supabase.co`
- **Alpaca:** `ALPACA_KEY`, `ALPACA_SECRET` (set in Railway env vars)
- **Anthropic:** `ANTHROPIC_API_KEY` (set in Railway env vars)
- **Redis:** `REDIS_URL` (Railway addon, when added)

## ⚠️ Critical Rules
- **NEVER replace `main.py` entirely** — always ADD new routes/functions only
- Always append new endpoints — do not remove or rewrite existing ones
- Test endpoints locally before deploying: `python main.py`
- After deploy, verify with: `curl https://whale-tracker-scraper-production.up.railway.app/api/health`
- Use `redis-py` for caching with fallback to direct DB if Redis unavailable

## 🛤️ Existing API Endpoints (do not break these)
- `GET /api/filings` — SEC insider filings
- `GET /api/summary` — Dashboard summary (top buys/sells)
- `GET /api/top-stocks` — Top traded stocks
- `GET /api/heatmap` — Sector heatmap data
- `GET /api/chart/:ticker` — Yahoo Finance chart data
- `GET /api/tickers` — List of tracked tickers
- `GET /api/sentiment/:ticker` — Reddit sentiment
- `GET /api/options/flow` — Options flow data
- `GET /api/quote/:ticker` — Alpaca real-time quote
- `GET /api/trades/:ticker` — Alpaca real-time trades
- `GET /api/version` — App version management

## ✅ Jira Integration — Close Tickets After Validation

### When asked to implement and close a Jira ticket:

**Step 1 — Implement the feature** (add new route to `main.py`)

**Step 2 — Validate locally**
```bash
cd ~/Downloads/whale-tracker-scraper
python -m py_compile main.py          # syntax check
python -m pytest tests/ -v            # run tests if present
python main.py &                       # start server
curl http://localhost:5000/api/health  # verify running
kill %1                                # stop server
```

**Step 3 — Deploy to Railway**
```bash
cd ~/Downloads/whale-tracker-scraper
railway up
```

**Step 4 — Verify deployed endpoint works**
```bash
curl https://whale-tracker-scraper-production.up.railway.app/api/NEW_ENDPOINT
```

**Step 5 — Only if all above passes, close the Jira ticket:**

```bash
JIRA_EMAIL="batchuusa@gmail.com"
JIRA_TOKEN="${JIRA_API_TOKEN}"   # export JIRA_API_TOKEN=your_token_here
CLOUD_ID="5b02f221-ab5d-4d82-9d37-763ed0b488fd"
TICKET="WHAL-XX"   # replace with actual ticket

AUTH=$(echo -n "$JIRA_EMAIL:$JIRA_TOKEN" | base64)

# 1. Add comment
curl -s -X POST \
  "https://api.atlassian.com/ex/jira/$CLOUD_ID/rest/api/3/issue/$TICKET/comment" \
  -H "Authorization: Basic $AUTH" \
  -H "Content-Type: application/json" \
  -d "{\"body\": {\"type\": \"doc\", \"version\": 1, \"content\": [{\"type\": \"paragraph\", \"content\": [{\"type\": \"text\", \"text\": \"Resolved by Claude (Anthropic AI Agent) — [describe what was built and deployed]\"}]}]}}"

# 2. Transition to Done
curl -s -X POST \
  "https://api.atlassian.com/ex/jira/$CLOUD_ID/rest/api/3/issue/$TICKET/transitions" \
  -H "Authorization: Basic $AUTH" \
  -H "Content-Type: application/json" \
  -d '{"transition": {"id": "31"}}'

echo "✅ $TICKET closed"
```

### Environment Variable Setup
Add to your `~/.zshrc` or `~/.bashrc`:
```bash
export JIRA_API_TOKEN="your_atlassian_api_token_here"
```
Get your token at: https://id.atlassian.com/manage-profile/security/api-tokens

### Jira Reference
- **Project:** WHAL
- **Cloud ID:** `5b02f221-ab5d-4d82-9d37-763ed0b488fd`
- **Done transition ID:** `31`
- **Jira URL:** `batchuusa.atlassian.net`
- **Comment format:** `"Resolved by Claude (Anthropic AI Agent) — [what was built and deployed]"`

### Example Claude Code prompt:
> "Implement WHAL-73 Redis caching for /api/filings and /api/summary endpoints, validate locally, deploy to Railway, verify the endpoints respond, then close the Jira ticket."
