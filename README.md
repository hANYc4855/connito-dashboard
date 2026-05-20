# connito-api

Real-time dashboard that monitors the Connito subnet cycle API.

## Endpoints proxied

| Path | Source |
|---|---|
| `/api/get_phase` | `/get_phase` |
| `/api/blocks_until_next_phase` | `/blocks_until_next_phase` |
| `/api/previous_phase_blocks` | `/previous_phase_blocks` |
| `/api/get_validator_whitelist` | `/get_validator_whitelist` |
| `/api/get_init_peer_id` | `/get_init_peer_id` |
| `/api/all` | All five endpoints in parallel |

## Quick start

```bash
cd /root/connito-api
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8080
```

Open http://localhost:8080 in your browser.

## Run with PM2

```bash
pm2 start "uvicorn app:app --host 0.0.0.0 --port 8080 --workers 4 --loop uvloop" \
  --name connito-api \
  --cwd /root/connito-api
pm2 save

uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4 --loop uvloop
```

## Dashboard features

- **Current Phase** hero card with phase name, block number, cycle index
- **Phase progress bar** showing blocks into / remaining in the current phase
- **Cycle timeline** listing all 9 phases, their start/end blocks and blocks until each
- **Previous phase blocks** reference grid
- **Validator whitelist** — all 7 whitelisted validator SS58 addresses
- **Init Peer IDs** — parsed multiaddr display
- Auto-refreshes every **12 seconds** (phase + leaderboard)
- **Miner val_loss / score history** — stored in `data/miner_history.json` as `uid` → `repo_id` → `revision` → samples (up to 100 revisions × 100 samples per repo). Old repos are kept on disk; the UI shows only the miner’s current `hf_repo_id`.
