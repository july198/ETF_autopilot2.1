import json
import sys
from pathlib import Path

# Allow running without installing the package
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from etf_auto_trader.config import load_config
from etf_auto_trader.runner import run_daily

if __name__ == "__main__":
    cfg = load_config("config.yaml")
    summary = run_daily(cfg)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
