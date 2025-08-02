import logging
import sys
from pathlib import Path

# Be able to import modules from src/
SRC_PATH = Path(__file__).resolve().parents[1]
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

# Make logging visible during cell execution
handler = logging.StreamHandler()
handler.flush = sys.stdout.flush  # type: ignore
logging.basicConfig(level=logging.INFO, handlers=[handler])
