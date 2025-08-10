import importlib
import os
from pathlib import Path
from types import SimpleNamespace
from dotenv import load_dotenv

ENVIRONMENT = (
    os.getenv("SABER_ENVIRONMENT")
    or os.getenv("ENVIRONMENT")
    or os.getenv("ENV")
    or "development"
).lower()

BASE_DIR = Path(__file__).resolve().parent.parent

for env_file in (BASE_DIR / "envs" / f"{ENVIRONMENT}.env", BASE_DIR / ".env"):
    if env_file.exists():
        load_dotenv(env_file, override=False)


def load_module(path: str):
    module = importlib.import_module(path)

    return {
        k: getattr(module, k)
        for k in dir(module)
        if k.isupper() and not k.startswith("_")
    }

def load_class(path: str, factory: dict):
    mod_path, _, attr = path.rpartition(".")
    module = importlib.import_module(mod_path)
    clazz = getattr(module, attr)
    return clazz(**factory)


conf = SimpleNamespace(**load_module("config.settings"))

__all__ = ["conf"]
