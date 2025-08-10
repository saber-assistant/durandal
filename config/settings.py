import os

APP_NAME = "orchestrator"
APP_VERSION = "0.1.0"

DEBUG = os.getenv("SABER_DEBUG", "false").lower() in ("true", "1", "yes")

# API settings
API_HOST = os.getenv("SABER_API_HOST", "0.0.0.0")
API_PORT = os.getenv("SABER_API_PORT", 8005)
API_KEY = os.getenv("SABER_API_KEY")  # required

# Database settings
DATABASE_URL = os.getenv("SABER_DATABASE_URL", "sqlite:///./intent_classifier.db")

# Queue settings
QUEUE_TYPE = os.getenv("SABER_QUEUE_TYPE", "memory")  # Options: memory, redis
QUEUE_SETTINGS = {
    "default_ttl": 3600
}

# Logging settings
LOG_LEVEL = os.getenv("SABER_LOG_LEVEL", "INFO")
LOG_FORMAT = os.getenv(
    "SABER_LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Middleware settings
ALLOWED_HOSTS = os.getenv("SABER_ALLOWED_HOSTS", "*").split(",")
