import os

## SEPARATORS

ALL_INTENT_SEPARATORS = {
    "local_model": {
        "path": "intent_classifier.intent_separators.LocalModelIntentSeparator",
        "job_cost": 1,
        "factory": lambda: {
        },
    }
}

# Comma-separated whitelist. Enable *all* if empty
_ENABLED = {a.strip() for a in os.getenv("ENABLED_SEPARATORS", "").split(",") if a.strip()}
_selected_aliases = _ENABLED or ALL_INTENT_SEPARATORS.keys()

# Build ordered list (preserve the declared order in ALL_INTENT_SEPARATORS)
INTENT_SEPARATORS = [
    {
        "alias": alias,
        "path": spec["path"],
        **spec["factory"](),          # factory runs only for enabled aliases
    }
    for alias, spec in ALL_INTENT_SEPARATORS.items()
    if alias in _selected_aliases
]



## CLASSIFIERS

DEFAULT_INTENT_CONFIDENCE_THRESHOLD = float(
    os.getenv("SABER_DEFAULT_INTENT_CONFIDENCE_THRESHOLD", 0.80)
)

# Full canonical list of classification layers
# Each layer is a dict with:
# - key/alias: unique identifier for the layer
# - path: import path to the layer class
# - factory: callable that returns a dict of parameters for the layer, doesn't complain about missing env vars for unused layers
ALL_CLASSIFICATION_LAYERS = {
    "local_model": {
        "path": "intent_classifier.intent_layers.LocalModelIntentLayer",
        "factory": lambda: {
            "job_cost": 5,
            "weights_path": "models/local_model.pt",
            "device": os.getenv("LOCAL_MODEL_DEVICE", "cpu"),
            "batch_size": 2,
            "confidence_threshold": 0.75,
        },
    }
}

# Comma-separated whitelist. Enable *all* if empty
_ENABLED = {a.strip() for a in os.getenv("ENABLED_LAYERS", "").split(",") if a.strip()}
_selected_aliases = _ENABLED or ALL_CLASSIFICATION_LAYERS.keys()

# Build ordered list (preserve the declared order in ALL_CLASSIFICATION_LAYERS)
CLASSIFICATION_LAYERS = [
    {
        "alias": alias,
        "path": spec["path"],
        **spec["factory"](),          # factory runs only for enabled aliases
    }
    for alias, spec in ALL_CLASSIFICATION_LAYERS.items()
    if alias in _selected_aliases
]