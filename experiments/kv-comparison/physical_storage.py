"""Provider-specific model identification; caller must also verify device ownership."""

import re


def admitted_model(provider, model):
    if not isinstance(model, str):
        return False
    model = model.strip().lower()
    if provider == "aws":
        return "instance storage" in model
    if provider == "gcp":
        # Each GCE local SSD is its own controller: nvme_card0, nvme_card1, ...
        # The persistent boot disk reports nvme_card-pd and stays excluded.
        return re.fullmatch(r"nvme_card[0-9]+", model) is not None or "local ssd" in model or "titanium" in model
    return False
