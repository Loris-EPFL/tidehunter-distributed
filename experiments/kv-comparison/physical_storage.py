"""Provider-specific model identification; caller must also verify device ownership."""


def admitted_model(provider, model):
    if not isinstance(model, str):
        return False
    model = model.strip().lower()
    if provider == "aws":
        return "instance storage" in model
    if provider == "gcp":
        return model == "nvme_card0" or "local ssd" in model or "titanium" in model
    return False
