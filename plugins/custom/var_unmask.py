import re

UNMASK_PATTERNS = (
    "wandb_api_key",
    "mount_secrets",
    "secret.json",
    "run_token",
)

_EXEMPT = tuple(re.sub(r"\W+", "_", pattern.lower()) for pattern in UNMASK_PATTERNS)


def _is_exempt(name) -> bool:
    try:
        if not isinstance(name, str):
            return False
        normalized = re.sub(r"\W+", "_", name.strip().lower())
        return any(pattern in normalized for pattern in _EXEMPT)
    except Exception:
        return False


def _patch_masker(masker) -> None:
    original = masker.should_hide_value_for_key

    def should_hide_value_for_key(name, _original=original, _is_exempt=_is_exempt):
        if _is_exempt(name):
            return False
        return _original(name)

    masker.should_hide_value_for_key = should_hide_value_for_key


def _install() -> None:
    from airflow._shared.secrets_masker import _secrets_masker

    _patch_masker(_secrets_masker())

    try:
        from airflow.sdk._shared.secrets_masker import _secrets_masker as _sdk_secrets_masker

        _patch_masker(_sdk_secrets_masker())
    except ImportError:
        pass


_install()
