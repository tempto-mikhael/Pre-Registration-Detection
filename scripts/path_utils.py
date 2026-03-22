from pathlib import Path
import sys


def resolve_existing_path(
    explicit_path: str | None,
    default_path: Path,
    description: str,
    fallbacks: list[Path] | None = None,
    required: bool = True,
) -> Path:
    """Resolve an existing input path, honoring explicit input first.

    If `explicit_path` is provided, it must exist. Otherwise the default path is
    tried first, then any fallback paths in order.
    """
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            sys.exit(f"ERROR: {description} not found: {path}")
        return path

    candidates = [default_path] + list(fallbacks or [])
    for idx, candidate in enumerate(candidates):
        if candidate.exists():
            if idx > 0:
                print(
                    f"WARNING: default {description} not found at {default_path.name}; "
                    f"using {candidate.name} instead"
                )
            return candidate

    if required:
        sys.exit(
            f"ERROR: {description} not found. Tried: "
            + ", ".join(str(candidate) for candidate in candidates)
        )

    print(
        f"WARNING: {description} not found. Tried: "
        + ", ".join(str(candidate) for candidate in candidates)
    )
    return default_path


def resolve_output_path(explicit_path: str | None, default_path: Path) -> Path:
    """Resolve an output path, defaulting to the provided path when omitted."""
    return Path(explicit_path) if explicit_path else default_path
