from rich.console import Console
from rich.theme import Theme

# Define custom theme for different message types
custom_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "bold green",
        "debug": "dim cyan",
        "progress": "magenta",
    }
)

# Global console instance
_console = Console(
    theme=custom_theme,
)


def get_console() -> Console:
    """
    Get the global console instance.

    Returns:
        Console instance configured with custom theme
    """

    return _console


# Convenience functions for different log levels
def print_info(message: str, **kwargs) -> None:
    """Print info message with cyan styling."""
    get_console().print(message, style="info", **kwargs)


def print_warning(message: str, **kwargs) -> None:
    """Print warning message with yellow styling."""
    get_console().print(message, style="warning", **kwargs)


def print_error(message: str, **kwargs) -> None:
    """Print error message with bold red styling."""
    get_console().print(message, style="error", **kwargs)


def print_success(message: str, **kwargs) -> None:
    """Print success message with bold green styling."""
    get_console().print(message, style="success", **kwargs)


def print_debug(message: str, **kwargs) -> None:
    """Print debug message with dim cyan styling."""
    get_console().print(message, style="debug", **kwargs)


def print_progress(message: str, **kwargs) -> None:
    """Print progress message with magenta styling."""
    get_console().print(message, style="progress", **kwargs)
