"""Project settings. There is no need to edit this file unless you want to change values
from the Kedro defaults. For further information, including these default values, see
https://docs.kedro.org/en/stable/kedro_project_setup/settings.html."""

import polars as pl
from pathlib import Path

# Instantiated project hooks.
# Hooks are executed in a Last-In-First-Out (LIFO) order.
from if_recomender.hooks import DataValidationHook

HOOKS = (DataValidationHook(),)

# Installed plugins for which to disable hook auto-registration.
# DISABLE_HOOKS_FOR_PLUGINS = ("kedro-viz",)

# Class that manages storing KedroSession data.
# from kedro.framework.session.store import BaseSessionStore
# SESSION_STORE_CLASS = BaseSessionStore
# Keyword arguments to pass to the `SESSION_STORE_CLASS` constructor.
# SESSION_STORE_ARGS = {
#     "path": "./sessions"
# }

# Directory that holds configuration.
# CONF_SOURCE = "conf"

# Class that manages how configuration is loaded.
# from kedro.config import OmegaConfigLoader

# CONFIG_LOADER_CLASS = OmegaConfigLoader


def get_max_period():
    """Compute max_period from CVM monthly data files."""
    cvm_path = Path("data/01_raw/cvm/data/pl")
    months = [int(f.stem) for f in cvm_path.glob("*.csv") if f.stem.isdigit()]
    return max(months)


# Keyword arguments to pass to the `CONFIG_LOADER_CLASS` constructor.
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "custom_resolvers": {
        "polars": lambda x: getattr(pl, x),
        "max_period": get_max_period,
    },
}

# Class that manages Kedro's library components.
# from kedro.framework.context import KedroContext
# CONTEXT_CLASS = KedroContext

# Class that manages the Data Catalog.
# from kedro.io import DataCatalog
# DATA_CATALOG_CLASS = DataCatalog
