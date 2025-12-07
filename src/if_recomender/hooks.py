import logging
from pathlib import Path
from typing import Any, Callable, ClassVar

import polars as pl
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline

from if_recomender.validation.raw.data_validator import RawDataValidator
from if_recomender.validation.models import (
    DataValidationConfig,
    DatasetValidationConfig,
    OutputValidationResult,
    ValidationResult,
    ValidationStatus,
    ValidationStrategy,
)
from if_recomender.validation.dataframe import (
    validate_allowed_values,
    validate_time_completeness,
    validate_uniqueness,
    validate_bounds,
)

logger = logging.getLogger(__name__)


class DataValidationHook:
    """Kedro hook for pre-parse and output data validation.

    Runs configured validations before pipeline (raw data) and after
    dataset save (output checks like uniqueness, bounds).
    """

    _validation_registry: ClassVar[dict[str, Callable]] = {
        "validate_time_completeness": validate_time_completeness,
        "validate_uniqueness": validate_uniqueness,
        "validate_bounds": validate_bounds,
        "validate_allowed_values": validate_allowed_values,
    }

    _OUTPUT_REPORT_COLUMNS: ClassVar[list[str]] = [
        "timestamp",
        "dataset_name",
        "validation_name",
        "status",
        "error_count",
        "details",
        "affected_groups_count",
        "affected_groups",
    ]

    def __init__(self):
        self._results: list[ValidationResult] = []
        self._output_results: list[OutputValidationResult] = []
        self._output_config: dict[str, dict] = {}
        self._input_config: dict[str, dict] = {}
        self._dataframe_report_include_passed: bool = False

    @hook_impl
    def before_pipeline_run(
        self,
        run_params: dict[str, Any],
        pipeline: Pipeline,
        catalog: DataCatalog,
    ) -> None:
        """Run pre-parse validations on raw datasets."""
        self._output_config = self._load_params(catalog, "output_validations", {})
        self._input_config = self._load_params(catalog, "input_validations", {})
        df_validation_config = self._load_params(catalog, "dataframe_validation", {})
        self._dataframe_report_include_passed = df_validation_config.get(
            "report_include_passed", False
        )

        # Run pre-parse validations if pipeline uses raw datasets
        # This includes: intermediate pipeline, or full/default pipeline (None)
        pipeline_name = run_params.get("pipeline_name")
        if pipeline_name not in (None, "intermediate", "__default__"):
            return

        try:
            config = self._load_validation_config(catalog)
        except Exception as e:
            logger.warning(f"Could not load validation config: {e}")
            return

        if not config.enabled:
            logger.info("Data validation is disabled")
            return

        self._results = []
        self._output_results = []

        logger.info("Starting pre-parse validation and fixes...")

        for dataset_name, dataset_config in config.datasets.items():
            try:
                self._validate_dataset(catalog, dataset_name, dataset_config)
            except Exception as e:
                logger.error(f"Error validating dataset {dataset_name}: {e}")
                raise

        if self._results:
            report_df = RawDataValidator.generate_report(
                self._results,
                include_passed=config.report_include_passed,
            )
            catalog.save("rpt_raw_data_validation_report", report_df)
            RawDataValidator.print_summary(self._results)

    @hook_impl
    def after_dataset_loaded(
        self,
        dataset_name: str,
        data: Any,
        node: Any,
    ) -> None:
        """Run input validations on loaded dataset or partitions."""
        if dataset_name not in self._input_config:
            return

        validations = self._input_config[dataset_name].get("validations", {})
        if not validations:
            return

        if isinstance(data, dict) and data and all(callable(v) for v in data.values()):
            logger.info(
                f"Running input validation on {len(data)} partitions of {dataset_name}..."
            )
            for partition_key, loader in data.items():
                partition_df = loader()
                if isinstance(partition_df, pl.DataFrame):
                    self._run_validations(
                        partition_df, f"{dataset_name}/{partition_key}", validations
                    )
        elif isinstance(data, pl.DataFrame):
            self._run_validations(data, dataset_name, validations)

    def _run_validations(
        self,
        df: pl.DataFrame,
        dataset_name: str,
        validations: dict,
    ) -> None:
        """Run validations on a DataFrame and log results."""
        for validation_name, params in validations.items():
            result = self._run_validation(df, dataset_name, validation_name, params)
            if result:
                self._output_results.append(result)
                if result.passed:
                    logger.info(
                        f"  ✓ {dataset_name}.{validation_name}: passed ({result.details})"
                    )
                else:
                    logger.warning(
                        f"  ✗ {dataset_name}.{validation_name}: FAILED - "
                        f"{result.error_count} issues ({result.details})"
                    )

    @hook_impl
    def after_dataset_saved(
        self,
        dataset_name: str,
        data: Any,
        node: Any,
    ) -> None:
        """Run output validations after a dataset is saved."""
        if dataset_name not in self._output_config:
            return

        validations = self._output_config[dataset_name].get("validations", {})
        logger.info(
            f"Running {len(validations)} output validation(s) for {dataset_name}..."
        )
        self._run_validations(data, dataset_name, validations)

    @hook_impl
    def after_pipeline_run(
        self,
        run_params: dict[str, Any],
        pipeline: Pipeline,
        catalog: DataCatalog,
    ) -> None:
        """Save output validation report after pipeline completes."""
        if not self._output_results:
            empty_df = pl.DataFrame({col: [] for col in self._OUTPUT_REPORT_COLUMNS})
            catalog.save("rpt_dataframe_validation_report", empty_df)

        results_to_report = self._output_results
        if not self._dataframe_report_include_passed:
            results_to_report = [r for r in self._output_results if not r.passed]

        rows = [
            {
                "timestamp": r.timestamp.isoformat(),
                "dataset_name": r.dataset_name,
                "validation_name": r.validation_name,
                "status": "passed" if r.passed else "failed",
                "error_count": r.error_count,
                "details": r.details or "",
                "affected_groups_count": len(r.affected_groups),
                "affected_groups": str(r.affected_groups),
            }
            for r in results_to_report
        ]

        report_df = pl.DataFrame(rows)
        catalog.save("rpt_dataframe_validation_report", report_df)

        passed = sum(1 for r in self._output_results if r.passed)
        failed = len(self._output_results) - passed
        logger.info(f"Output validation summary: {passed} passed, {failed} failed")

    def _run_validation(
        self,
        df: pl.DataFrame,
        dataset_name: str,
        validation_name: str,
        params: dict,
    ) -> OutputValidationResult | None:
        """Run a single output validation using the registry."""
        validation_func = self._validation_registry.get(validation_name, None)
        if validation_func is None:
            logger.warning(f"Unknown output validation: {validation_name}")
            return None

        return validation_func(df=df, dataset_name=dataset_name, **params)

    def _load_params(self, catalog: DataCatalog, key: str, default: Any) -> Any:
        """Load parameters from catalog with fallback."""
        try:
            params = catalog.load(f"params:{key}")
            return params if params else default
        except Exception:
            try:
                all_params = catalog.load("parameters")
                return all_params.get(key, default)
            except Exception:
                return default

    def _load_validation_config(self, catalog: DataCatalog) -> DataValidationConfig:
        """Load validation configuration from parameters."""
        params = self._load_params(catalog, "data_validation", None)

        if not params:
            logger.warning("No data_validation config found in parameters")
            return DataValidationConfig(enabled=False)

        datasets_config = {}
        for dataset_name, ds_config in params.get("datasets", {}).items():
            validations = {
                val_name: ValidationStrategy(strategy)
                for val_name, strategy in ds_config.get("validations", {}).items()
            }
            datasets_config[dataset_name] = DatasetValidationConfig(
                validations=validations
            )

        return DataValidationConfig(
            enabled=params.get("enabled", True),
            report_include_passed=params.get("report_include_passed", False),
            datasets=datasets_config,
        )

    def _validate_dataset(
        self,
        catalog: DataCatalog,
        dataset_name: str,
        config: DatasetValidationConfig,
    ) -> None:
        """Validate all files in a dataset."""
        if not config.validations:
            logger.debug(f"No validations configured for {dataset_name}")
            return

        ds_config = catalog.config_resolver.config.get(dataset_name)
        if ds_config is None:
            logger.warning(f"Dataset {dataset_name} not found in catalog config")
            return

        file_paths: list[Path] = []
        if "path" in ds_config:
            base_path = Path(ds_config["path"])
            if base_path.is_dir():
                file_paths = list(base_path.glob("*.csv"))
            elif base_path.exists():
                file_paths = [base_path]
        elif "filepath" in ds_config:
            filepath = Path(ds_config["filepath"])
            if filepath.exists():
                file_paths = [filepath]

        if not file_paths:
            logger.warning(f"No files found for dataset {dataset_name}")
            return

        logger.info(f"Validating {len(file_paths)} files in {dataset_name}")

        validator = RawDataValidator()
        dataset_start_idx = len(self._results)

        for idx, file_path in enumerate(file_paths, 1):
            logger.info(f"  [{idx}/{len(file_paths)}] Validating {file_path.name}...")
            results = validator.validate_and_fix(
                file_path,
                config.validations,
                dataset_name=dataset_name,
            )
            self._results.extend(results)

            for result in results:
                if result.status == ValidationStatus.FAILED:
                    logger.warning(
                        f"    ✗ {result.validation_name}: {result.status.value} "
                        f"(strategy: {result.strategy_applied.value})"
                    )
                elif result.status == ValidationStatus.FIXED:
                    logger.info(
                        f"    ✓ {result.validation_name}: {result.fixes_applied} fixes applied"
                    )

        dataset_results = self._results[dataset_start_idx:]
        if dataset_results:
            total_fixed = sum(
                1 for r in dataset_results if r.status == ValidationStatus.FIXED
            )
            total_failed = sum(
                1 for r in dataset_results if r.status == ValidationStatus.FAILED
            )
            total_passed = sum(
                1 for r in dataset_results if r.status == ValidationStatus.PASSED
            )
            logger.info(
                f"Dataset {dataset_name} summary: "
                f"{total_passed} passed, {total_fixed} fixed, {total_failed} failed"
            )
