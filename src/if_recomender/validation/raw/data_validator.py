"""Pre-parsing CSV validator for detecting and fixing data quality issues."""

import logging
from datetime import datetime
from pathlib import Path
from typing import ClassVar

import polars as pl
from pydantic import BaseModel

from if_recomender.utils import backup_file, restore_file
from if_recomender.validation.models import (
    FixResult,
    ValidationResult,
    ValidationStatus,
    ValidationStrategy,
)

logger = logging.getLogger(__name__)


class RawDataValidator(BaseModel):
    """CSV validator with configurable fix strategies.

    Example:
        validator = RawDataValidator()
        results = validator.validate_and_fix(
            Path("data.csv"),
            {"check_redundant_quotes": ValidationStrategy.FIX}
        )
    """

    encoding: str = "latin1"

    # Registry maps validation names to method names
    _validation_registry: ClassVar[dict[str, str]] = {
        "check_redundant_quotes": "_check_redundant_quotes",
        "check_malformed_quotes": "_check_malformed_quotes",
    }

    # Registry maps validation names to fix method names
    _fix_registry: ClassVar[dict[str, str]] = {
        "check_redundant_quotes": "_fix_redundant_quotes",
        "check_malformed_quotes": "_fix_malformed_quotes",
    }

    model_config = {"arbitrary_types_allowed": True}

    def validate_and_fix(
        self,
        file_path: Path,
        validations: dict[str, ValidationStrategy],
        dataset_name: str | None = None,
    ) -> list[ValidationResult]:
        """Run validations and apply fixes based on strategy.

        FIX repairs in-place; IGNORE removes affected lines.
        """
        results = []

        for validation_name, strategy in validations.items():
            if validation_name not in self._validation_registry:
                logger.warning(f"Unknown validation '{validation_name}' - skipping")
                continue

            logger.debug(
                f"Running {validation_name} on {file_path.name} "
                f"with strategy '{strategy.value}'"
            )

            check_result = self._run_check(file_path, validation_name, dataset_name)

            if check_result.status == ValidationStatus.PASSED:
                check_result.strategy_applied = strategy
                results.append(check_result)
                continue

            if strategy == ValidationStrategy.FIX:
                if validation_name in self._fix_registry:
                    fix_result = self._run_fix(file_path, validation_name, dataset_name)
                    result = ValidationResult(
                        file_path=str(file_path),
                        validation_name=validation_name,
                        status=ValidationStatus.FIXED
                        if fix_result.lines_fixed > 0
                        else ValidationStatus.PASSED,
                        strategy_applied=strategy,
                        dataset_name=dataset_name,
                        details=fix_result.details,
                        affected_lines=fix_result.lines_removed,
                        fixes_applied=fix_result.lines_fixed,
                    )
                    if fix_result.lines_fixed > 0:
                        logger.info(
                            f"Fixed {fix_result.lines_fixed} issues in "
                            f"{file_path.name} ({validation_name})"
                        )
                else:
                    logger.warning(
                        f"No FIX method for '{validation_name}', falling back to IGNORE"
                    )
                    fix_result = self._run_ignore(
                        file_path, check_result.affected_lines, validation_name
                    )
                    result = ValidationResult(
                        file_path=str(file_path),
                        validation_name=validation_name,
                        status=ValidationStatus.FIXED
                        if fix_result.lines_fixed > 0
                        else ValidationStatus.PASSED,
                        strategy_applied=strategy,
                        dataset_name=dataset_name,
                        details=f"Fallback to IGNORE: {fix_result.details}",
                        affected_lines=fix_result.lines_removed,
                        fixes_applied=fix_result.lines_fixed,
                    )
                    if fix_result.lines_fixed > 0:
                        logger.info(
                            f"Removed {fix_result.lines_fixed} lines (fallback) in "
                            f"{file_path.name} ({validation_name})"
                        )
            else:  # IGNORE
                fix_result = self._run_ignore(
                    file_path, check_result.affected_lines, validation_name
                )
                result = ValidationResult(
                    file_path=str(file_path),
                    validation_name=validation_name,
                    status=ValidationStatus.FIXED
                    if fix_result.lines_fixed > 0
                    else ValidationStatus.PASSED,
                    strategy_applied=strategy,
                    dataset_name=dataset_name,
                    details=fix_result.details,
                    affected_lines=fix_result.lines_removed,
                    fixes_applied=fix_result.lines_fixed,
                )
                if fix_result.lines_fixed > 0:
                    logger.info(
                        f"Removed {fix_result.lines_fixed} lines in "
                        f"{file_path.name} ({validation_name})"
                    )

            results.append(result)

        return results

    def _run_check(
        self,
        file_path: Path,
        validation_name: str,
        dataset_name: str | None = None,
    ) -> ValidationResult:
        """Run a single validation check."""
        method_name = self._validation_registry[validation_name]
        method = getattr(self, method_name)
        return method(file_path, dataset_name)

    def _run_fix(
        self, file_path: Path, validation_name: str, dataset_name: str | None = None
    ) -> FixResult:
        """Run a single fix operation WITH CONDITIONAL BACKUP and integrity validation.

        This method:
        1. Checks if the file actually needs fixing
        2. If no issues found, skips backup and fix (optimization)
        3. If issues found, creates a backup of the original file
        4. Applies the fix
        5. Validates the fix didn't corrupt the data
        6. Restores from backup if corruption is detected

        Args:
            file_path: Path to the file to fix
            validation_name: Name of the validation (maps to fix method)
            dataset_name: Optional dataset name for logging

        Returns:
            FixResult with success status and details
        """
        check_method_name = self._validation_registry.get(validation_name)
        if check_method_name:
            check_method = getattr(self, check_method_name)
            check_result = check_method(file_path, dataset_name)

            if len(check_result.affected_lines) == 0:
                logger.info(
                    f"No issues found for {validation_name} in {file_path.name}, skipping backup and fix"
                )
                return FixResult(
                    file_path=str(file_path),
                    fix_name=f"fix_{validation_name}",
                    lines_fixed=0,
                    lines_removed=[],
                    success=True,
                    details="No issues found, file not modified",
                )

        try:
            backup_path = backup_file(file_path)
            logger.info(
                f"Backed up {file_path.name} to {backup_path} (issues detected)"
            )
        except Exception as e:
            logger.error(f"Failed to create backup for {file_path.name}: {e}")
            return FixResult(
                file_path=str(file_path),
                fix_name=f"fix_{validation_name}",
                lines_fixed=0,
                success=False,
                details=f"Backup failed: {e}",
            )

        method_name = self._fix_registry[validation_name]
        method = getattr(self, method_name)
        result = method(file_path)

        if result.success:
            sanity_ok, sanity_msg = self._validate_fix_sanity(file_path, backup_path)
            if not sanity_ok:
                logger.error(
                    f"Sanity check failed for {file_path.name}! Restoring from backup..."
                )
                restore_file(backup_path, file_path)
                result.success = False
                result.details = f"Fix reverted - {sanity_msg}"

        return result

    def _run_ignore(
        self,
        file_path: Path,
        affected_lines: list[int],
        validation_name: str,
    ) -> FixResult:
        """Run the IGNORE strategy - remove affected lines with backup protection.

        Args:
            file_path: Path to the file to modify
            affected_lines: List of line numbers (1-indexed) to remove
            validation_name: Name of the validation (for logging)

        Returns:
            FixResult with success status and details
        """
        if not affected_lines:
            return FixResult(
                file_path=str(file_path),
                fix_name=f"ignore_{validation_name}",
                lines_fixed=0,
                lines_removed=[],
                success=True,
                details="No lines to remove",
            )

        try:
            backup_path = backup_file(file_path)
            logger.info(
                f"Backed up {file_path.name} to {backup_path} (IGNORE strategy)"
            )
        except Exception as e:
            logger.error(f"Failed to create backup for {file_path.name}: {e}")
            return FixResult(
                file_path=str(file_path),
                fix_name=f"ignore_{validation_name}",
                lines_fixed=0,
                lines_removed=[],
                success=False,
                details=f"Backup failed: {e}",
            )

        result = self._ignore_affected_lines(file_path, affected_lines)
        result.fix_name = f"ignore_{validation_name}"

        if result.success:
            sanity_ok, sanity_msg = self._validate_fix_sanity(file_path, backup_path)
            if not sanity_ok:
                logger.error(
                    f"Sanity check failed for {file_path.name}! Restoring from backup..."
                )
                restore_file(backup_path, file_path)
                result.success = False
                result.details = f"IGNORE reverted - {sanity_msg}"

        return result

    # =========================================================================
    # VALIDATION METHODS
    # =========================================================================

    def _check_redundant_quotes(
        self,
        file_path: Path,
        dataset_name: str | None = None,
    ) -> ValidationResult:
        """Check for fields with an ODD number of unescaped quotes.

        These are "redundant" quotes that cannot be properly paired/escaped.
        They should be REMOVED. Runs FIRST before check_malformed_quotes.

        Logic per field:
        - Remove already-escaped "" pairs
        - Count remaining single quotes
        - If ODD count → flag the line
        """
        affected_lines = []

        try:
            content = file_path.read_text(encoding=self.encoding)
            lines = content.split("\n")

            for line_num, line in enumerate(lines, start=1):
                if line_num == 1:
                    continue
                if not line.strip():
                    continue
                if self._line_has_odd_quotes(line):
                    affected_lines.append(line_num)

        except Exception as e:
            return ValidationResult(
                file_path=str(file_path),
                validation_name="check_redundant_quotes",
                status=ValidationStatus.FAILED,
                strategy_applied=ValidationStrategy.IGNORE,
                dataset_name=dataset_name,
                details=f"Error reading file: {e}",
            )

        if affected_lines:
            return ValidationResult(
                file_path=str(file_path),
                validation_name="check_redundant_quotes",
                status=ValidationStatus.FAILED,
                strategy_applied=ValidationStrategy.IGNORE,
                dataset_name=dataset_name,
                details=f"Found {len(affected_lines)} lines with redundant (odd) quotes",
                affected_lines=affected_lines,
            )

        return ValidationResult(
            file_path=str(file_path),
            validation_name="check_redundant_quotes",
            status=ValidationStatus.PASSED,
            strategy_applied=ValidationStrategy.IGNORE,
            dataset_name=dataset_name,
            details="No redundant quotes detected",
        )

    def _line_has_odd_quotes(self, line: str) -> bool:
        """Check if any field in the line has an ODD number of unescaped quotes.

        Args:
            line: A single CSV line

        Returns:
            True if any field has odd unescaped quote count
        """
        if '"' not in line:
            return False

        fields = line.split(";")
        for field in fields:
            cleaned_field = field.replace('""', "")
            quote_count = cleaned_field.count('"')
            if quote_count % 2 == 1:
                return True

        return False

    def _check_malformed_quotes(
        self,
        file_path: Path,
        dataset_name: str | None = None,
    ) -> ValidationResult:
        """Check for fields with an EVEN number of unescaped quotes (> 0).

        These are "malformed" quotes that need to be DOUBLED for proper CSV escaping.
        Runs SECOND after check_redundant_quotes has removed odd quotes.

        Logic per field:
        - Remove already-escaped "" pairs
        - Count remaining single quotes
        - If EVEN count > 0 → flag the line
        """
        affected_lines = []

        try:
            content = file_path.read_text(encoding=self.encoding)
            lines = content.split("\n")

            for line_num, line in enumerate(lines, start=1):
                if line_num == 1:
                    continue
                if not line.strip():
                    continue
                if self._line_has_even_quotes(line):
                    affected_lines.append(line_num)

        except Exception as e:
            return ValidationResult(
                file_path=str(file_path),
                validation_name="check_malformed_quotes",
                status=ValidationStatus.FAILED,
                strategy_applied=ValidationStrategy.IGNORE,
                dataset_name=dataset_name,
                details=f"Error reading file: {e}",
            )

        if affected_lines:
            return ValidationResult(
                file_path=str(file_path),
                validation_name="check_malformed_quotes",
                status=ValidationStatus.FAILED,
                strategy_applied=ValidationStrategy.IGNORE,
                dataset_name=dataset_name,
                details=f"Found {len(affected_lines)} lines with malformed (even) quotes",
                affected_lines=affected_lines,
            )

        return ValidationResult(
            file_path=str(file_path),
            validation_name="check_malformed_quotes",
            status=ValidationStatus.PASSED,
            strategy_applied=ValidationStrategy.IGNORE,
            dataset_name=dataset_name,
            details="No malformed quotes detected",
        )

    def _line_has_even_quotes(self, line: str) -> bool:
        """Check if any field in the line has an EVEN number (> 0) of unescaped quotes.

        Args:
            line: A single CSV line

        Returns:
            True if any field has even unescaped quote count > 0
        """
        if '"' not in line:
            return False

        fields = line.split(";")
        for field in fields:
            cleaned_field = field.replace('""', "")
            quote_count = cleaned_field.count('"')
            if quote_count > 0 and quote_count % 2 == 0:
                return True

        return False

    def _fix_redundant_quotes(self, file_path: Path) -> FixResult:
        """Remove quotes from fields with an ODD number of unescaped quotes.

        For each field (split by `;`):
        - Count unescaped quotes (after removing already-escaped "")
        - Odd count (1, 3, etc.): remove all unescaped quotes from that field
        - Even count or 0: leave field unchanged

        This runs FIRST to remove stray quotes before _fix_malformed_quotes doubles remaining pairs.
        """
        try:
            content = file_path.read_text(encoding=self.encoding)
            lines = content.split("\n")

            modified_lines = []
            modified_line_numbers = []

            for line_num, line in enumerate(lines, start=1):
                if line_num == 1:
                    modified_lines.append(line)
                    continue
                if not line.strip():
                    modified_lines.append(line)
                    continue

                original_line = line
                fixed_line = self._remove_odd_quotes_from_line(line)
                modified_lines.append(fixed_line)

                if fixed_line != original_line:
                    modified_line_numbers.append(line_num)

            fixed_content = "\n".join(modified_lines)
            file_path.write_text(fixed_content, encoding=self.encoding)

            return FixResult(
                file_path=str(file_path),
                fix_name="fix_redundant_quotes",
                lines_fixed=len(modified_line_numbers),
                lines_removed=modified_line_numbers,
                success=True,
                details=f"Removed redundant quotes in {len(modified_line_numbers)} lines",
            )

        except Exception as e:
            return FixResult(
                file_path=str(file_path),
                fix_name="fix_redundant_quotes",
                lines_fixed=0,
                lines_removed=[],
                success=False,
                details=f"Error fixing file: {e}",
            )

    def _remove_odd_quotes_from_line(self, line: str) -> str:
        """Remove unescaped quotes from fields with odd quote count.

        Args:
            line: A single CSV line

        Returns:
            The line with odd-quote fields fixed
        """
        if '"' not in line:
            return line

        fields = line.split(";")
        fixed_fields = []

        for field in fields:
            cleaned_field = field.replace('""', "")
            unescaped_count = cleaned_field.count('"')

            if unescaped_count % 2 == 1:
                fixed_field = self._remove_unescaped_quotes(field)
                fixed_fields.append(fixed_field)
            else:
                fixed_fields.append(field)

        return ";".join(fixed_fields)

    def _remove_unescaped_quotes(self, field: str) -> str:
        """Remove unescaped quotes from a field, preserving escaped "" pairs.

        Args:
            field: A single CSV field value

        Returns:
            The field with unescaped quotes removed
        """
        result = []
        i = 0

        while i < len(field):
            char = field[i]

            if char == '"':
                if i + 1 < len(field) and field[i + 1] == '"':
                    result.append('""')
                    i += 2
                    continue
                else:
                    i += 1
                    continue

            result.append(char)
            i += 1

        return "".join(result)

    def _fix_malformed_quotes(self, file_path: Path) -> FixResult:
        """Double quotes in fields with an even number of unescaped quotes (> 0).

        For each field (split by `;`):
        - Count unescaped quotes (after removing already-escaped "")
        - Even count > 0 (2, 4, etc.): double each unescaped quote (" → "")
        - Zero or odd count: leave field unchanged

        This runs second after _fix_redundant_quotes has removed odd quotes.
        """
        try:
            content = file_path.read_text(encoding=self.encoding)
            lines = content.split("\n")

            modified_lines = []
            modified_line_numbers = []

            for line_num, line in enumerate(lines, start=1):
                if line_num == 1:
                    modified_lines.append(line)
                    continue
                if not line.strip():
                    modified_lines.append(line)
                    continue

                original_line = line
                fixed_line = self._double_even_quotes_in_line(line)
                modified_lines.append(fixed_line)

                if fixed_line != original_line:
                    modified_line_numbers.append(line_num)

            fixed_content = "\n".join(modified_lines)
            file_path.write_text(fixed_content, encoding=self.encoding)

            return FixResult(
                file_path=str(file_path),
                fix_name="fix_malformed_quotes",
                lines_fixed=len(modified_line_numbers),
                lines_removed=modified_line_numbers,
                success=True,
                details=f"Doubled malformed quotes in {len(modified_line_numbers)} lines",
            )

        except Exception as e:
            return FixResult(
                file_path=str(file_path),
                fix_name="fix_malformed_quotes",
                lines_fixed=0,
                lines_removed=[],
                success=False,
                details=f"Error fixing file: {e}",
            )

    def _double_even_quotes_in_line(self, line: str) -> str:
        """Double unescaped quotes in fields with even quote count.

        Args:
            line: A single CSV line

        Returns:
            The line with even-quote fields fixed
        """
        if '"' not in line:
            return line

        fields = line.split(";")
        fixed_fields = []

        for field in fields:
            cleaned_field = field.replace('""', "")
            unescaped_count = cleaned_field.count('"')

            if unescaped_count > 0 and unescaped_count % 2 == 0:
                fixed_field = self._double_unescaped_quotes(field)
                fixed_fields.append(fixed_field)
            else:
                fixed_fields.append(field)

        return ";".join(fixed_fields)

    def _double_unescaped_quotes(self, field: str) -> str:
        """Double unescaped quotes in a field, preserving escaped "" pairs.

        Args:
            field: A single CSV field value

        Returns:
            The field with unescaped quotes doubled
        """
        result = []
        i = 0

        while i < len(field):
            char = field[i]

            if char == '"':
                if i + 1 < len(field) and field[i + 1] == '"':
                    result.append('""')
                    i += 2
                    continue
                else:
                    result.append('""')
            else:
                result.append(char)

            i += 1

        return "".join(result)

    # =========================================================================
    # GENERIC IGNORE METHOD
    # =========================================================================

    def _ignore_affected_lines(
        self, file_path: Path, affected_lines: list[int]
    ) -> FixResult:
        """Remove specific lines from file (generic IGNORE implementation).

        This is the unified line-removal strategy that works for ANY validation.
        Just provide the list of 1-indexed line numbers to remove.

        Args:
            file_path: Path to the file to modify
            affected_lines: List of line numbers (1-indexed) to remove

        Returns:
            FixResult with lines_removed populated
        """
        if not affected_lines:
            return FixResult(
                file_path=str(file_path),
                fix_name="ignore_affected_lines",
                lines_fixed=0,
                lines_removed=[],
                success=True,
                details="No lines to remove",
            )

        try:
            content = file_path.read_text(encoding=self.encoding)
            has_trailing_newline = content.endswith("\n")

            lines = content.split("\n")
            if lines and lines[-1] == "":
                lines = lines[:-1]

            lines_to_remove = set(affected_lines)
            filtered_lines = []
            removed_line_numbers: list[int] = []

            for line_num, line in enumerate(lines):
                line_num_1indexed = line_num + 1

                if line_num == 0:
                    filtered_lines.append(line)
                    continue
                if not line.strip():
                    continue
                if line_num_1indexed in lines_to_remove:
                    removed_line_numbers.append(line_num_1indexed)
                    logger.debug(
                        f"Removing line {line_num_1indexed} from {file_path.name}: "
                        f"{line[:80]}..."
                    )
                else:
                    filtered_lines.append(line)

            result_content = "\n".join(filtered_lines)
            if has_trailing_newline:
                result_content += "\n"
            file_path.write_text(result_content, encoding=self.encoding)

            removed_count = len(removed_line_numbers)
            return FixResult(
                file_path=str(file_path),
                fix_name="ignore_affected_lines",
                lines_fixed=removed_count,
                lines_removed=removed_line_numbers,
                success=True,
                details=f"Removed {removed_count} lines",
            )

        except Exception as e:
            return FixResult(
                file_path=str(file_path),
                fix_name="ignore_affected_lines",
                lines_fixed=0,
                lines_removed=[],
                success=False,
                details=f"Error removing lines: {e}",
            )

    def _validate_fix_sanity(
        self, fixed_file: Path, backup_file: Path
    ) -> tuple[bool, str]:
        """Basic sanity check - did we accidentally delete too much?

        Only checks we didn't corrupt/delete massive amounts of data.

        Args:
            fixed_file: Path to the fixed file
            backup_file: Path to the backup file

        Returns:
            Tuple of (is_valid, message)
        """
        try:
            # Check: Row count didn't drop dramatically
            with open(fixed_file, "r", encoding=self.encoding) as f:
                fixed_lines = sum(1 for _ in f)

            with open(backup_file, "r", encoding=self.encoding) as f:
                backup_lines = sum(1 for _ in f)

            # Allow up to 50% reduction (e.g., removing many bad lines)
            # But more than 50% deletion is suspicious
            if fixed_lines < backup_lines * 0.5:
                return False, (
                    f"Too many lines removed: {backup_lines} -> {fixed_lines} "
                    f"({(1 - fixed_lines / backup_lines) * 100:.1f}% reduction)"
                )

            return True, "Sanity check passed"

        except Exception as e:
            return False, f"Sanity check failed: {e}"

    @classmethod
    def get_available_validations(cls) -> list[str]:
        """Get list of available validation check names."""
        return list(cls._validation_registry.keys())

    @classmethod
    def get_available_fixes(cls) -> list[str]:
        """Get list of validations that have fix methods."""
        return list(cls._fix_registry.keys())

    @staticmethod
    def generate_report(
        results: list[ValidationResult],
        include_passed: bool = False,
    ) -> pl.DataFrame:
        """Generate a report DataFrame from validation results.

        Args:
            results: List of ValidationResult objects.
            include_passed: If False (default), only report failures and fixes.
                           If True, report all validation results including passed.

        Returns:
            Polars DataFrame with validation results.
        """
        if not include_passed:
            filtered_results = [r for r in results if r.status != "passed"]
            logger.info(
                f"Filtered {len(results)} validation results to {len(filtered_results)} "
                f"issues (excluding passed validations)"
            )
        else:
            filtered_results = results

        rows = [r.to_csv_dict() for r in filtered_results]

        if rows:
            return pl.DataFrame(rows)
        else:
            return pl.DataFrame(
                {
                    "timestamp": [],
                    "dataset_name": [],
                    "validation_name": [],
                    "status": [],
                    "strategy_applied": [],
                    "details": [],
                    "affected_lines_count": [],
                    "affected_lines": [],
                    "fixes_applied": [],
                }
            )

    @staticmethod
    def print_summary(results: list[ValidationResult]) -> None:
        """Log a summary of validation results.

        Args:
            results: List of ValidationResult objects.
        """
        if not results:
            logger.info("No validation results to summarize.")
            return

        status_counts: dict[str, int] = {}
        for result in results:
            status = result.status
            status_counts[status] = status_counts.get(status, 0) + 1

        validation_counts: dict[str, int] = {}
        for result in results:
            name = result.validation_name
            validation_counts[name] = validation_counts.get(name, 0) + 1

        logger.info("\n" + "=" * 60)
        logger.info("DATA VALIDATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total validations run: {len(results)}")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("")

        logger.info("Results by status:")
        for status, count in sorted(status_counts.items()):
            logger.info(f"  {status}: {count}")
        logger.info("")

        logger.info("Results by validation type:")
        for name, count in sorted(validation_counts.items()):
            logger.info(f"  {name}: {count}")
        logger.info("")

        failures = [r for r in results if r.status == "failed"]
        if failures:
            logger.info(f"FAILURES ({len(failures)}):")
            for failure in failures[:10]:
                logger.info(f"  - {failure.file_path}")
                logger.info(f"    Validation: {failure.validation_name}")
                logger.info(f"    Details: {failure.details}")
            if len(failures) > 10:
                logger.info(f"  ... and {len(failures) - 10} more")
        else:
            logger.info("No failures detected.")

        logger.info("=" * 60 + "\n")
