"""Pydantic models for data validation."""

from datetime import datetime
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


class ValidationStrategy(str, Enum):
    """Strategy to apply when a validation check fails."""

    FIX = "fix"
    IGNORE = "ignore"


class ValidationStatus(str, Enum):
    """Status of a validation check."""

    PASSED = "passed"
    FAILED = "failed"
    FIXED = "fixed"


class ValidationResult(BaseModel):
    """Result of a single validation check on a file."""

    file_path: str
    validation_name: str
    status: ValidationStatus
    strategy_applied: ValidationStrategy
    dataset_name: str | None = None
    details: str | None = None
    affected_lines: list[int] = Field(default_factory=list)
    fixes_applied: int = 0
    timestamp: datetime = Field(default_factory=datetime.now)

    model_config = {"use_enum_values": True}

    def to_csv_dict(self) -> dict[str, str | int]:
        """Convert to flat dict for CSV export."""
        file_name = Path(self.file_path).name
        dataset_name = (
            f"{self.dataset_name}/{file_name}" if self.dataset_name else file_name
        )
        return {
            "timestamp": self.timestamp.isoformat(),
            "dataset_name": dataset_name,
            "validation_name": self.validation_name,
            "status": self.status.value
            if isinstance(self.status, ValidationStatus)
            else self.status,
            "strategy_applied": self.strategy_applied.value
            if isinstance(self.strategy_applied, ValidationStrategy)
            else self.strategy_applied,
            "details": self.details or "",
            "affected_lines_count": len(self.affected_lines),
            "affected_lines": ",".join(map(str, self.affected_lines))
            if self.affected_lines
            else "",
            "fixes_applied": self.fixes_applied,
        }


class FixResult(BaseModel):
    """Result of a fix operation on a file."""

    file_path: str
    fix_name: str
    lines_fixed: int = 0
    lines_removed: list[int] = Field(default_factory=list)
    success: bool = True
    details: str | None = None
    timestamp: datetime = Field(default_factory=datetime.now)


class DatasetValidationConfig(BaseModel):
    """Configuration for validating a single dataset."""

    validations: dict[str, ValidationStrategy] = Field(default_factory=dict)


class DataValidationConfig(BaseModel):
    """Top-level configuration for data validation."""

    enabled: bool = True
    datasets: dict[str, DatasetValidationConfig] = Field(default_factory=dict)
    report_include_passed: bool = False


class OutputValidationResult(BaseModel):
    """Result of an output validation on a DataFrame."""

    dataset_name: str
    validation_name: str
    passed: bool
    error_count: int = 0
    details: str | None = None
    affected_groups: list[dict] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)
