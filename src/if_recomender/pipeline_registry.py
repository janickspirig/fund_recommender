"""Project pipelines."""

from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from if_recomender.pipelines.intermediate import intermediate_pipeline
from if_recomender.pipelines.primary import primary_pipeline
from if_recomender.pipelines.feature import feature_pipeline
from if_recomender.pipelines.model_input import model_input_pipeline
from if_recomender.pipelines.model_output import model_output_pipeline
from if_recomender.pipelines.reporting import reporting_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()
    pipelines["intermediate"] = intermediate_pipeline()
    pipelines["primary"] = primary_pipeline()
    pipelines["feature"] = feature_pipeline()
    pipelines["model_input"] = model_input_pipeline()
    pipelines["model_output"] = model_output_pipeline()
    pipelines["reporting"] = reporting_pipeline()
    pipelines["__default__"] = sum(pipelines.values())
    return pipelines
