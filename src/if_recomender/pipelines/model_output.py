from kedro.pipeline import pipeline, node
from ..nodes.mo.scoring_per_profile import mo_scoring_per_profile


def model_output_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=mo_scoring_per_profile,
                inputs=["mi_scoring_input", "params:investor_profiles"],
                outputs="mo_scores_per_profile",
                name="score_funds_per_profile",
            )
        ]
    )
