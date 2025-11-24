from kedro.pipeline import pipeline, node
from ..nodes.mi.create_scoring_input import mi_create_scoring_input


def model_input_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=mi_create_scoring_input,
                inputs=[
                    "fea_features_per_fund",
                    "params:normalization_lower_percentile",
                    "params:normalization_upper_percentile",
                    "params:use_log_volatility",
                ],
                outputs="mi_scoring_input",
                name="create_scoring_input",
            )
        ]
    )
