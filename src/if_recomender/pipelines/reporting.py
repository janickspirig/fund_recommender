from kedro.pipeline import pipeline, node
from ..nodes.rpt.rankings import rpt_create_rankings


def reporting_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=rpt_create_rankings,
                inputs=[
                    "mo_scores_per_profile",
                    "mo_guardrail_mark",
                    "pri_characteristics",
                    "params:n_top_funds_output",
                    "params:investor_profiles",
                ],
                outputs=["rpt_shortlist", "rpt_complete_ranking"],
                name="create_rankings",
            )
        ]
    )
