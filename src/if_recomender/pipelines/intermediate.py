# src/your_project_name/pipelines/data_processing/pipeline.py

from kedro.pipeline import pipeline, node
from ..nodes.int.filter_fi import int_filter_fixed_income_funds


def intermediate_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=int_filter_fixed_income_funds,
                inputs=[
                    "params:num_period_months",
                    "params:cvm_fi_fund_types",
                    "params:anbima_fi_fund_types",
                    "params:anbima_accessability",
                    "params:min_data_period_per_fund",
                    "params:remove_funds_w_negative_cvm_pl_values",
                    "params:max_period",
                    "raw_anbima_fund_characteristics",
                    "raw_cvm_monthly_fund_data",
                ],
                outputs=[
                    "int_period_fund_data",
                    "int_fund_characteristics",
                    "int_funds_in_scope",
                ],
                name="filter_fixed_income_funds",
            )
        ]
    )
