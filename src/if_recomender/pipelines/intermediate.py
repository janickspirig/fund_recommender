from kedro.pipeline import pipeline, node
from ..nodes.int.funds_in_scope import int_determine_funds_in_scope
from ..nodes.int.normalize_monthly_pl import int_normalize_monthly_pl
from ..nodes.int.normalize_daily_quotas import int_normalize_daily_quotas
from ..nodes.int.normalize_blc import int_normalize_blc


def intermediate_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=int_determine_funds_in_scope,
                inputs=[
                    "params:cvm_fi_fund_types",
                    "params:anbima_fi_fund_types",
                    "params:anbima_accessability",
                    "params:remove_funds_w_negative_cvm_pl_values",
                    "params:max_period",
                    "raw_anbima_fund_characteristics",
                    "raw_cvm_monthly_fund_data",
                ],
                outputs="int_funds_in_scope",
                name="determine_funds_in_scope",
            ),
            node(
                func=int_normalize_monthly_pl,
                inputs=["raw_cvm_monthly_fund_data", "int_funds_in_scope"],
                outputs="int_monthly_pl",
                name="normalize_monthly_pl",
            ),
            node(
                func=int_normalize_daily_quotas,
                inputs=["raw_cvm_daily_quotas", "int_funds_in_scope"],
                outputs="int_daily_quotas",
                name="normalize_daily_quotas",
            ),
            node(
                func=int_normalize_blc,
                inputs=[
                    "raw_cvm_blc_1_data",
                    "raw_cvm_blc_2_data",
                    "raw_cvm_blc_3_data",
                    "raw_cvm_blc_4_data",
                    "raw_cvm_blc_5_data",
                    "raw_cvm_blc_6_data",
                    "raw_cvm_blc_7_data",
                    "int_funds_in_scope",
                ],
                outputs="int_blc",
                name="normalize_blc",
            ),
        ]
    )
