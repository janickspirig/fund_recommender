# src/your_project_name/pipelines/data_processing/pipeline.py

from kedro.pipeline import pipeline, node
from ..nodes.pri.monthly_nav import pri_create_monthly_nav_data
from ..nodes.pri.returns import pri_create_returns_per_fund
from ..nodes.pri.characteristics import pri_create_fund_characteristics
from ..nodes.pri.fund_managers import pri_create_fund_managers
from ..nodes.pri.instrument_prices import pri_create_instrument_prices
from ..nodes.pri.composition import pri_create_composition
from ..nodes.pri.instrument_rating import pri_create_instrument_rating


def primary_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=pri_create_monthly_nav_data,
                inputs=["int_period_fund_data"],
                outputs="pri_nav_per_period",
                name="create_monthly_nav",
            ),
            node(
                func=pri_create_returns_per_fund,
                inputs=["pri_nav_per_period", "params:num_period_months"],
                outputs="pri_returns_per_fund",
                name="create_returns_per_fund",
            ),
            node(
                func=pri_create_fund_characteristics,
                inputs=[
                    "int_period_fund_data",
                    "int_funds_in_scope",
                    "int_fund_characteristics",
                ],
                outputs="pri_characteristics",
                name="create_fund_characteristics",
            ),
            node(
                func=pri_create_fund_managers,
                inputs=["pri_characteristics"],
                outputs="pri_fund_managers",
                name="create_fund_managers",
            ),
            node(
                func=pri_create_instrument_prices,
                inputs=[
                    "int_funds_in_scope",
                    "raw_cvm_blc_1_data",
                    "raw_cvm_blc_2_data",
                    "raw_cvm_blc_3_data",
                    "raw_cvm_blc_4_data",
                    "raw_cvm_blc_5_data",
                    "raw_cvm_blc_6_data",
                    "raw_cvm_blc_7_data",
                    "raw_cvm_blc_8_data",
                ],
                outputs="pri_instrument_prices",
                name="create_instrument_prices",
            ),
            node(
                func=pri_create_composition,
                inputs=["pri_instrument_prices"],
                outputs="pri_composition",
                name="create_composition",
            ),
            node(
                func=pri_create_instrument_rating,
                inputs=["int_funds_in_scope", "raw_cvm_blc_5_data"],
                outputs="pri_instrument_rating",
                name="create_instrument_rating",
            ),
        ]
    )
