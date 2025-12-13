from kedro.pipeline import pipeline, node
from ..nodes.pri.daily_nav import pri_create_daily_nav_data
from ..nodes.pri.daily_returns import pri_create_daily_returns
from ..nodes.pri.monthly_nav import pri_create_monthly_nav_data
from ..nodes.pri.characteristics import pri_create_fund_characteristics
from ..nodes.pri.fund_managers import pri_create_fund_managers
from ..nodes.pri.instrument_registry import pri_create_instrument_registry
from ..nodes.pri.instrument_prices import pri_create_instrument_prices
from ..nodes.pri.instrument_rating import pri_create_instrument_rating


def primary_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=pri_create_fund_characteristics,
                inputs=[
                    "raw_anbima_fund_characteristics",
                    "int_funds_in_scope",
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
                func=pri_create_instrument_registry,
                inputs=["int_blc"],
                outputs="pri_instrument_registry",
                name="create_instrument_registry",
            ),
            node(
                func=pri_create_instrument_prices,
                inputs=["pri_instrument_registry", "params:max_period"],
                outputs="pri_instrument_prices",
                name="create_instrument_prices",
            ),
            node(
                func=pri_create_instrument_rating,
                inputs=["pri_instrument_registry"],
                outputs="pri_instrument_rating",
                name="create_instrument_rating",
            ),
            node(
                func=pri_create_monthly_nav_data,
                inputs=["int_monthly_pl"],
                outputs="pri_nav_per_period",
                name="create_monthly_nav",
            ),
            node(
                func=pri_create_daily_nav_data,
                inputs=["int_daily_quotas", "int_funds_in_scope"],
                outputs="pri_daily_quota_nav",
                name="create_daily_nav",
            ),
            node(
                func=pri_create_daily_returns,
                inputs=["pri_daily_quota_nav"],
                outputs="pri_daily_returns",
                name="create_daily_returns",
            ),
        ]
    )
