# src/your_project_name/pipelines/data_processing/pipeline.py

from kedro.pipeline import pipeline, node
from ..nodes.feat.volatility import feat_calculate_volatility
from ..nodes.feat.sharpe_ratio import feat_calculate_sharpe_ratio
from ..nodes.feat.liquidity import feat_calculate_liquidity
from ..nodes.feat.concentration import feat_calculate_concentration
from ..nodes.feat.asset_diversification import feat_calculate_asset_diversification
from ..nodes.feat.credit_quality import feat_calculate_credit_quality
from ..nodes.feat.fund_age import feat_calculate_fund_age
from ..nodes.feat.merge_features import feat_merge_all_features


def feature_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=feat_calculate_volatility,
                inputs=["pri_returns_per_fund"],
                outputs="fea_volatility_per_fund",
                name="calculate_volatility",
            ),
            node(
                func=feat_calculate_sharpe_ratio,
                inputs=[
                    "pri_returns_per_fund",
                    "params:risk_free_rate_annual",
                    "params:max_period",
                ],
                outputs="fea_sharpe_ratio_per_fund",
                name="calculate_sharpe_ratio",
            ),
            node(
                func=feat_calculate_liquidity,
                inputs=["pri_characteristics"],
                outputs="fea_liquidity_per_fund",
                name="calculate_liquidity",
            ),
            node(
                func=feat_calculate_concentration,
                inputs=["pri_instrument_prices"],
                outputs="fea_concentration_per_fund",
                name="calculate_concentration",
            ),
            node(
                func=feat_calculate_asset_diversification,
                inputs=["pri_instrument_prices"],
                outputs="fea_asset_diversification_per_fund",
                name="calculate_asset_diversification",
            ),
            node(
                func=feat_calculate_credit_quality,
                inputs=[
                    "pri_instrument_rating",
                    "pri_instrument_prices",
                    "params:credit_rating",
                ],
                outputs="fea_credit_quality_per_fund",
                name="calculate_credit_quality",
            ),
            node(
                func=feat_calculate_fund_age,
                inputs=[
                    "pri_characteristics",
                    "params:fund_age_cap_years",
                ],
                outputs="fea_fund_age_per_fund",
                name="calculate_fund_age",
            ),
            node(
                func=feat_merge_all_features,
                inputs=[
                    "fea_volatility_per_fund",
                    "fea_sharpe_ratio_per_fund",
                    "fea_liquidity_per_fund",
                    "fea_concentration_per_fund",
                    "fea_asset_diversification_per_fund",
                    "fea_credit_quality_per_fund",
                    "fea_fund_age_per_fund",
                    "pri_characteristics",
                ],
                outputs="fea_features_per_fund",
                name="merge_all_features",
            ),
        ]
    )
