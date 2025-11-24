# src/your_project_name/pipelines/data_processing/pipeline.py

from kedro.pipeline import pipeline, node
from .nodes import load_data, process_data, save_data


def create_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=load_data, inputs=None, outputs="raw_data", name="load_data_node"
            ),
            node(
                func=process_data,
                inputs="raw_data",
                outputs="processed_data",
                name="process_data_node",
            ),
            node(
                func=save_data,
                inputs="processed_data",
                outputs=None,  # No explicit output for this node
                name="save_data_node",
            ),
        ]
    )
