import argparse
from pathlib import Path

import mlflow
import mltable
from numpy import ndarray
import pandas as pd
from catboost import CatBoostRegressor
from pandas import DataFrame

from data import COLUMNS_FOR_X, COLUMN_FOR_Y
from azureml.core import Run

def main(model_path: str, testdata_param: str, scoreddata_path: str):
    """Score model. Combine predicted values and true values.

    Args:
        model_path (str): Path of trained model.
        testdata_path (str): Path of test dataset to read.
        scoreddata_path (str): Path of scored data to read.
    """
    model = load_model(model_path)

    ##추가 221104
    input_tabular_ds = Run.get_context().input_datasets["testdata"]
    test_data = input_tabular_ds.to_pandas_dataframe()
    ##
    
    # test_data = read_data(testdata_path)

    predicted = predict(model, test_data)
    scored = generate_scored_data(predicted, test_data)

    save_scored_data(scored, scoreddata_path)


# def read_data(path: str) -> DataFrame:
#     """Read tabular dataset.

#     Args:
#         path (str): Path of dataset.

#     Returns:
#         DataFrame: Dataframe of dataset.
#     """
#     tbl = mltable.load(path)
#     return tbl.to_pandas_dataframe()


def load_model(model_path: str) -> CatBoostRegressor:
    """Load catboost model

    Args:
        model_path (str): Path of model.

    Returns:
        CatBoostRegressor: Trained model.
    """
    return mlflow.catboost.load_model(model_path)


def predict(model: CatBoostRegressor, test_data: DataFrame) -> ndarray:
    """Predict on test dataset.

    Args:
        model (CatBoostRegressor): Path of model.
        test_data (DataFrame): Dataframe of test dataset.

    Returns:
        ndarray: Predicted values
    """
    x_test = test_data[COLUMNS_FOR_X].to_numpy()
    pred = model.predict(x_test)
    return pred


def generate_scored_data(pred: ndarray, test_data: DataFrame) -> DataFrame:
    """Combine predicted values and true values.

    Args:
        pred (ndarray): Predicted values.
        test_data (DataFrame): True values.

    Returns:
        DataFrame: Scored dataset.
    """
    df_pred = DataFrame(pred, columns=["pred"])
    y_test = DataFrame(test_data[COLUMN_FOR_Y].values, columns=["true"])

    return pd.concat([df_pred, y_test], axis=1)


def save_scored_data(scored_data: DataFrame, save_path: str):
    """Save scored data.

    Args:
        scored_data (DataFrame): Dataset.
        save_path (str): Path to save.
    """
    path = Path(save_path)
    path.mkdir(exist_ok=True, parents=True)

    scored_data.to_csv(path / "predictions.csv", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    ##input
    parser.add_argument(
        "--model-path", type=str, dest="model_path")
    
    # parser.add_argument(
    #     "--testdata-path",
    #     type=str,
    #     dest="testdata_path",
    #     default="./data/processed/test",
    # )
    
     ##추가 221104
    parser.add_argument("--param1", type=str, dest="testdata_param") 
    
    ##output
    parser.add_argument(
        "--scoreddata-path",
        type=str,
        dest="scoreddata_path",
        default="./data/interim",
    )

    args = parser.parse_args()
    main(**vars(args))
