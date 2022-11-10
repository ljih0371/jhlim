import argparse
from pathlib import Path

import mlflow
import pandas as pd
from azureml.core import Run
from pandas import DataFrame
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
)


PREDICTION = "pred"
TRUE_VALUE = "true"


def main(scoreddata_path: str):
    """Evaluate model. Calculate MAE, RMSE, and MAPE.

    Args:
        scoreddata_path (str): Path of scored dataset.
    """
    scored_data = read_data(scoreddata_path)

    # TODO: Parent Logging 기능은 확인 필요
    parent = get_parent_run()
    log_mae(scored_data, run=parent)
    log_rmse(scored_data, run=parent)
    log_mape(scored_data, run=parent)


def read_data(path: str) -> DataFrame:
    """Read scored dataset.

    Args:
        path (str): Path of dataset.

    Returns:
        DataFrame: DataFrame of dataset.
    """
    dfs = []

    path = Path(path)
    files = path.glob("*.csv")
    for file in files:
        dfs.append(pd.read_csv(file, index_col=0, encoding="utf-8").copy())

    data = pd.concat(dfs)
    return data


def get_parent_run() -> Run:
    """Get parent run. If not exists, return None.

    Returns:
        Run: Parent run object.
    """
    run = Run.get_context()

    if not hasattr(run, "parent"):
        return None
    return run.parent


def log_mae(scored_data: DataFrame, run: Run = None):
    """Log MAE.

    Args:
        scored_data (DataFrame): Scored dataset.
        run (Run, optional): Run object to log. Defaults to None.
    """
    KEY = "MAE"
    mae = cal_error_mae(scored_data)
    mlflow.log_metric(KEY, mae)

    if run:
        run.log(KEY, mae)


def log_rmse(scored_data: DataFrame, run: Run = None):
    """Log RMSE

    Args:
        scored_data (DataFrame): Scored dataset.
        run (Run, optional): Run object to log. Defaults to None.
    """
    KEY = "RMSE"
    rmse = cal_error_rmse(scored_data)
    mlflow.log_metric(KEY, rmse)

    if run:
        run.log(KEY, rmse)


def log_mape(scored_data: DataFrame, run: Run = None):
    """Log MAPE

    Args:
        scored_data (DataFrame): Scored dataset.
        run (Run, optional): Run object to log. Defaults to None.
    """
    KEY = "MAPE"
    mape = cal_error_mae(scored_data)
    mlflow.log_metric(KEY, mape)

    if run:
        run.log(KEY, mape)


def cal_error_mae(scored_data: DataFrame) -> float:
    """Claculate MAE

    Args:
        scored_data (DataFrame): Scored dataset.

    Returns:
        float: MAE
    """
    return mean_absolute_error(scored_data[TRUE_VALUE], scored_data[PREDICTION])


def cal_error_rmse(scored_data: DataFrame) -> float:
    """Claculate RMSE

    Args:
        scored_data (DataFrame): Scored dataset.

    Returns:
        float: RMSE
    """
    return mean_squared_error(scored_data[TRUE_VALUE], scored_data[PREDICTION]) ** 0.5


def cal_error_mape(scored_data: DataFrame) -> float:
    """Claculate MAPE

    Args:
        scored_data (DataFrame): Scored dataset.

    Returns:
        float: MAPE
    """
    return mean_absolute_percentage_error(
        scored_data[TRUE_VALUE], scored_data[PREDICTION]
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scoreddata-path",
        type=str,
        dest="scoreddata_path",
        default="./data/interim",
    )

    args = parser.parse_args()
    main(**vars(args))
