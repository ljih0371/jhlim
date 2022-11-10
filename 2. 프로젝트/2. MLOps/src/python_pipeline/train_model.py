import argparse
# from msilib import Table
# from msilib.schema import tables
import re
from distutils.dir_util import copy_tree
from pathlib import Path

import mlflow
import mltable
from azureml.core import Run
from azureml.interpret import ExplanationClient
from catboost import CatBoostRegressor
from interpret.ext.blackbox import TabularExplainer
from numpy import ndarray
from pandas import DataFrame
from sklearn.model_selection import train_test_split

from data import COLUMNS_FOR_X, COLUMN_FOR_Y
from azureml.data.dataset_consumption_config import DatasetConsumptionConfig

def main(
    location: str,
    # traindata_path: str,
    model_path: str,
    test_size: float,
    shuffle: bool,
    random_state: int,
    message: str,
    traindata_param: str
):
    """Train catoboost model.

    Args:
        location (str): Location of PV power station.
        traindata_path (str): Path of train dataset to read.
        model_path (str): Path of model to save.
        test_size (float): Represent the proportion of the dataset to include in the\
            test split.
        shuffle (bool): Whether or not to shuffle the data before splitting.
        random_state (int): Controls the shuffling applied to the data before applying\
            the split.
        message (str): External parameter.
    """
    print()
    print('-' * len(message))
    print(f">>> {message}")
    print('-' * len(message))
    print()

    model_path = Path(model_path)

    ##추가 221103
    input_tabular_ds = Run.get_context().input_datasets["traindata"]
    train_data = input_tabular_ds.to_pandas_dataframe()
    ##
    
    # train_data = read_train_data(traindata_path)
    x_train, x_test, y_train, _ = split_data(
        train_data, test_size, shuffle, random_state
    )

    model = train_catboost_regressor(x_train, y_train)

    model_name = create_model_name(location)
    log_model(model, model_name)
    save_trained_model(model, model_name, model_path)

    explainer = get_explainer(model, x_train)
    global_explanation = explain_global(explainer, x_test)
    upload_explain(global_explanation)


# def read_train_data(path: str) -> DataFrame:
#     """Read train dataset. The type of dataset should be `Tabular`.

#     Args:
#         path (str): Mounted path of train dataset.

#     Returns:
#         DataFrame: DataFrame of train dataset.
#     """
#     tbl = mltable.load(path)
#     return tbl.to_pandas_dataframe()


def split_data(
    data: DataFrame,
    test_size: float = 0.3,
    shuffle: bool = True,
    random_state: int = 34,
) -> list:
    """split data to train dataset and validation set.

    Args:
        data (DataFrame): Preprocessed training data, where `n_sample` is the number of\
            samples and `n_features` is the number of features.
        test_size (float, optional): Represent the proportion of the dataset to include\
            in the test split.. Defaults to 0.3.
        shuffle (bool, optional): Whether or not to shuffle the data before splitting.\
            Defaults to True.
        random_state (int, optional): Controls the shuffling applied to the data before\
            applying the split.. Defaults to 34.

    Returns:
        list: List of [x_train, x_test, y_train, y_test]
    """
    return train_test_split(
        data[COLUMNS_FOR_X],
        data[COLUMN_FOR_Y],
        test_size=test_size,
        shuffle=shuffle,
        random_state=random_state,
    )


def train_catboost_regressor(x_train: ndarray, y_train: ndarray) -> CatBoostRegressor:
    """Train model.

    Args:
        x_train (ndarray): Feature values.
        y_train (ndarray): Target values.

    Returns:
        CatBoostRegressor: trained model.
    """
    logger = TrainLogger()

    model = CatBoostRegressor()
    model.fit(x_train, y_train, log_cout=logger)
    return model


class TrainLogger:
    """Logger for logging catboost's outputs while training."""

    # * Extract learnning loss
    # * ex) nnn: learn: n.nnnnnnn total: n.nns remaining: nn.nnus
    pattern = re.compile(r"(?<=learn: )\d+.\d+")

    def write(self, message: str):
        """Write learnning loss. It is essential method for logger.

        Args:
            message (str): Output stream.
        """
        print(message)

        learn = self.pattern.search(message)
        if learn is not None:
            mlflow.log_metric("learn", float(learn.group()))


def create_model_name(location: str) -> str:
    """Create model name `{location}_catboost`.

    Args:
        location (str): Location of PV power station.

    Returns:
        str: Name of model.
    """
    return f"{location}_catboost"


def save_trained_model(model: CatBoostRegressor, name: str, model_path: Path):
    """Save trained model using MLFlow. It saves MLFlow model to current workspace and \
        then copy to `model_path`.

    Args:
        model (CatBoostRegressor): Trained model.
        name (str): Name of model.
        model_path (Path): Path to save.
    """
    if not Path(name).exists():
        mlflow.catboost.save_model(model, name)

    model_path.mkdir(exist_ok=True, parents=True)
    copy_tree(name, str(model_path))


def log_model(model: CatBoostRegressor, name: str):
    """Register model.add()

    Args:
        model (CatBoostRegressor): Trained model.
        name (str): Name of model
    """
    mlflow.catboost.log_model(model, name, registered_model_name=name)


def get_explainer(model: CatBoostRegressor, x_train: ndarray) -> TabularExplainer:
    return TabularExplainer(model, x_train, features=COLUMNS_FOR_X)


def explain_global(
    explainer: TabularExplainer, x_test: ndarray
):
    """Globally explains the black box model.

    Args:
        explainer (TabularExplainer): Explainer.
        x_test (ndarray): Features of test dataset.

    Returns:
        DynamicGlobalExplanation: A model explanation object.
    """
    global_explanation = explainer.explain_global(x_test)
    return global_explanation


def upload_explain(explain):
    """Upload explanation to Azure using Run object.

    Args:
        explain (DynamicGlobalExplanation): The model explanation object.
    """
    run = Run.get_context()
    client = ExplanationClient.from_run(run)

    client.upload_model_explanation(explain)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # output
    parser.add_argument(
        "--model-path", type=str, dest="model_path", default="./outputs")
    
    ## input  
    parser.add_argument("--location", type=str, dest="location")
    # parser.add_argument(
    #     "--traindata-path",
    #     type=str,
    #     dest="traindata_path",
    #     default="./data/processed/train",
    # )
  
    parser.add_argument("--test-size", type=float, dest="test_size", default=0.3)
    parser.add_argument("--shuffle", type=bool, dest="shuffle", default=True)
    parser.add_argument("--random-state", type=int, dest="random_state", default=34)
    parser.add_argument("--message", type=str, dest="message", default="Test")

    ##추가 221103
    parser.add_argument("--param1",  type=str, dest="traindata_param") 
    
    args = parser.parse_args()
    main(**vars(args))
