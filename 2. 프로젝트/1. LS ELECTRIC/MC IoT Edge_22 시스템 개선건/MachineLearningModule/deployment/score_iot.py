
# For Edge
import json
import yaml
import time
import os
import re
import pickle
import shutil
from datetime import datetime
from datetime import timedelta
from time import sleep
from azureml.core.model import Model

# For Clf
import h5py
import pandas as pd
from classify import Classifier
from preprocess_signal_data import Run_
from pkgs.pipelines import Feature050 as Featurizing
from pkgs.utils import read_json


def init_preprocessor(feature_params, Featurizing, test_no, input_type="ReadData"):
    # set preprocess class
    preproc = Run_(
        feature_params=feature_params, Featurizing=Featurizing, test_no=test_no
    )
    if input_type == "ReadData":
        preproc.pl.steps = [
            (step_name, step)
            for step_name, step in preproc.pl.steps
            if step.name != "ReadData"
        ]

    return preproc


def score(
    preproc,
    input_values,
    feature_names,
    column_names,
    cut_off,
    rule_model_params,
    mode,
    input_name="ReadData",
    parallel=1,
    verbose=0,
):
    # Preprocess
    df = preproc.pl.run(
        input_name=input_name,
        input_values=[input_values],
        feature_names=feature_names,
        parallel=parallel,
        verbose=verbose,
    )
    df = pd.DataFrame(df[0])
    df["TEST_NO"] = [0,1,2]
    df_features = df[column_names]

    # Predict
    df["PROB"] = loaded_model.predict_proba(df_features)[:,1]
    
    # Classify
    df["ML_R"] = df.PROB >= cut_off
    df["ML_R"] = df.ML_R.astype(int)
    df = Classifier.classify_all(
        df=df[df.TEST_NO.isin(preproc.featurizer.params["test_no"])],
        rule_cutoffs=rule_model_params,
        mode=mode,
    )
    return df


def init():
    global preproc, loaded_model, model_version, column_names, line, cut_off, rule_model_params, model_path, error_dir
    # 수정필요  --------------------------------------------------------
    model_version = "03_05_02"  # model_name 입력
    # ----------------------------------------------------------------
    file_name = "{}.pickle".format(model_version)
    edge_config = "/home/data/edge_config.yml"
#     edge_config = "../deployment/config/edge_config.yml"
    feature_params = read_json("config/feature050_parameters.json")
    column_names = read_json("config/model_03_050_02_features.json")["feature_names"]

    # set preprocess class
    preproc = init_preprocessor(
        feature_params=feature_params, Featurizing=Featurizing, test_no=[0, 1, 2]
    )

    # load model
    model_path = Model.get_model_path(file_name)
    with open(model_path, "rb") as f:
        loaded_model = pickle.load(f)

    # load line info
    with open(edge_config, "r") as stream:
        try:
            edge_config = yaml.load(stream, Loader=yaml.BaseLoader)
            line = edge_config["config"]["line"]["name"]
            cut_off = float(edge_config["config"]["param"]["cutoff"])
            rule_model_params = edge_config["config"]["param"]["rule_model"]

        except yaml.YAMLError as exc:
            print("line config error: ", exc)

    error_dir = "/home/data/error_file/"
    try:
        os.makedirs(error_dir, mode=777)
    except:
        pass


def run(input_json):
    print("\n", "mlmodule start")
    print("\n", datetime.now() + timedelta(hours=9), "\n")
    # for test#
    input_json = json.loads(input_json)
    print("\n", "json loaded", "\n")
    print(input_json, "\n")
    mltime = datetime.now() + timedelta(hours=9)
    chtime = input_json["chtime"]
    print("chtime : ", chtime)
    ct = datetime.strptime(
        chtime.replace("T", " ").split("+")[0][:-1], "%Y-%m-%d %H:%M:%S.%f"
    )
    diff = mltime - ct
    input_json["chtime"] = str(ct)
    input_json["mltime"] = str(mltime)
    input_json["etime_ch"] = diff.seconds + diff.microseconds / 1e6

    # file load
    init_time = time.time()
    # input_json = json.loads(input_json)
    input_path = input_json["path"]
    print("\n", input_path)

    for attempt in range(3):
        try:
#             input_raw = pr.ReadDataBlob(input_path)
            print("Attempt: " + str(attempt + 1) + "  Time: " + str(datetime.now()))
            with h5py.File(input_path, "r") as f:
                tmp = f["Raw"][:]
            input_raw = pd.DataFrame(tmp, columns=[0, 1, 2])
            print("\n", "hdf loaded")

        except Exception as ex:
            if attempt == 2:
                ex_message = str(ex)

            sleep(0.02)
            continue

        break

    else:
        input_json["b"] = 2
        input_json["prob"] = 2
        input_json["error"] = ex_message
        input_json["etime"] = time.time() - init_time

        result_json = [json.dumps(input_json)]
        print("*" * 5, " ", "LOAD ERROR", " ", "*" * 5)
        print(result_json)
        try:
            shutil.copy(input_path, error_dir)
        except:
            pass

    # for test#
    # diff = (datetime.now()+ timedelta(hours=9))-mltime
    # input_json['etime_load'] = diff.seconds + diff.microseconds/1E6
    input_json["etime_load"] = time.time() - init_time

    ### json insert
    input_json["cutoff"] = cut_off
    input_json["TRHD_NM_SET"] = "/".join([key for key in rule_model_params.keys()])
    input_json["TRHD_VAL_SET"] = "/".join(
        [str(value) for value in rule_model_params.values()]
    )
    input_json["FTUR_NM_SET"] = "/".join(
        [
            "/".join([col + "_TEST_N1" for col in column_names]),
            "/".join([col + "_TEST_N2" for col in column_names]),
            "/".join([col + "_TEST_N3" for col in column_names]),
        ]
    )
    input_json["lid"] = line
    input_json["v"] = model_version

    try:
        filename = input_path.split("/")[-1]
        filename = filename.strip("Data\\").split("_")
        input_json["bc"] = "_".join(filename[0:2])
        input_json["dtfull"] = filename[2][:-3]

    except Exception as ex:  # 에러 종류
        input_json["bc"] = "ERROR"
        input_json["dtfull"] = str(datetime.now())
        input_json["r"] = 2
        input_json["prob"] = 2
        input_json["error"] = str(ex)
        input_json["etime"] = time.time() - init_time

        result_json = [json.dumps(input_json)]
        print("*" * 5, " ", "FILENAME ERROR", " ", "*" * 5)
        print(result_json)
        try:
            cmd = "cp " + input_json["path"] + " " + error_dir
            os.system(cmd)
        except:
            pass

    try:
        df = score(
            preproc=preproc,
            input_values=input_raw,
            feature_names=preproc.featurizer.params["feature_names"],
            column_names=column_names,
            cut_off=cut_off,
            rule_model_params=rule_model_params,
            mode="or",
            input_name="ReadData",
            parallel=1,
            verbose=0,
        )
        
        print(df)
        input_json["r"] = int(df.R.values[0])
        input_json["prob"] = float(df.PROB.values[0])
        input_json["error"] = str(df.TEST_NG_R.values[0])
        input_json["etime"] = time.time() - init_time
        input_json["FTUR_VAL_SET"] = "/".join(
            [str(value) for value in df_features.flatten()]
        )

        result_json = [json.dumps(input_json)]
        print(result_json)

    except Exception as ex:  # 에러 종류
        input_json["r"] = 2
        input_json["prob"] = 2
        input_json["error"] = str(ex)
        input_json["etime"] = time.time() - init_time
        input_json["FTUR_VAL_SET"] = ""

        result_json = [json.dumps(input_json)]
        print("*" * 5, " ", "ERROR", " ", "*" * 5)
        print(result_json)
        try:
            cmd = "cp " + input_json["path"] + " " + error_dir
            os.system(cmd)
        except:
            pass

    return result_json
