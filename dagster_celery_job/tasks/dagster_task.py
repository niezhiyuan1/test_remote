# coding=utf-8
"""
Auther:niezy
Email:niezy4@lenovo.com
"""
import math
import pickle
import time
import zlib

import numpy as np
import pandas as pd
import requests
from dagster import asset
from retrying import retry


class ResponseTimeProcess:

    def __init__(self):
        pass

    def get_prometheus_data(self, params, spcial=False):
        url = params.get("task_predict_input_dataset").get("url")
        param = params.get("task_predict_input_dataset").get("data_catalog_list")[0]
        if spcial:
            filter_catalog_id = params.get("custom_parameter").get("filter_catalog_id")
            param['catalog_id'] = filter_catalog_id
        items = self.request_data(url, param)
        try:
            df = self.format_data(items)
            return df
        except Exception as e:
            raise e

    @retry(stop_max_attempt_number=20, wait_fixed=10)
    def request_data(self, url, param, verify=False):
        """请求数据"""
        try:
            res = requests.post(url, json=param, verify=verify)
            if res.status_code == 200:
                result = res.json()
                return result
        except Exception as e:
            raise e

    def format_data(self, items):
        df = pd.DataFrame(items["content"]["data"][0]["values"])
        df.columns = ["date", "value"]
        df["value"] = [float(i) for i in df["value"]]
        df["date"] = [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tt)) for tt in df["date"]]
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date", drop=True, append=False, inplace=False, verify_integrity=False)
        return df


class ResponseTimeAnomalyDetect:
    delay_win = 12  # 每次检测delay_win窗口数据 当异常持续30min时候，设定为异常
    delay_anomaly_win = 2  # 1H 内有20min异常
    min_thred = 15  # 低于15ms的响应直接判断为正常
    MSG = ''
    # min_length = 288 * 7  # 最少7天数据， 否则给出警告
    min_train_warn = 30  # 用于构建分布的数据，警告值
    min_train_error = 10  # 用户构建分布数据 ，错误值

    def __init__(self, parameter):
        self.min_length = int((1440 * 60 * 7) / parameter.get("custom_parameter").get("step"))
        self.prob = parameter.get("custom_parameter").get("prob", 0.05)
        self.min_io_threshold = parameter.get("custom_parameter").get("min_io_threshold", 100)
        self.day_win = int((1440 * 60) / parameter.get("custom_parameter").get("step"))

    def preprocess(self, data):
        # 数据校验， 但数据量太少时候，给出警告，检测结果可能不准确
        if len(data) < self.min_length:
            self.MSG = "用于训练的历史数据较少，检测结果可能不准确"
        # nan 直接填充为0，不用于模型构建
        data = data.fillna(0)
        return data

    def get_beta_score(aelf, x, a, b):
        score = (math.pow(x + 1e-6, a - 1) * math.pow(1 - x + 1e-6, b - 1)) / (
                math.gamma(a) * math.gamma(b) / (math.gamma(a + b)))
        return score

    def get_beta_detect_result(self, series, train_series, a, b, prob):
        anomaly_result = None
        detect_result = []
        min_s = min(train_series)
        max_s = max(train_series)
        for i in series[-self.delay_win:]:
            if i < self.min_thred:
                _label = 0
            else:
                x = (i - min_s) / (max_s - min_s)
                score = self.get_beta_score(x, a, b)
                if score < prob:
                    _label = 1
                else:
                    _label = 0
            detect_result.append(_label)
        detect_result_label = []
        if sum(detect_result) >= self.delay_anomaly_win and detect_result[-1] == 1:
            for i in series:
                if i < self.min_thred:
                    _label = 0
                else:
                    x = (i - min_s) / (max_s - min_s)
                    score = self.get_beta_score(x, a, b)
                    if score < prob:
                        _label = 1
                    else:
                        _label = 0
                detect_result_label.append(_label)
            for j in range(self.day_win + self.delay_win, len(detect_result_label)):
                if detect_result_label[j] == 1 and max(
                        detect_result_label[j - self.day_win - self.delay_win:j - self.day_win + self.delay_win]) == 1:
                    detect_result_label[j] = 0
            if detect_result_label[-1] == 1:
                anomaly_result = 1  # 异常
            else:
                anomaly_result = 0
                detect_result_label = []
        else:
            anomaly_result = 0  # 正常

        return anomaly_result, detect_result_label

    def fit_beta(self, series):
        X = (series - min(series)) / (max(series) - min(series))  # 归一化
        u = np.mean(X)  # 均值
        s = np.var(X)  # 方差
        a = u * ((u * (1 - u) / s) - 1)  # 参数估计、矩估计，估计alpha BETA
        b = (1 - u) * ((u * (1 - u) / s) - 1)
        return a, b

    def dynamic_beta(self, data, prob=0.05):
        data = self.preprocess(data)  # preprocess
        normal_res_dict = {"MSG": self.MSG, "anomaly_result": 0, "anomaly_data": None}
        if np.sum(data["value"][-self.delay_win:] < self.min_thred) <= self.delay_anomaly_win:
            return normal_res_dict  # 如果检测数据<15ms,则不需要检测
        else:
            series = data["value"]
            train_series = np.array([i for i in series if i > self.min_thred])  # 用>15ms的数据构建分布
            if len(train_series) < self.min_train_error:
                self.MSG = "用于训练的数据组不足，不进行检测"
                return normal_res_dict
            elif len(train_series) < self.min_train_warn:
                self.MSG = "用于训练的有效数据较少，检测结果可能不准确"
            a, b = self.fit_beta(train_series)
        anomaly_result, detect_result_label = self.get_beta_detect_result(series, train_series, a, b, prob)
        if len(detect_result_label) > 0:
            data["detect_result_label"] = detect_result_label
            normal_res_dict["anomaly_data"] = data
            normal_res_dict["anomaly_result"] = anomaly_result
        else:
            normal_res_dict["anomaly_result"] = anomaly_result
        return normal_res_dict

    def detect(self, data, parameter):
        result = self.dynamic_beta(data, self.prob)
        if result['anomaly_result'] == 1:
            vm = ResponseTimeProcess()
            io_data = vm.get_prometheus_data(params=parameter, spcial=True)
            if io_data['value'][-1] <= self.min_io_threshold:
                result = {"MSG": self.MSG, "anomaly_result": 0, "anomaly_data": None}
        return result


@retry(stop_max_attempt_number=100, wait_fixed=10)
def save_predict_data(url, data, verify=False):
    try:
        res = requests.post(url, json=data, verify=verify)
        if res.status_code == 200:
            return
        raise "save fail"
    except Exception as e:
        raise e


def zlib_compress_data(data):
    """压缩数据，将DataFrame的表格数据加密成二进制的 字符串    :param data: DataFrame 对象    :return: 二进制字符串    """
    data = zlib.compress(pickle.dumps(data), 5)
    return str(data, "ISO-8859-1")


@retry(stop_max_attempt_number=20, wait_fixed=10)
def request_data(url, param, verify=False):
    """请求数据"""
    try:
        res = requests.post(url, json=param, verify=verify)
        if res.status_code == 200:
            result = res.json()
            return result
    except Exception as e:
        raise e


@asset
def dataset_input(parameter):
    """
    存储卷IO延迟数据获取
    :return:
    """
    url = parameter.get("task_predict_input_dataset").get("url")
    param = parameter.get("task_predict_input_dataset").get("data_catalog_list")[0]
    items = request_data(url, param)
    return items


@asset
def dataset_to_datafream(dataset_input):
    """
    获取的时序数据转换成DataFrame
    :return:
    """
    df = pd.DataFrame(dataset_input["content"]["data"][0]["values"])
    df.columns = ["date", "value"]
    df["value"] = [float(i) for i in df["value"]]
    df["date"] = [time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tt)) for tt in df["date"]]
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date", drop=True, append=False, inplace=False, verify_integrity=False)
    print(df.tail(10))
    return df


@asset
def model_time_anomaly_detect(dataset_to_datafream, parameter):
    """
    获取时序数据生成相关数学模型
    :param dataset_to_datafream:
    :return:
    """
    model = ResponseTimeAnomalyDetect(parameter)
    result = model.detect(dataset_to_datafream, parameter)
    return result


@asset
def anomaly_detect_handle(model_time_anomaly_detect, parameter):
    """
    判断当前返回值是否存在异常
    :return:
    """
    alg_result = {"request_info": parameter, "result_info": [], "task_status": True}
    try:
        if model_time_anomaly_detect.get("anomaly_data") is not None:
            model_time_anomaly_detect["anomaly_data"] = zlib_compress_data(model_time_anomaly_detect["anomaly_data"])
        alg_result["result_info"] = model_time_anomaly_detect
    except Exception as e:
        result_info = {"MSG": str(e), "anomaly_result": 0, "anomaly_data": None}
        alg_result["result_info"] = result_info
        alg_result["task_status"] = False
    finally:
        return alg_result


@asset
def save_check_result(anomaly_detect_handle, parameter):
    """
    保存当前数据返回结果
    :param anomaly_detect_handle:
    :return:
    """

    save_result = parameter.get("task_predict_save_result")
    url = save_result.get("url")
    storage_type = save_result.get("storage_type")
    data = {
        "storage_type": storage_type,
        "job_id": parameter.get("job_id"),
        "rowkey": parameter.get("task_id"),
        "content": anomaly_detect_handle
    }
    try:
        save_predict_data(url, data)
        return True
    except Exception as e:
        raise e


from dagster_celery_job.celery import app


@app.task()
def dagster_task(execute_data):
    param = execute_data
    input = dataset_input(param)
    datafream = dataset_to_datafream(input)
    result = model_time_anomaly_detect(datafream, param)
    res = anomaly_detect_handle(result, param)
    save_check_result(res, param)
    print("--save_check_result success--")
