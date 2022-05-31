import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler


def read_data(folder_path='/Users/shiyang/Desktop/Projects/cryptocurrency/Exp Data/v2/', name='returns', binary=False):
    data = pd.read_csv(folder_path + name + '.csv', index_col=0)
    if binary:
        data = data.applymap(lambda x: 1 if x > 0 else 0)
    return data


def series_to_supervised(data, n_in=1, n_out=1):
    """
    transform the raw dataset to lagged training dataset
    data: raw dataset
    n_in: number of days want to predict
    n_out: number of historical days want to use for prediction
    """
    n_vars = 1 if type(data) is list else data.shape[1]
    cols, names = list(), list()
    # input sequence (t-n ... t-1)
    for i in range(n_in, 0, -1):
        cols.append(data.shift(i))
        names += [f'{data.columns[j][3:]}(t-{i})' for j in range(n_vars)]
    # forecast sequence (t, t+1, ... t+n)
    for i in range(0, n_out):
        cols.append(data.shift(-i))
        if i == 0:
            names += [f'{data.columns[j][3:]}(t)' for j in range(n_vars)]
        else:
            names += [f'{data.columns[j][3:]}(t+{i})' for j in range(n_vars)]
    # put it all together
    agg = pd.concat(cols, axis=1)
    agg.columns = names
    # drop rows with NaN values
    agg.dropna(inplace=True)
    return agg


def seq2seq_X_y(data, feature_num, i, lag, look_back, step_ahead):
    train_X = data[(i-look_back):i, :-step_ahead].reshape((look_back, lag, feature_num))
    train_y = data[(i-look_back):i, -step_ahead:]
    return train_X, train_y


def simple_X_y(data, feature_num, i, look_back, step_ahead):
    scaler = MinMaxScaler()
    scaler.fit(data[(i-look_back):i,:])
    train = scaler.transform(data[(i-look_back):i,:])
    test = scaler.transform(data[i:(i+1),:])
    train_X = train[:,:-step_ahead]
    train_y = train[:,-step_ahead:]
    test_X = test[:,:-step_ahead]
    test_y = test[:, -step_ahead:]
    return train_X, test_X, train_y, test_y, scaler


def drop_columns(dataset, ref, focal, horizion):
    drop = []
    for col in ref.columns:
        if col[3:] != focal:
            for t in range(horizion):
                if t == 0:
                    drop.append(f'{col[3:]}(t)')
                else:
                    drop.append(f'{col[3:]}(t+{t})')
    dataset.drop(columns=drop, axis=1, inplace=True)
    return dataset