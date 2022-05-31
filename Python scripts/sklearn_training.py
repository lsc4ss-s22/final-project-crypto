from utils import *
import numpy as np


def train_sklearn(dataset, feature_num, i, lag, lookback, horizion, model, step_ahead):
    # split the data set
    train_X, test_X, train_y, test_y, scaler = simple_X_y(dataset, feature_num, i, lookback, horizion)
    trained_model = model.fit(train_X, train_y)
    prediction = []
    real = []
    for step in range(step_ahead):
        train_X, test_X, train_y, test_y, scaler = simple_X_y(dataset, feature_num, i+step, lookback, horizion)
        predicted_y = trained_model.predict(test_X)
        prediction.append(scaler.inverse_transform(np.concatenate((test_X, predicted_y), axis=None).reshape(1,-1))[0][-1])
        real.append(scaler.inverse_transform(np.concatenate((test_X, test_y), axis=None).reshape(1,-1))[0][-1])
    return prediction, real