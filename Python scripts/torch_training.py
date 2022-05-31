from tsai.all import *
from IPython.display import clear_output
from utils import *

def train_torch(dataset, feature_num, i, lag, lookback, horizion, model, step_ahead):
    X, y = seq2seq_X_y(dataset, feature_num, i, lag, lookback, horizion)
    # to TSTensor
    X, y = TSTensor(X), TSTensor(y)
    # create the model
    # model = LSTM(LAG, HORIZION, hidden_size=100, n_layers=2, bias=True, rnn_dropout=0.2, bidirectional=True)
    # learner
    batch_tfms = TSNormalize()
    splits = TSSplitter(step_ahead)(y)
    dls = get_ts_dls(X, y, splits=splits, batch_tfms=batch_tfms)
    learner = ts_learner(dls, model, metrics=mae, verbose=False)
    # fit
    learner.fit_one_cycle(20, 1e-2)
    clear_output(wait=True)
    # return results
    return learner.final_record[-1]