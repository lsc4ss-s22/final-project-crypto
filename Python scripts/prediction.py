from utils import *
from sklearn_training import *
from torch_training import *
from tqdm import tqdm


def regression_task(dataset, focal, in_system, model1, model2=None, horizion=1, step_ahead=1, lookback=730, lag=7, **kwargs):
    # get the focal currency for later usage
    focal_currency = dataset['USD'+focal].values
    # for comparasion purpose, I do the training on the whole dataset and the in_system dataset
    whole_dataset = dataset.copy()
    feature_num_full = whole_dataset.shape[1]
    # convert the format of the dataset, using historiy datapoints to predict n step ahead 
    whole_dataset = series_to_supervised(whole_dataset, n_in=lag, n_out=horizion)
    whole_dataset = drop_columns(whole_dataset, dataset, focal, horizion)
    whole_dataset = whole_dataset.values
    
    in_sys_dataset = dataset[in_system]
    feature_num_part = in_sys_dataset.shape[1]
    in_sys_dataset = series_to_supervised(in_sys_dataset, n_in=lag, n_out=horizion)
    in_sys_dataset = drop_columns(in_sys_dataset, dataset[in_system], focal, horizion)
    in_sys_dataset = in_sys_dataset.values
    
    if 'jump' in kwargs.keys():
        jump = kwargs['jump']
    else:
        jump = horizion
        
    # using rolling window strategy to test the performance of the model
    full_loss, part_loss = [], []
    for i in range(lookback, whole_dataset.shape[0]-step_ahead, jump):
        if 'tsai' in str(type(model1)):
            _all = train_torch(whole_dataset, feature_num_full, i, lag, lookback, horizion, model1, step_ahead)
            if model2:
                _part = train_torch(in_sys_dataset, feature_num_part, i, lag, lookback, horizion, model2, step_ahead)
            else:
                _part = train_torch(in_sys_dataset, feature_num_part, i, lag, lookback, horizion, model1, step_ahead)
        else:
            predicted_all, real = train_sklearn(whole_dataset, feature_num_full, i, lag, lookback, horizion, model1, step_ahead)
            predicted_part, _ = train_sklearn(in_sys_dataset, feature_num_part, i, lag, lookback, horizion, model1, step_ahead)
            _all, _part = dynamic_evaluation(real, predicted_all, predicted_part, metrics.mean_absolute_error)
        full_loss.append(_all)
        part_loss.append(_part)
    
    print('Trained successfully!')
    return full_loss, part_loss