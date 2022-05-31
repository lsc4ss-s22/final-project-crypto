from audioop import getsample
from utils import *
from prediction import regression_task
from visulization import *
from tsai.all import *
from ray import tune
from ray.tune.schedulers import ASHAScheduler
import warnings
warnings.filterwarnings('ignore')

FOCAL = 'BTC'
FEATURE_NUM = 14
LAG = 7
HORIZION = 1
SPLITS=30
LOOK_BACK=3*365+SPLITS
JUMP=30


exchanges = read_data(folder_path='data/v2/', name='exchange', binary=False)
exchanges = exchanges[exchanges.index >= '2015-01-01']
cryptos = ['USDBTC', 'USDDOGE', 'USDETH', 'USDLTC', 'USDXRP']
convens = ['USDEUR', 'USDJPY', 'USDGBP', 'USDAUD', 'USDCAD', 'USDCHF', 'USDCNY', 'USDSEK', 'USDNZD']
exchanges = exchanges[cryptos+convens]


# hyperparameter tunning
# LSTM
# search_space = {
#     "hidden_size": tune.grid_search([50, 100, 200]),
#     "n_layers": tune.grid_search([1, 2 ,3, 4]),
#     "rnn_dropout": tune.grid_search([0, 0.2, 0.4, 0.8])
# }

# TST
search_space = {
    "n_layers": tune.grid_search([1, 2 ,3]),
    "d_model": tune.grid_search([64, 128, 256]),
    "n_heads": tune.grid_search([2, 4, 8, 16]),
    "dropout": tune.grid_search([0, 0.1, 0.2]),
    "fc_dropout": tune.grid_search([0, 0.1, 0.2])
}

# XCM
# search_space = {
#     "concat_pool": tune.grid_search([False, True]),
#     "fc_dropout": tune.grid_search([0, 0.1, 0.2, 0.3])
# }

# gMLP
# search_space = {
#     "d_model": tune.grid_search([128, 256]),
#     "d_ffn": tune.grid_search([128, 256, 512]),
#     "depth": tune.grid_search([2, 6, 10])
# }

# ResNet
# search_space = {
#     "nf": tune.grid_search([64, 128, 256]),
#     "fc_dropout": tune.grid_search([0, 0.1, 0.2])
# }

def tunning_job(config):
    model1 = TST(LAG, HORIZION, 14, n_layers=config['n_layers'], d_model=config['d_model'],
                 n_heads=config['n_heads'], dropout=config['dropout'], fc_dropout=config['fc_dropout'])
    model2 = TST(LAG, HORIZION, 5, n_layers=config['n_layers'], d_model=config['d_model'],
                 n_heads=config['n_heads'], dropout=config['dropout'], fc_dropout=config['fc_dropout'])

    loss_all, loss_part  = regression_task(exchanges, 'BTC', cryptos, model1, model2, HORIZION, SPLITS, LOOK_BACK, LAG, jump=JUMP)
    tune.report(mean_mae_withc=np.mean(loss_all), mean_mae_withoutc=np.mean(loss_part))

asha_scheduler = ASHAScheduler(time_attr="training_iteration", metric="mean_mae_withc", mode="min")

analysis = tune.run(tunning_job, config=search_space, name='TST', scheduler=asha_scheduler,
                    local_dir="/project/graziul/ra/shiyanglai/experiment3/Crypto-Conven-Spillovers/tune",
                    stop={'training_iteration': 300}, max_failures=10, verbose=1, resume="AUTO",
                    resources_per_trial={"cpu": 4, "gpu": 1})