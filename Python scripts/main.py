import boto3
import argparse
from utils import *
from prediction import regression_task
from visulization import *
from tsai.all import *
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# hyperparameters
LAG = 7                              # number of days that the model will look back for prediction
HORIZION = 1                         # number of days the model will predict at each round
SPLITS = 30                          # number of rounds the model will predict using each time window
LOOK_BACK = 2*365+SPLITS*HORIZION    # number of days within a time window + number of rounds * number of days per round
JUMP = 30                            # number of days that will be shifted (rolling window shifting speed)

start = datetime.now()

returns = read_data(folder_path='../data/', name='returns_cleaned', binary=False)
# returns = returns[returns.index >= '2015-01-01']
cryptos = list(returns.iloc[:, :103].columns)
convens = list(returns.iloc[:, 103:].columns)

if __name__ == '__main__':
    # parse the command lines
    parser = argparse.ArgumentParser(description='Model Training and Evaluation.')
    parser.add_argument('coin', help='the name of the coin.')
    args = parser.parse_args()
    
    print('Start Checking!')
    
    if args.coin in cryptos:
        feature_num1 = len(cryptos+convens)
        feature_num2 = len(cryptos)
        in_sys = cryptos
    elif args.coin in convens:
        feature_num1 = len(cryptos+convens)
        feature_num2 = len(convens)
        in_sys = convens
    else:
        raise ValueError
        

    bilstm_1 = LSTM(LAG, HORIZION, feature_num1, n_layers=3, bias=True, bidirectional=True)
    bilstm_2 = LSTM(LAG, HORIZION, feature_num2, n_layers=3, bias=True, bidirectional=True)

    resnet_1 = ResNet(LAG, HORIZION)
    resnet_2 = None

    xcm_1 = XCM(LAG, HORIZION, feature_num1)
    xcm_2 = XCM(LAG, HORIZION, feature_num2)

    mlstm_fcn_1 = MLSTM_FCN(LAG, HORIZION, feature_num1)
    mlstm_fcn_2 = MLSTM_FCN(LAG, HORIZION, feature_num2)

    gmlp_1 = gMLP(LAG, HORIZION, feature_num1)
    gmlp_2 = gMLP(LAG, HORIZION, feature_num2)

    tst_1 = TST(LAG, HORIZION, feature_num1)
    tst_2 = TST(LAG, HORIZION, feature_num2)

    task = [('BiLSTMs', bilstm_1, bilstm_2), ('MLSTM_FCN', mlstm_fcn_1, mlstm_fcn_2), ('ResNet', resnet_1, resnet_2),
        ('gMLP', gmlp_1, gmlp_2), ('XCM', xcm_1, xcm_2), ('TST', tst_1, tst_2)]
    
    print('Start Training!')
    
    for model_name, model1, model2 in task:
        print(f'Start {model_name} training.')
        loss_all, loss_part = regression_task(returns, args.coin[3:], in_sys, model1, model2, HORIZION, SPLITS, LOOK_BACK, LAG, jump=JUMP)

        df = pd.DataFrame({'window_index': [i for i in range(len(loss_all))] * 2, 'MAE': loss_all + loss_part,
                        'type': ['Within- & Cross-Market' for i in range(len(loss_all))] + ['Within-Market' for i in range(len(loss_all))]})
        df.to_csv(f'../result_{args.coin[3:]}_{model_name}.csv')
    
    print(f'Job Finished! The total running time is {datetime.now()-start}')
