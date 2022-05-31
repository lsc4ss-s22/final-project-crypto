import numpy as np
import pandas as pd
import glob, os

class DataPipline():
    
    def __init__(self, workspace, start_date, end_date, _by='Close'):
        self.workspace = workspace
        self.start_date = start_date
        self.end_date = end_date
        self._by = _by

    def read_currencies(self, folder_path, _type='conventional'):
        datasets = {}
        if _type == 'conventional':
            files = glob.glob(os.path.join(folder_path, "*.csv"))
            for file in files:
                name = ''.join(file.split('/')[-1].split('.')[0])
                datasets[name] = pd.read_csv(file)
        else:
            files = glob.glob(os.path.join(folder_path, "*.xlsx"))
            for file in files:
                name = file.split('/')[-1].split(' ')[0] + 'USD'
                datasets[name] = pd.read_excel(file)
        return datasets
    
    def fill_dataset(self, dataset, start_date, end_date):
        dataset.Date = pd.to_datetime(dataset.Date)
        dataset = dataset[(dataset.Date >= start_date) & (dataset.Date <= end_date)]
        dates = pd.date_range(str(start_date), str(end_date)).to_list()
        dataset.set_index('Date', inplace=True)
        new_dataset = pd.DataFrame(columns=dataset.columns, index=dates)
        for index, row in dataset.iterrows():
            new_dataset.loc[index, :] = dataset.loc[index, :]
        new_dataset.fillna(method='bfill', inplace=True)
        new_dataset.fillna(method='ffill', inplace=True)
        new_dataset.reset_index(inplace=True)
        new_dataset.rename(columns={'index': 'Date'}, inplace=True)
        return new_dataset
    
    def unify_unit(self, dataset, set_name):
        if set_name[-3:] == 'USD':
            for col in ['Open', 'Close', 'High', 'Low']:
                dataset[col] = 1/dataset[col]
            set_name = 'USD' + set_name[:-3]
        return dataset, set_name
    
    def calculate_returns(self, dataset, _by='Close', how='mixed'):
        returns = pd.Series(index=[dataset.Date.values[1:]])
        for index, row in dataset.iterrows():
                if index != 0:
                    today = dataset.loc[index, _by]
                    yesterday = dataset.loc[index-1, _by]
                    returns.loc[row['Date']] = np.log(today) - np.log(yesterday)

        if how == 'mixed':
            return returns
        elif how == 'positive':
            return pd.Series(data=[a if a > 0 else 0 for a in returns],
                             index=[dataset.Date.values[1:]])
        elif how == 'negative':
            return pd.Series(data=[a if a < 0 else 0 for a in returns],
                             index=[dataset.Date.values[1:]])
        else:
            raise ValueError
    
    def calculate_volatility(self, dataset):
        volatility = pd.Series(index=dataset.Date.values)
        for index, row in dataset.iterrows():
            h = np.log(row['High'])
            l = np.log(row['Low'])
            c = np.log(row['Close'])
            o = np.log(row['Open'])
            v = 0.511*(h-l)**2 - 0.019*((c-o)*(h+l-2*o)-2*(h-o)*(l-o)) - 0.383*(c-o)**2
            volatility.loc[row['Date']] = v
        return volatility
    

    def activate(self):
        crypto = self.read_currencies(self.workspace + "Crypto Data", _type='crypto')
        conven = self.read_currencies(self.workspace + "Conven Data", _type='conventional')
        origional_keys = list(conven.keys()).copy()
        for set_name in origional_keys:
            conven[set_name] = self.fill_dataset(conven[set_name], self.start_date, self.end_date)
            dataset, new_set_name = self.unify_unit(conven[set_name], set_name)
            if set_name != new_set_name:
                del conven[set_name]
                conven[new_set_name] = dataset
        origional_keys = list(crypto.keys()).copy()
        for set_name in origional_keys:
            crypto[set_name] = self.fill_dataset(crypto[set_name], self.start_date, self.end_date)
            dataset, new_set_name = self.unify_unit(crypto[set_name], set_name)
            if set_name != new_set_name:
                del crypto[set_name]
                crypto[new_set_name] = dataset
                
        conv_returns = []
        for name in conven.keys():
            conv_returns.append(self.calculate_returns(conven[name], _by=self._by, how='mixed'))
        conv_returns = pd.concat(conv_returns, axis=1)
        conv_returns.columns = conven.keys()
        conv_returns.to_csv(self.workspace + 'Exp Data/v2/conv_returns.csv')
        
        cryp_returns = []
        for name in crypto.keys():
            cryp_returns.append(self.calculate_returns(crypto[name], _by=self._by, how='mixed'))
        cryp_returns = pd.concat(cryp_returns, axis=1)
        cryp_returns.columns = crypto.keys()
        cryp_returns.to_csv(self.workspace + 'Exp Data/v2/crypto_returns.csv')
        
        returns = pd.merge(cryp_returns, conv_returns, how='inner', left_index=True, right_index=True)
        returns.to_csv(self.workspace + 'Exp Data/v2/returns.csv')

        conv_returns = []
        for name in conven.keys():
            conv_returns.append(self.calculate_returns(conven[name], _by=self._by, how='positive'))
        conv_returns = pd.concat(conv_returns, axis=1)
        conv_returns.columns = conven.keys()
        conv_returns.to_csv(self.workspace + 'Exp Data/v2/conv_preturns.csv')
        
        cryp_returns = []
        for name in crypto.keys():
            cryp_returns.append(self.calculate_returns(crypto[name], _by=self._by, how='positive'))
        cryp_returns = pd.concat(cryp_returns, axis=1)
        cryp_returns.columns = crypto.keys()
        cryp_returns.to_csv(self.workspace + 'Exp Data/v2/crypto_preturns.csv')
        
        returns = pd.merge(cryp_returns, conv_returns, how='inner', left_index=True, right_index=True)
        returns.to_csv(self.workspace + 'Exp Data/v2/preturns.csv')

        conv_returns = []
        for name in conven.keys():
            conv_returns.append(self.calculate_returns(conven[name], _by=self._by, how='negative'))
        conv_returns = pd.concat(conv_returns, axis=1)
        conv_returns.columns = conven.keys()
        conv_returns.to_csv(self.workspace + 'Exp Data/v2/conv_nreturns.csv')

        cryp_returns = []
        for name in crypto.keys():
            cryp_returns.append(self.calculate_returns(crypto[name], _by=self._by, how='negative'))
        cryp_returns = pd.concat(cryp_returns, axis=1)
        cryp_returns.columns = crypto.keys()
        cryp_returns.to_csv(self.workspace + 'Exp Data/v2/crypto_nreturns.csv')
        
        returns = pd.merge(cryp_returns, conv_returns, how='inner', left_index=True, right_index=True)
        returns.to_csv(self.workspace + 'Exp Data/v2/nreturns.csv')
        
        conv_volat = []
        for name in conven.keys():
            conv_volat.append(self.calculate_volatility(conven[name]))
        conv_volat = pd.concat(conv_volat, axis=1)
        conv_volat.columns = conv_volat.keys()
        conv_volat.to_csv(self.workspace + 'Exp Data/v2/conv_volatility.csv')

        cryp_volat = []
        for name in crypto.keys():
            cryp_volat.append(self.calculate_volatility(crypto[name]))
        cryp_volat = pd.concat(cryp_volat, axis=1)
        cryp_volat.columns = cryp_volat.keys()
        cryp_volat.to_csv(self.workspace + 'Exp Data/v2/crypto_volatility.csv')
        
        volat = pd.merge(cryp_volat, conv_volat, how='inner', left_index=True, right_index=True)
        volat.to_csv(self.workspace + 'Exp Data/v2/volatility.csv')
        
        conv_ex = []
        for name in conven.keys():
            conv_ex.append(conven[name][self._by])
        conv_ex = pd.concat(conv_ex, axis=1)
        conv_ex.columns = conven.keys()
        conv_ex.to_csv(self.workspace + 'Exp Data/v2/conv_exchange.csv')

        cryp_ex = []
        for name in crypto.keys():
            cryp_ex.append(crypto[name][self._by])
        cryp_ex = pd.concat(cryp_ex, axis=1)
        cryp_ex.columns = crypto.keys()
        cryp_ex.to_csv(self.workspace + 'Exp Data/v2/crypto_exchange.csv')
        
        ex = pd.merge(cryp_ex, conv_ex, how='inner', left_index=True, right_index=True)
        ex.to_csv(self.workspace + 'Exp Data/v2/exchange.csv')

        
        print('SUCCESS!')