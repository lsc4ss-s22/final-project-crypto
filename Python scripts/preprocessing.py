"""
Author: Shiyang Lai, Peihan Gao
E-mail: shiyanglai@uchicago.edu
Purpose: Organizing and formalizing data set and parallelizing for analysis usage
"""
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql.types import DateType
from pyspark.sql import Window
from pyspark.sql.functions import coalesce, last, first
import boto3
import io
from io import StringIO 

class DataPipline():
    
    def __init__(self, start_date, end_date, _by='Close') :
        self.start_date = start_date
        self.end_date = end_date
        self._by = _by

    def read_currencies(self, spark):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('crpytoconven')
        prefix_objs = bucket.objects.filter(Prefix="project/data/forex", )
        datasets = {}
        for obj in prefix_objs:
            if obj.key.endswith('.csv'):
                name = obj.key.split('/')[-1].split('.')[0].split('_')[2].upper()
                datasets[name] = spark.read.csv('s3://crpytoconven/'+ obj.key,header=True, sep=';')
        return datasets
    
    def fill_dataset(self, dataset, spark, start_date, end_date,_type="crpyto"):
        if _type=="crpyto":
            dataset = dataset.withColumn('Date', 
                   to_date(unix_timestamp(dataset['Date'], 'yyyy-MM-dd').cast("timestamp")))
        else:
            dataset = dataset.withColumn('Date', 
                   to_date(unix_timestamp(dataset['Date'], 'MM/dd/yyyy').cast("timestamp")))

        dataset = dataset.filter((dataset.Date >= start_date) & (dataset.Date <= end_date))
        dates = pd.date_range(str(start_date), str(end_date)).to_list()
        dates= [t.to_pydatetime() for t in dates]
        new_dataset = spark.createDataFrame(dates, DateType())
        new_dataset = new_dataset.withColumnRenamed("value","Date") 
        new_dataset = new_dataset.join(dataset, on='Date', how='left')
        w1 = Window.orderBy('Date')
        w2 = w1.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        cols = (set(new_dataset.columns)-set(['Date']))
        new_dataset = new_dataset.select([ c for c in new_dataset.columns if c not in cols ] + \
        [coalesce(last(c,True).over(w1), first(c,True).over(w2)).alias(c) for c in cols])

        for colu in (set(new_dataset.columns)-set(['Symbol','Date'])):
            new_dataset=new_dataset.withColumn(colu, new_dataset[colu].cast('double')) 
        return new_dataset
    
    def unify_unit(self,dataset, set_name):
        if set_name[-3:] == 'USD':
            for colu in ['Open', 'Close', 'High', 'Low']:
                dataset = dataset.withColumn(colu, 1/dataset[colu])
            set_name = 'USD' + set_name[:-3]
        return dataset, set_name
    
    def calculate_returns(self, dataset, _by='Close', how='mixed'):
        pandasDF = dataset.toPandas()
        returns = pd.Series(index=[pandasDF.Date.values[1:]])

        for index, row in pandasDF.iterrows():
            if index != 0:
                today = pandasDF.loc[index, _by]
                yesterday = pandasDF.loc[index-1, _by]
                returns.loc[row['Date']] = np.log(today) - np.log(yesterday)
                
        if how == 'mixed':
            return returns 
        elif how == 'positive':
            return pd.Series(data=[a if a > 0 else 0 for a in returns],
                            index=[pandasDF.Date.values[1:]])
        elif how == 'negative':
            return pd.Series(data=[a if a < 0 else 0 for a in returns],
                            index=[pandasDF.Date.values[1:]])
        else:
            raise ValueError
    
    def calculate_volatility(self, dataset):
        pandasDF = dataset.toPandas()
        volatility = pd.Series(index=pandasDF.Date.values)
        for _, row in pandasDF.iterrows():
            h = np.log(row['High'])
            l = np.log(row['Low'])
            c = np.log(row['Close'])
            o = np.log(row['Open'])
            v = 0.511*(h-l)**2 - 0.019*((c-o)*(h+l-2*o)-2*(h-o)*(l-o)) - 0.383*(c-o)**2
            volatility.loc[row['Date']] = v
        return volatility
    
    def process(self, crypto, spark): 
        conv_returns = []
        conv_preturns = []
        conv_nreturns = []
        conv_volatility = []
        conv_exchange = []
        crypto_returns = []
        crypto_preturns = []
        crypto_nreturns = []
        crypto_volatility = []
        crypto_exchange = []
        origional_keys = list(crypto.keys()).copy()
        for set_name in origional_keys:
            crypto[set_name] = self.fill_dataset(crypto[set_name], spark, self.start_date, self.end_date, _type="crpyto")
            dataset, new_set_name = self.unify_unit(crypto[set_name], set_name)
            if set_name != new_set_name:
                del crypto[set_name]
                crypto[new_set_name] = dataset

        conven = self.read_currencies(spark=spark)
        origional_keys = list(conven.keys()).copy()
        for set_name in origional_keys:
            conven[set_name] = self.fill_dataset(conven[set_name], spark, self.start_date, self.end_date,_type="conven")
            dataset, new_set_name = self.unify_unit(conven[set_name], set_name)
            if set_name != new_set_name:
                del conven[set_name]
                conven[new_set_name] = dataset
        
        for name in conven.keys():
            conv_returns.append(self.calculate_returns(conven[name], _by=self._by, how='mixed'))
            conv_preturns.append(self.calculate_returns(conven[name], _by=self._by, how='positive'))
            conv_nreturns.append(self.calculate_returns(conven[name], _by=self._by, how='negative'))
            conv_volatility.append(self.calculate_volatility(conven[name]))
            conv_exchange.append(conven[name].toPandas()[self._by])
        for name in crypto.keys():
            crypto_returns.append(self.calculate_returns(crypto[name], _by=self._by, how='mixed'))
            crypto_preturns.append(self.calculate_returns(crypto[name], _by=self._by, how='positive'))
            crypto_nreturns.append(self.calculate_returns(crypto[name], _by=self._by, how='negative'))
            crypto_volatility.append(self.calculate_volatility(crypto[name]))
            crypto_exchange.append(crypto[name].toPandas()[self._by])
            
        conv_l=[conv_returns, conv_preturns, conv_nreturns, conv_volatility, conv_exchange]
        cryp_l=[crypto_returns,crypto_preturns,crypto_nreturns,crypto_volatility,crypto_exchange] 
        
        df_conv_l = []
        df_cryp_l = []
        agg_l = []

        for con in conv_l:
            conv = pd.concat(con, axis=1)
            conv.columns = conven.keys() 
            df_conv_l.append(conv)

        for cry in cryp_l:
            cryp = pd.concat(cry, axis=1)
            cryp.columns = crypto.keys()          
            df_cryp_l.append(cryp)           
        
        for i in range(5):
            df= pd.merge(df_conv_l[i], df_cryp_l[i], how='inner', left_index=True, right_index=True)
            agg_l.append(df)


        return df_conv_l + df_cryp_l + agg_l

    def upload_s3(self, df_lst):
        s3_resource = boto3.resource('s3')
        bucket = 'crpytoconven' 
        csv_buffer = StringIO()
        
        for c_df in df_lst:
            c_df.to_csv(csv_buffer)
            name =[x for x in globals() if globals()[x] is c_df][0]
            s3_resource.Object(bucket, 'project/data/preprocessed/'+name+'.csv').put(Body=csv_buffer.getvalue())
                                
    def activate(self,crypto, spark):
        df_lst = self.process(crypto, spark)
        self.upload_s3(df_lst)

        print('SUCCESS!')