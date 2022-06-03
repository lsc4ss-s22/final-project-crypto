"""
Author: Shiyang Lai (original code), Peihan Gao (parallel with pyspark)
E-mail: shiyanglai@uchicago.edu
Purpose: Organizing and formalizing data set and parallelizing for analysis usage
"""
import pandas as pd
from pyspark.sql.types import DateType
from pyspark.sql import Window
import pyspark.sql.functions as f
import boto3
from pyspark.sql.functions import col, unix_timestamp, to_date, when, monotonically_increasing_id, coalesce, last, first
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
        dataset=dataset.withColumn("index", monotonically_increasing_id())
        dataset=dataset.withColumn('lg'+_by, f.log('Close'))
        w = Window.orderBy('index')
        returns = dataset.withColumn('lead', f.lag('lg'+_by, 1).over(w)) \
        .withColumn('returns', f.when(f.col('lead').isNotNull(), f.col('lg'+_by) - f.col('lead')).otherwise(f.lit(None))) \
        .where(col('index')!=0)[['Date','returns']]

        if how == 'mixed':
            return returns 
        elif how == 'positive':
            conditions = when(col("returns") > 0, col("returns"))\
                .otherwise(0)
            returns = returns .withColumn("returns", conditions)
            return returns
        elif how == 'negative':
            conditions = when(col("returns") < 0, col("returns"))\
                .otherwise(0)
            returns = returns .withColumn("returns", conditions)
            return returns
        else:
            raise ValueError
        
    def calculate_volatility(self, data):
        h = f.log('High')
        l = f.log('Low')
        c = f.log('Close')
        o = f.log('Open')
        v = 0.511*(h-l)**2 - 0.019*((c-o)*(h+l-2*o)-2*(h-o)*(l-o)) - 0.383*(c-o)**2
        dataset = data.withColumn('volatility', v)
        volatility = dataset[['Date','volatility']]
        return volatility

    def calculate_exchange(self, dataset, _by='Close'):
        exchange = dataset[['Date',_by]]
        return exchange
    
    def preprocess(self, crypto, spark): 
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
        return crypto, conven

    def init_calculate(self,conven,crypto,key_nm,how='mixed',_type='conventional', calculate='returns'):
        if calculate == 'returns':
            if _type=='conventional':
                return self.calculate_returns(conven[key_nm], _by=self._by, how=how).\
                    withColumnRenamed("returns", key_nm)
            else:
                return self.calculate_returns(crypto[key_nm], _by=self._by, how=how).\
                    withColumnRenamed("returns", key_nm)
        elif calculate == 'volatility':
            if _type=='conventional':
                return self.calculate_volatility(conven[key_nm]).withColumnRenamed("volatility", key_nm)
            else:
                return self.calculate_volatility(crypto[key_nm]).withColumnRenamed("volatility", key_nm)
        else:
            if _type=='conventional':
                return self.calculate_exchange(conven[key_nm],_by=self._by).withColumnRenamed(self._by, key_nm)
            else:
                return self.calculate_exchange(crypto[key_nm],_by=self._by).withColumnRenamed(self._by, key_nm)
    
    def get_calculate(self, conven,crypto, how='mixed', _type='conventional', calculate='returns'):
        conven_keys = [k for k in conven.keys()]
        crypto_keys = [k for k in crypto.keys()]
        if _type == 'conventional':
            calculate_df = self.init_calculate(conven=conven,crypto=crypto,key_nm=conven_keys[0],how=how, _type=_type)
            for name in conven_keys[1:]:
                calculate_df = calculate_df.join( \
                    self.init_calculate(conven=conven,crypto=crypto,key_nm=name,how=how, _type=_type,calculate=calculate), on = 'Date')
        else:
            calculate_df = self.init_calculate(conven=conven,crypto=crypto,key_nm=crypto_keys[0],how=how, _type=_type)
            for name in crypto_keys[1:]:
                calculate_df = calculate_df.join( \
                    self.init_calculate(conven=conven,crypto=crypto,key_nm=name,how=how, _type=_type,calculate=calculate), on = 'Date')              
        return calculate_df    
        
    def activate(self,crypto, spark):
        crypto, conven = self.preprocess(crypto, spark)

        types= ['conventional', 'crypto' ]
        hows = ['mixed','positive','negative']
        calculates =['returns', 'volatility', 'exchange']
        
        df_dct = {}
        for calculate in calculates:
            for _type in types:
                if calculate == 'returns':
                    for how in hows:
                        df_dct[_type+calculate+how] = self.get_calculate(conven,crypto, how=how, _type=_type, calculate=calculate)
                else:
                    df_dct[_type+calculate] = self.get_calculate(conven,crypto, _type=_type, calculate=calculate)
                    
        for calculate in calculates:
            if calculate == 'returns':
                for how in hows:  
                    df_dct[calculate+how] = df_dct[types[0]+calculate+how].join(df_dct[types[1]+calculate+how],on='Date', how="inner")
            else:
                df_dct[calculate] = df_dct[types[0]+calculate].join(df_dct[types[1]+calculate],on='Date', how="inner")      
        
        print('Processing complete!')
        return df_dct

    def upload_s3(self,crypto, spark):
        df_dct = self.activate(crypto, spark)
        print('Begin uploading data to s3')
        
        for key, df in df_dct.items():
            df.write.parquet('s3://crpytoconven/project/data/preprocessed/'+key+'.parquet',mode="overwrite")
