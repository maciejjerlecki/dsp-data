
# coding: utf-8

# In[2]:


import os, sys
if 'ipykernel' in sys.argv[0]:
    from IPython.core.display import display, HTML
    display(HTML("<style>.container { width:90% !important; }</style>"))
    get_ipython().magic('load_ext sparkver')
    get_ipython().magic('sparkver')


# In[3]:


APP_NAME = 'piekny notebook'
QUEUE = 'ads_data'
EXECUTOR_MEMORY = '8g'
EXECUTOR_OVERHEAD = 2048
EXECUTOR_CORES = 8
EXECUTORS = 50
DRIVER_MEMORY = '8g'

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from time import time, strftime
from datetime import datetime, timedelta
import datetime
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark import StorageLevel

spark = (SparkSession.builder
    .appName(APP_NAME)
    .config('spark.yarn.queue', QUEUE)
    .config('spark.executor.memory', EXECUTOR_MEMORY)
    .config('spark.executor.cores', EXECUTOR_CORES)
    .config('spark.dynamicAllocation.enabled','true')
    .config('spark.dynamicAllocation.initialExecutors', 8)
    .config('spark.dynamicAllocation.minExecutors', 1)
    .config('spark.dynamicAllocation.maxExecutors', EXECUTORS)
    .config('spark.dynamicAllocation.schedulerBacklogTimeout', '3s')
    .config('spark.dynamicAllocation.executorIdleTimeout', '120s')
    .config('spark.shuffle.service.enabled','true')
    .config('spark.shuffle.service.port', 7337)
    .config('spark.yarn.executor.memoryOverhead', EXECUTOR_OVERHEAD)
    .config('spark.yarn.appMasterEnv.JAVA_HOME', '/opt/jre1.8.0')
    .config('spark.executorEnv.JAVA_HOME', '/opt/jre1.8.0')
    .config('spark.speculation', 'true')
    .config('spark.driver.memory', DRIVER_MEMORY)
    .config('spark.driver.maxResultSize', '20g')
    .enableHiveSupport()
    .getOrCreate())
print('Application master: http://rm.hadoop.qxlint:8088/proxy/%s' % spark.sparkContext.applicationId)


# In[4]:


import pandas as pd
import numpy as np


# In[5]:


import requests
print(pd.__version__)


# In[6]:


yest=(datetime.datetime.now()+timedelta(days=-1)).strftime("%Y-%m-%d")


# In[7]:


pd.set_option('display.max_colwidth', 99999)
pd.set_option('display.max_columns', 999)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


# In[34]:


emis = (spark.table('datahub.pl_allegro_advertising_ppc_emission_emitted_2')
        .where(col('dt')==yest)
        .where(size(col('emission_units'))>1)
        .withColumn('emis_explode', explode(col('emission_units')))
        .select(col('emission_id')
                , col('emis_explode.emission_unit_id')
                , col('emis_explode.debug_info')['ctr'].alias('ctr')
                , col('emis_explode.debug_info')['global_ctr'].alias('global_ctr')
                , col('emis_explode.debug_info')['quality_score'].alias('quality_score')
                , col('emis_explode.debug_info')['ad_rank'].alias('ad_rank')
                , col('emis_explode.debug_info')['relevance'].alias('relevance')
            )
        .withColumn('x', (col('global_ctr')/lit(0.02)) * (1+1*col('relevance')))
        .limit(10000000) ## biore ok 1% wszystkich emission unitow z dnia
       )


# In[35]:


emis.printSchema()


# In[36]:


emis.limit(3).toPandas()


# In[37]:


emis.count()


# In[38]:


spark.sql("""DROP TABLE IF EXISTS ads_data.rnbw2_raw_data""")


# In[39]:


emis.write.saveAsTable('ads_data.rnbw2_raw_data')


# In[ ]:


# ## sciagawka

# mda = (spark.table('ads_prod.mongo_dump_accounts_parquet')
#        .where(col('v_date')==yest)
#        .where(col('hour')=='23')
#        .withColumn('clients_explode', explode('clients'))
#        .select(col('clients_explode')['id'].alias('client_id')
#                , col('clients_explode')['sellerid'].alias('account_id')
#                , col('clients_explode')['name'].alias('login')
#               )
#       )

# client_stats = (spark.table('ads_prod.client_stats')
#                 .where(col('v_date').between(fromDate, toDate))
#                 .withColumn('mth', substring(col('v_date'),0,7))
#                 .groupBy('client_id', 'mth')
#                 .agg(sum('total_cost').alias('reve_core_gross'))
#                )

# client_gmv = (spark.table('ads_prod.client_gmv')
#               .where(col('date').between(fromDate, toDate))
#               .withColumn('mth', substring(col('date'),0,7))
#               .groupBy(col('key')['client_id'].alias('client_id'), 'mth')
#               .agg(sum(col('aggregation')['direct_transactions_value'] 
#                        + col('aggregation')['indirect_transactions_value']).alias('gmv_core'))
#              )

# premium_stats = (spark.table('ads_prod.premium_inventory_unit_stats')
#               .where(col('v_date').between(fromDate, toDate))
#               .withColumn('mth', substring(col('v_date'),0,7))
#               .groupBy('client_id', 'mth')
#               .agg(sum('total_cost').alias('reve_premium_gross'))
#              )

# premium_gmv = (spark.table('ads_prod.client_premium_aggregated_attribution_gmv')
#             .where(col('v_date').between(fromDate, toDate))
#             .withColumn('mth', substring(col('v_date'),0,7))
#             .groupBy(col('client')['client_id'].alias('client_id'), 'mth')
#             .agg(sum(col('value')['post_click_value'] + col('value')['post_view_value']).alias('gmv_premium'))
#            )

