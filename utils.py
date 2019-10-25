# -*- coding: UTF-8 -*-
__author__ = 'xun.kang'


import math
import pandas as pd
import numpy as np

'''
子订单金额缺失 填充
'''
def get_orderdetail_amount(tmp,df):
    tmp1 = tmp.groupby(['order_id'],as_index=False).agg({'order_total_payment':'mean',
                                              'order_amount':'mean',
                                              'order_detail_id':'count'})
    tmp1['order_total_payment'] = tmp1.apply(lambda x: round(x['order_total_payment']*1.0/x['order_detail_id'],2), axis=1)
    tmp1['order_amount'] = tmp1.apply(lambda x: round(x['order_amount']*1.0/x['order_detail_id'],2), axis=1)
    del tmp1['order_detail_id']
    tmp1.rename(columns={'order_amount': 'order_amount_mean','order_total_payment':'order_total_payment_mean'}, inplace=True)
    test = df.merge(tmp1,on=['order_id'],how='left')

    tmp2 = test[test['order_amount_mean'].isnull() == False]
    tmp2['order_detail_amount'] = tmp2['order_amount_mean']
    tmp2['order_detail_payment'] = tmp2['order_total_payment_mean']

    tmp3 = test[test['order_amount_mean'].isnull() == True]
    df =pd.concat([tmp2,tmp3])
    del df['order_total_payment_mean']
    del df['order_amount_mean']
    return df

'''
i2u
item:userlist(set去重)
item:usercount
'''
def get_dict(df):
    tmp = df.groupby(['customer_id','goods_id'],as_index=False).agg({'order_detail_id':'count'})
    #item to userlist
    good_users = tmp.groupby('goods_id',as_index=False)['customer_id'].agg({'goods_userlist': lambda x: set(list(x))}).sort_values(by='goods_id',ascending=True)
    #item to usercount
    goods1 = df.groupby(['goods_id'])['customer_id'].nunique().reset_index().sort_values(by='goods_id',ascending=True)
    goods1.rename(columns={'customer_id': 'goods_usercount'}, inplace=True)
    good_users['goods_usercount'] = goods1['goods_usercount']

    item_len = good_users.shape[0]
    return item_len,good_users

'''
sim_matrix
param:sim
'''
def get_sim_matrix(good_users,item_len,sim):
    print 'sim_matrix'
    #sim_matrix
    si = np.zeros((item_len,item_len))
    for i in range(item_len):
        for j in range(item_len):
            if i <= j:
                goodsi = good_users.loc[i]['goods_userlist']
                goodsj = good_users.loc[j]['goods_userlist']
                goodsi_usercount = good_users.loc[i]['goods_usercount']
                goodsj_usercount = good_users.loc[j]['goods_usercount']
                sim_len = len(goodsi & goodsj)
                sim_len_union = len(goodsi | goodsj)
                if sim == 'cos':
                    if goodsi_usercount == 0 or goodsj_usercount == 0 or sim_len == 0:
                        similarity = 0
                    else:
                        similarity = sim_len*1.0 / (math.sqrt(goodsi_usercount*goodsj_usercount))
                elif sim == 'jaccard':
                    similarity = sim_len*1.0/sim_len_union
                else:
                    similarity = sim_len
                si[j][i] = similarity
                si[i][j] = similarity
    return si

'''
PCA,降维 i2u
'''
def get_PCA(si,good_users):
    #pca
    print 'get_pca'
    from sklearn.decomposition import PCA
    pca = PCA(n_components=10)
    item_pca = pca.fit_transform(si)
    main_componet = np.arange(0,10,1)
    pca_columns = ['pca'+str(i+1) for i in main_componet]
#     pca_columns = ['pca1','pca2','pca3','pca4','pca5','pca6','pca7','pca8','pca9','pca10']
    item_pca = pd.DataFrame(item_pca,columns=pca_columns)
    item_pca['freq_goods'] = good_users['goods_id']
#     item_pca['freq_goods'] = item_pca.index
#     item_pca['freq_goods'] = item_pca['freq_goods'].map(lambda x:item2index[x])
    return item_pca

'''
谱聚类,根据相似矩阵聚类 item
'''
from sklearn.cluster import SpectralClustering
from sklearn import metrics
def Clustering(si,good_users):
    si_Cluster = SpectralClustering(n_clusters=3, gamma=0.1).fit_predict(si)
    item_cluster = pd.DataFrame(si_Cluster,columns=['cluster'])
    item_cluster['freq_goods'] = good_users['goods_id']
    return item_cluster

'''
i2u: item:userlist(根据时间排序,不去重,获得购买物品的用户序列)
'''
#item_to_users LDA
def item2index(df):
    print 'get_dict'
    iindex = dict()
    item = df['goods_id'].sort_values(ascending=True).unique()
    itemcount = len(item)
    for i in range(itemcount):
            iindex[i] = item[i]
    return iindex,itemcount

def get_item_to_user(df):
    print 'itu'
    #item_users
    tmp = df.groupby(['goods_id','order_pay_date','customer_id'],as_index=False).agg({'order_detail_id':'count'}).sort_values(by=['goods_id','order_pay_date'],ascending=True)
    tmp = tmp.groupby('goods_id',as_index=False).agg({'customer_id': lambda x: list(x)})
    return tmp

def get_corpus_user(df):
    print 'corpus'
    corpus = []
    for index,row in df.iterrows():
        item_list = ' '.join(map(lambda x:str(x),row['customer_id']))
        corpus.append(item_list)
    return corpus

from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer

'''
LDA 计算 语义topic:
将 每个 item 的user序列当做 document
序列中每个 user 当做 word
'''
def get_LDA_user(corpus,tmp):
    print 'cnt'
    vectorizer = CountVectorizer()
    cntTf = vectorizer.fit_transform(corpus)
    print 'lda'
    lda = LatentDirichletAllocation(n_topics=40,
                                random_state=0)
    print 'fit'
    docres = lda.fit_transform(cntTf)
    topic = np.arange(0,40,1)
    lda_columns = ['lda'+str(i+1) for i in topic]
    print 'df'
    i2u_lda = pd.DataFrame(docres,columns=lda_columns)
    i2u_lda['freq_goods'] = tmp['goods_id']
    return i2u_lda

def most_freq(l):
    freq = dict()
    for item in l:
        if item is None or item=='' or item==np.nan:
            continue
        if item is not None:
            freq.setdefault(item, 0)
            freq[item] += 1
    s = sorted(freq.items(), key=lambda x:x[1], reverse=True)
    if len(s)>0:
        return s[0][0]
    return ''