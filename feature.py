# -*- coding: UTF-8 -*-
__author__ = 'xun.kang'

from utils import *

def make_feature_and_label(date1,date2,isSubmit):
    print 'user data...'
    date1['count'] = 1
##用户特征
#1.订单数量
#2.是否会员
    customer_id = date1.groupby(['customer_id'],as_index=False).agg({'count':'count','member_id':most_freq})
    customer_id['is_member'] = customer_id['member_id'] > 0
    del customer_id['member_id']
#2.性别 ohe
    user_gender = date1.groupby(['customer_id'],as_index=False)['customer_gender'].agg({'customer_gender':most_freq})
    customer_id = customer_id.join(pd.get_dummies(user_gender.customer_gender, prefix='gender'))
#3.省份(最近出现的)
    tmp = date1.groupby(['customer_id'])['customer_province'].last().reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#4.城市
    tmp = date1.groupby(['customer_id'])['customer_city'].last().reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#5.用户有评价的子订单数目
    tmp = date1[date1['is_customer_rate']==1].groupby(['customer_id'])['count'].agg({'is_customer_rate_0':'count'}).reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#6.用户无评价的子订单数目
    tmp = date1[date1['is_customer_rate']==0].groupby(['customer_id'])['count'].agg({'is_customer_rate_1':'count'}).reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#7.有会员折扣的子订单数目
    tmp = date1[(date1['is_member_actived']==1) & (date1['goods_has_discount']==1)].groupby(['customer_id'])['count'].agg({'is_customer_have_discount_count':'count'}).reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')

#商品特征
    print 'goods data...'
#8.统计这个用户购买商品的价格信息
    print 'goods data...'
    tmp = date1.groupby(['customer_id'],as_index=False)['goods_price'].agg({'goods_price_max':'max',
                                                                                    'goods_price_min':'min',
                                                                                    'goods_price_mean':'mean'})
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#9.有折扣的子订单数
    tmp = date1[date1['goods_has_discount']==1].groupby(['customer_id'])['count'].agg({'goods_has_discount_counts':'count'}).reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#10.无折扣的子订单数
    tmp = date1[date1['goods_has_discount']==0].groupby(['customer_id'])['count'].agg({'goods_has_not_discount_counts':'count'}).reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')

#11.商品状态在售中的子订单
    tmp = date1[date1['goods_status']==1].groupby(['customer_id'])['count'].agg({'goods_status_1':'count'}).reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#12.商品状态库中子订单
    tmp = date1[date1['goods_status']==2].groupby(['customer_id'])['count'].agg({'goods_status_2':'count'}).reset_index()
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')

##父订单维度
    print 'order data...'
#13.订单折扣金额
    tmp = date1.groupby(['customer_id'],as_index=False)['order_total_discount'].agg({'order_total_discount_sum':'sum'})
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#14.订单状态（不应该用最大值，而是最常见的值）
    tmp = date1.groupby(['customer_id'],as_index=False)['order_status'].agg({'order_status_most':most_freq})
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')

##子订单维度
    print 'order detail data...'
#15.子订单折扣
    tmp = date1.groupby(['customer_id'],as_index=False)['order_detail_discount'].agg({'order_detail_discount_sum':'sum'})
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#16.子订单状态
    tmp = date1.groupby(['customer_id'],as_index=False)['order_detail_status'].agg({'order_detail_status_most':most_freq})
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#17.订单时间（一年中的第几天）
    date1['order_pay_dayofyear'] = date1['order_pay_time'].dt.dayofyear
    tmp = date1.groupby(['customer_id'],as_index=False)['order_pay_dayofyear'].agg({'order_pay_dayofyear_max':'max','order_pay_dayofyear_min':'min'})
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')
#18.最近订单与最远订单差距时间
    customer_id['order_pay_dayofyear_gap'] = customer_id['order_pay_dayofyear_max'] - customer_id['order_pay_dayofyear_min'] + 1

##统计特征
    print 'statictics data...'
#19.用户购买商品种类（不同ID不同种类）
    customer_id['user_goods_count'] = date1.groupby(['customer_id'],as_index=False)['goods_id'].nunique()

#20.用户平均购买商品个数
    tmp = date1.groupby(['customer_id'],as_index=False)['order_detail_goods_num'].agg({'user_good_amount_max':'max','user_good_amount_min':'min','user_good_amount_mean':'mean'})
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')

#21.用户购买订单的天数
    customer_id['user_order_day'] = date1.groupby(['customer_id'],as_index=False)['order_pay_date'].nunique()
#22.用户在不同天购买了订单，rebuy 行为
    customer_id['user_order_rebuy_day'] = customer_id['user_order_day']>1
#23.用户购买订单数
    customer_id['user_order_count'] = date1.groupby(['customer_id'],as_index=False)['order_detail_id'].nunique()
#24.用户1天购买订单数（perday）
    customer_id['user_order_perday'] = customer_id.apply(lambda x: round(x['user_order_count']*1.0/x['user_order_day'],2), axis=1)
#25.最近购买时间离年底多久
    customer_id['order_pay_date_last'] = pd.to_datetime('2013-12-31').dayofyear - customer_id['order_pay_dayofyear_max'] + 1

#26.用户最常购买的商品 id
    tmp = date1.groupby(['customer_id'],as_index=False)['goods_id'].agg({'freq_goods':most_freq})
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')

#27.商品的购买人数
    goods1 =date1.groupby(['goods_id'])['customer_id'].nunique().reset_index()
    goods1.rename(columns={'goods_id': 'freq_goods'}, inplace=True)
    goods1.rename(columns={'customer_id': 'goods_customer'}, inplace=True)

#28.商品的购买天数
    goods2 = date1.groupby(['goods_id'])['order_pay_date'].nunique().reset_index()
    goods1['goods_day'] = goods2['order_pay_date']

    print '29'
#29.商品的订单数
    goods3 = date1.groupby(['goods_id'])['order_detail_id'].nunique().reset_index()
    goods1['goods_count'] = goods3['order_detail_id']

    print '30'
#30.商品在不同天被下了订单,rebuy行为
    goods = date1.groupby('goods_id',as_index=False)['month-day'].nunique()
    goods1['goods_order_rebuy_day'] = goods.map(lambda x : 1 if x>1 else 0)
    customer_id = customer_id.merge(goods1,on=['freq_goods'],how='left')


    print '31'
#31.商品月度订单数（agg）、商品月度用户数（agg）
    tmp = date1.groupby(['goods_id','month'],as_index=False).agg({'order_detail_id':'count'})
    goods_tmp = date1.groupby(['goods_id','month'],as_index=False)['customer_id'].nunique().reset_index()
    tmp['customer_id'] = goods_tmp['customer_id']
    tmp.rename(columns={'goods_id': 'freq_goods'}, inplace=True)

    tmp_count = tmp.groupby(['freq_goods'],as_index=False)['order_detail_id'].agg({'goods_monthly_count_mean':'mean',
                                                                          'goods_monthly_count_max':'max',
                                                                          'goods_monthly_count_median':'median',
                                                                         'goods_monthly_count_std':'std'})

    tmp_user = tmp.groupby(['freq_goods'],as_index=False)['customer_id'].agg({'goods_monthly_user_mean':'mean',
                                                                          'goods_monthly_user_max':'max',
                                                                          'goods_monthly_user_median':'median',
                                                                         'goods_monthly_user_std':'std'})

    tmp_count['goods_monthly_count_std'].fillna(0.0,inplace=True)
    tmp_user['goods_monthly_user_std'].fillna(0.0,inplace=True)
    customer_id = customer_id.merge(tmp_count,on=['freq_goods'],how='left')
    customer_id = customer_id.merge(tmp_user,on=['freq_goods'],how='left')

#32.用户monthly订单数（agg）、用户monthly订单天数（agg）
#用户月度订单数、月度订单天数
    tmp = date1.groupby(['customer_id','month'],as_index=False).agg({'order_detail_id':'count'}).sort_values(by=['customer_id','month'],ascending=True)
    unique_tmp = date1.groupby(['customer_id','month'],as_index=False)['month-day'].nunique().reset_index()
    tmp['month-day'] = unique_tmp['month-day']
    tmp_count = tmp.groupby(['customer_id'],as_index=False)['order_detail_id'].agg({'monthly_count_mean':'mean',
                                                                          'monthly_count_max':'max',
                                                                          'monthly_count_median':'median',
                                                                         'monthly_count_std':'std'}).sort_values(by=['customer_id'],ascending=True)

    tmp_day = tmp.groupby(['customer_id'],as_index=False)['month-day'].agg({'monthly_day_mean':'mean',
                                                                          'monthly_day_max':'max',
                                                                          'monthly_day_median':'median',
                                                                         'monthly_day_std':'std'}).sort_values(by=['customer_id'],ascending=True)

    tmp_count['monthly_count_std'].fillna(0.0,inplace=True)
    tmp_day['monthly_day_std'].fillna(0.0,inplace=True)
    customer_id = customer_id.merge(tmp_count,on=['customer_id'],how='left')
    customer_id = customer_id.merge(tmp_day,on=['customer_id'],how='left')

    print '33'
#33.用户monthly商品数（agg）
    unique_tmp = date1.groupby(['customer_id','month'],as_index=False)['goods_id'].nunique().reset_index()
    tmp['goods_id'] = unique_tmp['goods_id']
    tmp_goods = tmp.groupby(['customer_id'],as_index=False)['goods_id'].agg({'monthly_goods_mean':'mean',
                                                                          'monthly_goods_max':'max',
                                                                          'monthly_goods_median':'median',
                                                                         'monthly_goods_std':'std'}).sort_values(by=['customer_id'],ascending=True)

    customer_id = customer_id.merge(tmp_goods,on=['customer_id'],how='left')


    #交叉特征
#34.<用户-物品>购买天数、购买订单数
    tmp = date1.groupby(['customer_id','goods_id'],as_index=False).agg({'order_detail_id':'count'})
    goods_tmp = date1.groupby(['customer_id','goods_id'],as_index=False)['month-day'].nunique().reset_index()
    tmp['month-day'] = goods_tmp['month-day']

    #对用户 （agg）
    tmp_usergoodcount = tmp.groupby(['customer_id'],as_index=False)['order_detail_id'].agg({'userGoods_count_mean':'mean',
                                                                          'userGoods_count_max':'max',
                                                                          'userGoods_count_median':'median',
                                                                         'userGoods_count_std':'std'})

    tmp_usergoodcount['userGoods_count_std'].fillna(0.0,inplace=True)

    tmp_usergoodday = tmp.groupby(['customer_id'],as_index=False)['month-day'].agg({'userGoods_day_mean':'mean',
                                                                          'userGoods_day_max':'max',
                                                                          'userGoods_day_median':'median',
                                                                         'userGoods_day_std':'std'})

    tmp_usergoodday['userGoods_day_std'].fillna(0.0,inplace=True)

    customer_id = customer_id.merge(tmp_usergoodcount,on=['customer_id'],how='left')
    customer_id = customer_id.merge(tmp_usergoodday,on=['customer_id'],how='left')

    #对商品（agg）
    tmp_goodusercount = tmp.groupby(['goods_id'],as_index=False)['order_detail_id'].agg({'goodsUser_count_mean':'mean',
                                                                          'goodsUser_count_max':'max',
                                                                          'goodsUser_count_median':'median',
                                                                         'goodsUser_count_std':'std'})
    tmp_goodusercount['goodsUser_count_std'].fillna(0.0,inplace=True)
    tmp_goodusercount.rename(columns={'goods_id': 'freq_goods'}, inplace=True)
    customer_id = customer_id.merge(tmp_goodusercount,on=['freq_goods'],how='left')


    tmp_gooduserday = tmp.groupby(['goods_id'],as_index=False)['month-day'].agg({'goodsUser_day_mean':'mean',
                                                                          'goodsUser_day_max':'max',
                                                                          'goodsUser_day_median':'median',
                                                                         'goodsUser_day_std':'std'})
    tmp_gooduserday['goodsUser_day_std'].fillna(0.0,inplace=True)
    tmp_gooduserday.rename(columns={'goods_id': 'freq_goods'}, inplace=True)
    customer_id = customer_id.merge(tmp_gooduserday,on=['freq_goods'],how='left')

    print '35'

##35.商品有重复购买行为的买家数
    tmp1 = tmp[tmp['month-day'] > 1]
    goods = tmp1.groupby(['goods_id'],as_index=False)['customer_id'].agg({'goods_rebuy_usercount':'count'})
    goods.rename(columns={'goods_id': 'freq_goods'}, inplace=True)
    customer_id = customer_id.merge(goods,on=['freq_goods'],how='left')

    print '36'
##36.商品有重复购买行为的买家比例（重复买家/总买家）
    customer_id['goods_rebuy_usercount_ratio'] = customer_id.apply(lambda x: x['goods_rebuy_usercount']*1.0/x['goods_customer'], axis=1)

    print '37'
##37.商品重复购买天数（对所有用户汇总）
    tmp['goods_rebuy_day'] = tmp['month-day'].map(lambda x:x-1)
    goods = tmp.groupby(['goods_id'],as_index=False)['goods_rebuy_day'].agg({'goods_rebuyUser_daycount':'sum'})
    goods.rename(columns={'goods_id': 'freq_goods'}, inplace=True)
    customer_id = customer_id.merge(goods,on=['freq_goods'],how='left')

    print '38'
##38.商品重复购买比例（对所有用户汇总）
    goods = tmp.groupby(['goods_id'],as_index=False)['month-day'].agg({'goods_allUser_daycount':'sum'})
    goods.rename(columns={'goods_id': 'freq_goods'}, inplace=True)
    customer_id = customer_id.merge(goods,on=['freq_goods'],how='left')
    customer_id['goods_rebuyUser_daycount_ratio'] = customer_id.apply(lambda x: x['goods_rebuyUser_daycount']*1.0/x['goods_allUser_daycount'], axis=1)
    del customer_id['goods_allUser_daycount']

##39.PCA特征(i2u)
    print 'pca43'
    item_len,good_users = get_dict(date1)
    si = get_sim_matrix(good_users,item_len,'count')
    item_pca = get_PCA(si,good_users)
    customer_id = customer_id.merge(item_pca,on=['freq_goods'],how='left')

##40.LDA特征(i2u)
    print 'lda'
    tmp = get_item_to_user(date1)
    corpus = get_corpus_user(tmp)
    i2u_lda = get_LDA_user(corpus,tmp)
    customer_id = customer_id.merge(i2u_lda,on=['freq_goods'],how='left')

##41.聚类特征
    print 'SpectralClustering'
    item_len,good_users = get_dict(date1)
    si = get_sim_matrix(good_users,item_len,'cos')
    item_cluster = Clustering(si,good_users)
    customer_id = customer_id.merge(item_cluster,on=['freq_goods'],how='left')

##42.用户月度子订单价格（agg）
    print 'order_detail_price'
    tmp = date1.groupby(['customer_id','month'],as_index=False).agg({'order_detail_amount':'sum'}).sort_values(by=['customer_id','month'],ascending=True)
    tmp_count = tmp.groupby(['customer_id'],as_index=False)['order_detail_amount'].agg({'monthly_price_mean':'mean',
                                                                          'monthly_price_max':'max',
                                                                          'monthly_price_median':'median',
                                                                         'monthly_price_std':'std'}).sort_values(by=['customer_id'],ascending=True)
    tmp_count['monthly_price_std'].fillna(0.0,inplace=True)
    customer_id = customer_id.merge(tmp_count,on=['customer_id'],how='left')

##43.用户月度订单折扣（agg）
    print 'order_monthly_discount_price'
    tmp = date1.groupby(['customer_id','month'],as_index=False).agg({'order_total_discount':'sum'}).sort_values(by=['customer_id','month'],ascending=True)
    tmp_count = tmp.groupby(['customer_id'],as_index=False)['order_total_discount'].agg({'monthly_discount_price_mean':'mean',
                                                                          'monthly_discount_price_max':'max',
                                                                          'monthly_discount_price_median':'median',
                                                                         'monthly_discount_price_std':'std'}).sort_values(by=['customer_id'],ascending=True)
    tmp_count['monthly_discount_price_std'].fillna(0.0,inplace=True)
    customer_id = customer_id.merge(tmp_count,on=['customer_id'],how='left')

##44.商品的评价次数 / 评价人数
    print 'goods_people_comment_count'
    tmp = date1.groupby(['goods_id'],as_index=False)['is_customer_rate'].agg({'goods_comment_count':'count'})
    tmp_user = date1.groupby(['goods_id'],as_index=False)['customer_id'].nunique()
    tmp['goods_comment_user'] = tmp_user
    tmp.rename(columns={'goods_id': 'freq_goods'}, inplace=True)
    customer_id = customer_id.merge(tmp,on=['freq_goods'],how='left')

##45.用户父订单个数
    tmp = date1.groupby(['customer_id'])['order_id'].nunique().reset_index()
    tmp.rename(columns={'order_id': 'father_order_count'}, inplace=True)
    customer_id = customer_id.merge(tmp,on=['customer_id'],how='left')

    data = customer_id

    if isSubmit==False:
        data['label'] = 0
        data.loc[data['customer_id'].isin(list(date2['customer_id'].unique())),'label'] = 1
        print data['label'].mean()
    print data.shape
    return data



