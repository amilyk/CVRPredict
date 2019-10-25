# -*- coding: UTF-8 -*-
__author__ = 'xun.kang'
from feature import *
from sklearn.model_selection import train_test_split
from model import *
from utils import *
from sklearn import preprocessing
import warnings
warnings.filterwarnings('ignore')

'''
读入数据
'''
print 'part1.reading'

df = pd.read_csv('../test/round1_diac2019_train.csv',low_memory=False)
df['order_pay_time'] = pd.to_datetime(df['order_pay_time'])
df['order_pay_date'] = df['order_pay_time'].dt.date

df['month'] = df['order_pay_date'].map(lambda x : x.month)
df['month-day'] = df['order_pay_date'].map(lambda x : str(x.month)+'_'+str(x.day))

'''
预处理
1.处理异常值
子订单订单折扣:存在负数（1/100)比例，与父订单总金额的折扣互为相反数，因为父订单都只有 1 个子订单。

2.缺失值
性别缺失比例为 60%，对于缺失比例大的特征不好填充，就用未知填补。无法用性别交叉统计特征，统计结果会影响预估。
用户 id 缺失: 应该舍弃，无法用作用户对 label 的预估。
子订单价格 order_detail_amount存在缺失值na，缺失比例(1/20)
结合父订单价格发现:order_detail_payment存在缺失，缺失比例(1/20)
order_detail_amount 为 0 时，订单总金额大于 0.即为 0 时也是缺失
order_detail_payment为 0 时，订单总实付金额大于0，即为 0 也是缺失
'''

print 'part2.pre'
##子订单金额为 na,填充
tmp = df[df['order_detail_amount'].isnull() == True]
df = get_orderdetail_amount(tmp,df)

##子订单金额为 0，总金额不为 0.且父订单下所有子订单金额均为 0，缺失。
keep =[]
for index,row in df.iterrows():
    if (row['order_detail_payment'] == 0 and row['order_total_payment'] != 0):
        keep.append(index)
tmp = df.loc[keep]
df = get_orderdetail_amount(tmp,df)

#异常值：价格为负值
df['order_detail_discount'] = df['order_detail_discount'].map(lambda x: -x if x < 0 else x)

#na：舍弃customer_id=na 的行
df = df[df['customer_id'].isnull()==False]

#na：填补性别缺失 0 填补
df.customer_gender.fillna(0,inplace=True)

#前半年用于训练
train_history = df[(df['order_pay_date'].astype(str)<='2013-07-03')]
#整年数据 fit
online_history = df[(df['order_pay_date'].astype(str)<='2013-12-31')]
# train_label 相对于 train_history 的未来180天的数据
train_label = df[df['order_pay_date'].astype(str)>='2013-07-04']

'''
生成训练数据和提交数据
'''
print 'part3.featuring'
train = make_feature_and_label(train_history,train_label,False)
submit = make_feature_and_label(online_history,None,True)
for data in [train, submit]:
    data['customer_city'] = preprocessing.LabelEncoder().fit_transform(data['customer_city'].fillna('None'))
    data['customer_province'] = preprocessing.LabelEncoder().fit_transform(data['customer_province'].fillna('None'))
    data['freq_goods'] = preprocessing.LabelEncoder().fit_transform(data['freq_goods'])
    data.fillna(0,inplace=True)

# 构建机器学习所需的label和data
y = train['label']
feature = [x for x in train.columns if x not in ['customer_id','label']]
X = train[feature]
'''
保存正负样本比例 划分训练测试
'''
print 'part4.split'
X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.25, random_state=42,stratify=y)
X_submit = submit[feature]

'''
模型训练
'''
print 'part5.train predict'
y_submit,preds = xgboost_model(X_train, X_valid, y_train, y_valid,X_submit)
#线下 loss
print logloss(y_valid,preds)

'''
生成提交结果的数据格式
'''
print 'part6.submitdata'
all_customer = pd.DataFrame(df[['customer_id']]).drop_duplicates(['customer_id']).dropna()
# print(all_customer.shape)
submit_df = pd.DataFrame({'customer_id':submit['customer_id'].values,'result':y_submit})
all_customer = pd.merge(all_customer,submit_df,on=['customer_id'],how='left',copy=False)
all_customer = all_customer.sort_values(['customer_id'])
all_customer['customer_id'] = all_customer['customer_id'].astype('int64')
all_customer.to_csv('../test/round1_diac2019_test.csv',index=False)


