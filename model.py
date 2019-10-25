# -*- coding: UTF-8 -*-
__author__ = 'xun.kang'

import xgboost as xgb
import numpy as np

def xgboost_model(X_train, X_valid, y_train, y_valid,X_submit):

    dtrain = xgb.DMatrix(X_train,label=y_train)
    dtest = xgb.DMatrix(X_valid,label=y_valid)
    param = {'max_depth':6, 'eta':0.1, 'objective':'binary:logistic','verbosity':'2','eval_metric':'logloss'}
    num_round = 10
    bst = xgb.train(param, dtrain, num_round)
    preds = bst.predict(dtest)
    X_submit = xgb.DMatrix(X_submit)
    y_submit = bst.predict(X_submit)
    return y_submit,preds

'''
metric
'''
def logloss(y_true, y_pred,deta = 3, eps=1e-15):
    # Prepare numpy array data
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    assert (len(y_true) and len(y_true) == len(y_pred))
    # Clip y_pred between eps and 1-eps
    p = np.clip(y_pred, eps, 1-eps)
    loss = np.sum(- y_true * np.log(p) * deta - (1 - y_true) * np.log(1-p))
    return loss / len(y_true)

