import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler, StandardScaler, LabelEncoder
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier, VotingClassifier, GradientBoostingClassifier, ExtraTreesClassifier
from sklearn.model_selection import train_test_split, KFold
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier, NeighborhoodComponentsAnalysis


def transform_data(X,y):
    """
    Transform data using a standard scaler.
    """
    
    flatten_model = make_pipeline(
        StandardScaler()
    )
    
    Labeler = LabelEncoder()
    
    X_norm = flatten_model.fit_transform(X)
    
    y = Labeler.fit_transform(y)
    
    return X_norm, y, Labeler


def train_model(X,y):
    
    RF_clf = RandomForestClassifier(n_estimators=200, oob_score=True, criterion='gini', random_state=0)
    ET_clf = ExtraTreesClassifier(n_estimators=200, oob_score=True, criterion='gini', random_state=0 ,bootstrap=True)
    NB = GaussianNB()
    kNN = KNeighborsClassifier(n_neighbors=int(len(pd.unique(y))/2))

    model = make_pipeline(
     
    VotingClassifier(estimators=[
         ('RF', RF_clf), ('NB', NB), ('kNN', kNN), ('ET', ET_clf)], voting='hard')
        )
    
    
    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=0.2)
    model.fit(X_train,pd.to_numeric(y_train))
    
    score = model.score(X_test, y_test)
    
    print(f"Ensemble Voting Classifier (RF, ET, NB, kNN) score: {score*100:.1f}%" )

    return model


def make_predictions(X_pred, model):
    
    return model.predict(X_pred)
    


def main():
    data = pd.read_csv(sys.argv[1])
    data_pred = pd.read_csv(sys.argv[2])
    
    y = data['city'] # TODO
    X = data.drop(columns=['city']) # TODO
    
    X_norm,y,le = transform_data(X,y)

                            
    X_pred = data_pred.drop(columns=['city'])
                            
    X_norm_pred, __, __ = transform_data(X_pred,y)
    
    model = train_model(X_norm,y)
    
    predictions = le.inverse_transform(make_predictions(X_norm_pred,model))
    
    pd.Series(predictions).to_csv(sys.argv[3], index=False, header=False)



if __name__ == '__main__':
    main()
