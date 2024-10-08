import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
import joblib
from datetime import datetime
from geopy.distance import geodesic

def calculate_age(born):
    today = datetime.now()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def preprocess_data(df):
    # Conversion de la date en datetime
    df['current_time'] = pd.to_datetime(df['current_time'], unit='ms')
    df['dob'] = pd.to_datetime(df['dob'])
    
    # Calcul de l'âge
    df['age'] = df['dob'].apply(calculate_age)
    
    # Ajout des colonnes de temps
    df['hour'] = df['current_time'].dt.hour
    df['dayofweek'] = df['current_time'].dt.dayofweek
    df['month'] = df['current_time'].dt.month
    df['dayofyear'] = df['current_time'].dt.dayofyear
    df['dayofmonth'] = df['current_time'].dt.day
    df['weekofyear'] = df['current_time'].dt.isocalendar().week
    
    # Calcul de la distance
    df['distance'] = df.apply(lambda row: geodesic((row['lat'], row['long']), 
                                                   (row['merch_lat'], row['merch_long'])).km, axis=1)
    
    # Extraction du card_issuer_Bank et card_issuer_MMI
    df['card_issuer_Bank'] = df['cc_num'].astype(str).str[1:6].astype(int)
    df['card_issuer_MMI'] = 'mmi' + df['cc_num'].astype(str).str[0]
    
    return df

def load_preprocessor(preprocessor_path='scaler.joblib'):
    return joblib.load(preprocessor_path)

def predict(preprocessor, model, data):
    df = pd.DataFrame([data])
    df = preprocess_data(df)
    
    # Sélection des colonnes pertinentes
    numeric_features = ['amt', 'lat', 'long', 'city_pop', 'merch_lat', 'merch_long', 'age', 
                        'hour', 'dayofweek', 'month', 'dayofyear', 'dayofmonth', 'weekofyear', 
                        'card_issuer_Bank', 'distance']
    categorical_features = ['category', 'gender', 'card_issuer_MMI']
    
    df = df[numeric_features + categorical_features]
    
    # Utiliser le préprocesseur
    X_processed = preprocessor.transform(df)
    
    # Faire la prédiction
    return model.predict(X_processed)[0]

def main():
    # Chargement du préprocesseur
    preprocessor = load_preprocessor()
    
    # Chargement du modèle (assurez-vous que le fichier existe)
    model = joblib.load('model.joblib')
    
    # Exemple de données (dans un scénario réel, cela proviendrait de votre API)
    data = {
        "cc_num": 4969856774088583,
        "merchant": "fraud_Fahey Inc",
        "category": "kids_pets",
        "amt": 62.41,
        "gender": "F",
        "zip": 70003,
        "lat": 29.9975,
        "long": -90.2146,
        "city_pop": 137067,
        "merch_lat": 30.441576,
        "merch_long": -90.938334,
        "current_time": 1727864419287,
        "dob": "1951-12-04"
    }
    
    # Faire une prédiction
    prediction = predict(preprocessor, model, data)
    print(f"Prédiction : {'Fraude' if prediction == 1 else 'Pas de fraude'}")

if __name__ == "__main__":
    main()