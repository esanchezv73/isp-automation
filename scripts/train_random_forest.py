#!/usr/bin/env python3
"""
train_random_forest.py

Random Forest para Threshold Classification
├─ Propósito: Clasificar si debería ocurrir failover (binario: 0/1)
├─ Features: Todas de ml_features incluyendo degradation_cycle
├─ Output: Predicción binaria + Feature importance
└─ Aplicación: Validación, alertas, threshold optimization
"""

import logging
import warnings
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from model_utils import MLDataLoader, MLModelEvaluator, MLPipelineHelper

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RandomForestThresholdClassifier:
    """
    ✅ Random Forest para Binary Classification
    
    Objetivo: Predecir si debería haber failover
    ├─ Input: Features de ml_features
    ├─ Output: Predicción binaria (0/1) + Feature importance
    └─ Validación: Comparar con XGBoost weights
    """
    
    def __init__(self, n_estimators=100, max_depth=10, random_state=42):
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.random_state = random_state
        self.model = None
        self.feature_cols = None
        self.feature_importance = None
    
    def train(self, X_train, y_train):
        """
        Entrenar Random Forest
        
        Args:
            X_train: Training features
            y_train: Training target
        """
        
        logger.info("\n" + "=" * 80)
        logger.info("🌲 RANDOM FOREST: Entrenando modelo de clasificación")
        logger.info("=" * 80)
        
        logger.info(f"\n⚙️ Parámetros:")
        logger.info(f"   n_estimators: {self.n_estimators}")
        logger.info(f"   max_depth: {self.max_depth}")
        logger.info(f"   random_state: {self.random_state}")
        
        logger.info(f"\n📊 Dataset de entrenamiento:")
        logger.info(f"   Samples: {len(X_train)}")
        logger.info(f"   Features: {X_train.shape[1]}")
        
        # Crear modelo
        self.model = RandomForestClassifier(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            random_state=self.random_state,
            n_jobs=-1,
            verbose=0
        )
        
        # Entrenar
        logger.info(f"\n🔄 Entrenando {self.n_estimators} árboles...")
        self.model.fit(X_train, y_train)
        logger.info(f"✅ Entrenamiento completado")
        
        # Guardar feature importance
        self.feature_importance = self.model.feature_importances_
    
    def evaluate(self, X_train, X_test, y_train, y_test, feature_cols):
        """
        Evaluar modelo en train y test
        """
        
        self.feature_cols = feature_cols
        
        # Predicciones
        y_train_pred = self.model.predict(X_train)
        y_test_pred = self.model.predict(X_test)
        y_test_proba = self.model.predict_proba(X_test)[:, 1]
        
        # Evaluar training
        logger.info("\n" + "-" * 80)
        logger.info("TRAINING SET PERFORMANCE")
        logger.info("-" * 80)
        train_metrics = MLModelEvaluator.evaluate_model(
            y_train, y_train_pred, 
            model_name="Random Forest (Training)"
        )
        
        # Evaluar testing
        logger.info("\n" + "-" * 80)
        logger.info("TESTING SET PERFORMANCE")
        logger.info("-" * 80)
        test_metrics = MLModelEvaluator.evaluate_model(
            y_test, y_test_pred, y_test_proba,
            model_name="Random Forest (Testing)"
        )
        
        # Feature importance
        logger.info("\n" + "-" * 80)
        MLModelEvaluator.log_feature_importance(
            self.feature_importance,
            feature_cols,
            model_name="Random Forest",
            top_n=15
        )
        
        return {
            'train_metrics': train_metrics,
            'test_metrics': test_metrics,
            'feature_importance': self.get_feature_importance_df()
        }
    
    def get_feature_importance_df(self):
        """
        Retorna DataFrame con feature importance ordenado
        """
        
        import pandas as pd
        
        df = pd.DataFrame({
            'feature': self.feature_cols,
            'importance': self.feature_importance
        }).sort_values('importance', ascending=False)
        
        return df
    
    def predict(self, X):
        """
        Hacer predicción en nuevos datos
        
        Args:
            X: Features (numpy array o DataFrame)
        
        Returns:
            Predicción binaria y probabilidad
        """
        
        pred = self.model.predict(X)
        proba = self.model.predict_proba(X)[:, 1]
        
        return pred, proba
    
    def get_decision_rules(self, max_rules=5):
        """
        Extraer decisiones principales de los árboles
        
        Nota: Simplificado, mostraría lógica de decisión
        """
        
        logger.info("\n" + "=" * 80)
        logger.info("📋 DECISIONES PRINCIPALES (Árboles Base)")
        logger.info("=" * 80)
        
        # Obtener profundidad de árboles
        logger.info(f"\n🌳 Estadísticas de Árboles:")
        depths = [tree.get_depth() for tree in self.model.estimators_]
        logger.info(f"   Profundidad promedio: {sum(depths)/len(depths):.1f}")
        logger.info(f"   Min: {min(depths)}, Max: {max(depths)}")
        
        logger.info(f"\n✅ RF usa {self.n_estimators} árboles de decisión")
        logger.info(f"   Cada árbol es independiente")
        logger.info(f"   Predicción final: Voto mayoritario")


def main():
    """
    Pipeline completo de Random Forest
    """
    
    logger.info("=" * 80)
    logger.info("🚀 RANDOM FOREST PARA THRESHOLD CLASSIFICATION")
    logger.info("=" * 80)
    
    # 1. Cargar datos
    logger.info("\nPASO 1: Cargar datos de ml_features")
    logger.info("-" * 80)
    
    loader = MLDataLoader()
    df = loader.load_ml_features(days=30)
    
    # 2. Preparar features
    logger.info("\nPASO 2: Preparar features")
    logger.info("-" * 80)
    
    X, y, feature_cols = MLPipelineHelper.prepare_data(df)
    
    # 3. Split train/test
    logger.info("\nPASO 3: Split train/test")
    logger.info("-" * 80)
    
    X_train, X_test, y_train, y_test = MLPipelineHelper.split_data(X, y)
    
    # 4. Entrenar Random Forest
    logger.info("\nPASO 4: Entrenar Random Forest")
    logger.info("-" * 80)
    
    rf_model = RandomForestThresholdClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42
    )
    
    rf_model.train(X_train, y_train)
    
    # 5. Evaluar modelo
    logger.info("\nPASO 5: Evaluar modelo")
    logger.info("-" * 80)
    
    results = rf_model.evaluate(X_train, X_test, y_train, y_test, feature_cols)
    
    # 6. Análisis de decisiones
    logger.info("\nPASO 6: Análisis de decisiones")
    logger.info("-" * 80)
    
    rf_model.get_decision_rules()
    
    # 7. Comparación con XGBoost
    logger.info("\n" + "=" * 80)
    logger.info("📊 COMPARACIÓN CON XGBOOST")
    logger.info("=" * 80)
    
    rf_importance = rf_model.get_feature_importance_df()
    
    logger.info(f"\n✅ Random Forest vs XGBoost Feature Importance:")
    logger.info(f"\n{'Feature':<30} {'RF Importance':<20} {'Esperado (XGBoost)':<20}")
    logger.info(f"{'-'*70}")
    
    top_features_rf = rf_importance.head(5)
    for _, row in top_features_rf.iterrows():
        feature = row['feature']
        rf_imp = row['importance'] * 100
        
        # Valores esperados del XGBoost training anterior
        expected = {
            'degradation_cycle': 53.9,
            'dns_jitter_ms': 22.8,
            'peer_jitter_ms': 19.8,
            'dns_latency_ms': 2.3,
            'peer_latency_ms': 1.2
        }
        
        expected_imp = expected.get(feature, 0)
        
        logger.info(f"{feature:<30} {rf_imp:>18.2f}% {expected_imp:>18.2f}%")
    
    # 8. Resumen final
    logger.info("\n" + "=" * 80)
    logger.info("✅ RANDOM FOREST TRAINING COMPLETADO")
    logger.info("=" * 80)
    
    logger.info(f"\n📊 Resultados Finales:")
    logger.info(f"   Test Accuracy:  {results['test_metrics']['accuracy']:.4f}")
    logger.info(f"   Test Precision: {results['test_metrics']['precision']:.4f}")
    logger.info(f"   Test Recall:    {results['test_metrics']['recall']:.4f}")
    logger.info(f"   Test F1:        {results['test_metrics']['f1']:.4f}")
    
    if 'auc' in results['test_metrics']:
        logger.info(f"   Test ROC-AUC:   {results['test_metrics']['auc']:.4f}")
    
    logger.info(f"\n🔍 Feature Importance Top 3:")
    for idx, (_, row) in enumerate(rf_importance.head(3).iterrows(), 1):
        logger.info(f"   {idx}. {row['feature']}: {row['importance']*100:.2f}%")
    
    logger.info(f"\n✅ LISTO PARA VALIDACIÓN vs XGBoost 🚀")
    
    return rf_model, results


if __name__ == '__main__':
    rf_model, results = main()
