#!/usr/bin/env python3
"""
xgboost_optimizer.py

Módulo XGBoost para optimizar pesos de scoring en BGP Failover

Objetivo: Encontrar los pesos ÓPTIMOS basado en datos históricos:
- Peer latency weight (actual: 0.7)
- DNS latency weight (actual: 0.3)
- Loss penalty multiplier (actual: 100)
- Jitter penalty (actual: 0.5)

Uso:
    from xgboost_optimizer import ScoringWeightOptimizer
    optimizer = ScoringWeightOptimizer()
    optimizer.train(training_df)
    weights = optimizer.get_optimized_weights()
"""

import xgboost as xgb
import pandas as pd
import numpy as np
import logging
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, roc_auc_score, roc_curve
)
import matplotlib.pyplot as plt
import seaborn as sns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScoringWeightOptimizer:
    """
    Usa XGBoost para aprender los pesos óptimos de scoring basado en datos históricos
    
    El modelo predice si DEBERÍA OCURRIR un failover basado en:
    - Métricas de red (latencia, jitter, pérdida)
    - Contexto (hora del día, día de la semana)
    
    Feature importance → Pesos óptimos
    """
    
    def __init__(self):
        self.model = None
        self.feature_importance = None
        self.X_test = None
        self.y_test = None
        self.y_pred = None
        self.y_pred_proba = None
    
    def prepare_features(self, df):
        """
        Prepara features para el modelo
        
        Las características son los COMPONENTES de la fórmula:
        - peer_latency_ms: componente de latencia del peer
        - dns_latency_ms: componente de latencia del DNS
        - peer_loss_pct: pérdida en peer
        - dns_loss_pct: pérdida en DNS
        - peer_jitter_ms: jitter del peer
        - dns_jitter_ms: jitter del DNS
        - degradation_cycle: fase de degradación (0-3) ✅ NUEVO
        - hour_of_day: contexto temporal
        - is_peak_traffic: hora pico vs off-peak
        - is_weekend: contexto de día
        """
        
        features = [
            'peer_latency_ms',
            'dns_latency_ms',
            'peer_loss_pct',
            'dns_loss_pct',
            'peer_jitter_ms',
            'dns_jitter_ms',
            'degradation_cycle',            # ✅ NUEVO: Fase de degradación
            'hour_of_day',
            'is_peak_traffic',  # ✅ CORREGIDO: is_peak → is_peak_traffic
            'is_weekend'
        ]
        
        X = df[features].copy()
        y = df['should_failover'].copy()
        
        # Validar
        if X.isnull().any().any():
            logger.warning("⚠️ Hay valores NULL en features, rellenando con 0")
            X = X.fillna(0)
        
        return X, y, features
    
    def train(self, df, test_size=0.2, random_state=42):
        """
        Entrena el modelo XGBoost
        
        Args:
            df: DataFrame con datos de entrenamiento
            test_size: proporción de datos para testing
            random_state: seed para reproducibilidad
        
        Returns:
            tuple: (y_test, y_pred, y_pred_proba)
        """
        
        logger.info("\n" + "=" * 80)
        logger.info("🤖 XGBoost: Optimizando Weights de Scoring")
        logger.info("=" * 80)
        
        # Preparar datos
        X, y, features = self.prepare_features(df)
        
        # Split train/test con stratification (mantener balance)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )
        
        self.X_test = X_test
        self.y_test = y_test
        
        logger.info(f"\n📊 Dataset:")
        logger.info(f"   Training:  {len(X_train):6d} samples")
        logger.info(f"   Testing:   {len(X_test):6d} samples")
        logger.info(f"   Total:     {len(X):6d} samples")
        logger.info(f"   Features:  {X.shape[1]}")
        
        # ✅ NUEVO: Verificar que degradation_cycle está presente
        if 'degradation_cycle' in X.columns:
            logger.info(f"\n✓ degradation_cycle PRESENTE:")
            logger.info(f"   Valores únicos: {sorted(X['degradation_cycle'].unique())}")
            logger.info(f"   Rango: {X['degradation_cycle'].min()} - {X['degradation_cycle'].max()}")
        else:
            logger.warning(f"\n✗ degradation_cycle NO ENCONTRADO en features")
        
        logger.info(f"\n⚖️ Balance de clases:")
        for label in [0, 1]:
            count = (y == label).sum()
            pct = count / len(y) * 100
            label_name = "No Failover" if label == 0 else "Failover"
            logger.info(f"   {label_name:15s}: {count:6d} ({pct:5.1f}%)")
        
        # Entrenar modelo
        logger.info(f"\n🔄 Entrenando modelo XGBoost...")
        logger.info(f"   n_estimators: 100")
        logger.info(f"   max_depth: 6")
        logger.info(f"   learning_rate: 0.1")
        
        # ✅ NUEVO: Calcular ratio de clases para manejar desbalance
        scale_pos_weight = (y == 0).sum() / (y == 1).sum() if (y == 1).sum() > 0 else 1.0
        logger.info(f"   scale_pos_weight: {scale_pos_weight:.2f} (ratio desbalance)")
        
        self.model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,  # ✅ NUEVO: Manejar desbalance
            random_state=random_state,
            use_label_encoder=False,
            eval_metric='logloss',
            verbose=0
        )
        
        self.model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )
        
        # Predicciones
        self.y_pred = self.model.predict(X_test)
        self.y_pred_proba = self.model.predict_proba(X_test)[:, 1]
        
        # Evaluación
        accuracy = accuracy_score(y_test, self.y_pred)
        precision = precision_score(y_test, self.y_pred, zero_division=0)
        recall = recall_score(y_test, self.y_pred, zero_division=0)
        f1 = f1_score(y_test, self.y_pred, zero_division=0)
        
        try:
            auc = roc_auc_score(y_test, self.y_pred_proba)
        except:
            auc = 0.0
        
        logger.info(f"\n✅ Modelo entrenado:")
        logger.info(f"   Accuracy:  {accuracy:.4f} (corrección general)")
        logger.info(f"   Precision: {precision:.4f} (de 100 fallos predichos, ¿cuántos fueron reales?)")
        logger.info(f"   Recall:    {recall:.4f} (de 100 fallos reales, ¿cuántos detectamos?)")
        logger.info(f"   F1 Score:  {f1:.4f} (balance precision-recall)")
        logger.info(f"   ROC-AUC:   {auc:.4f} (discriminación)")
        
        # Feature importance
        self.feature_importance = pd.DataFrame({
            'feature': features,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        return self.y_test, self.y_pred, self.y_pred_proba
    
    def get_optimized_weights(self):
        """
        Extrae los pesos optimizados del modelo
        
        Mapea feature importance → pesos de scoring
        
        Returns:
            dict con pesos optimizados
        """
        
        if self.feature_importance is None:
            logger.error("❌ Debes entrenar el modelo primero")
            return None
        
        logger.info("\n" + "=" * 80)
        logger.info("📈 PESOS OPTIMIZADOS (Feature Importance)")
        logger.info("=" * 80)
        
        # Mostrar importancia de features
        logger.info("\n🔍 Importancia de cada característica:")
        for idx, (_, row) in enumerate(self.feature_importance.iterrows(), 1):
            feature = row['feature']
            importance = row['importance']
            pct = importance * 100
            
            # Barra visual
            bar = "█" * int(pct / 2)
            logger.info(f"   {idx}. {feature:20s} {bar:40s} {pct:5.1f}%")
        
        # Extraer componentes específicos
        peer_lat = self.feature_importance[
            self.feature_importance['feature'] == 'peer_latency_ms'
        ]['importance'].values[0]
        
        dns_lat = self.feature_importance[
            self.feature_importance['feature'] == 'dns_latency_ms'
        ]['importance'].values[0]
        
        peer_loss = self.feature_importance[
            self.feature_importance['feature'] == 'peer_loss_pct'
        ]['importance'].values[0]
        
        dns_loss = self.feature_importance[
            self.feature_importance['feature'] == 'dns_loss_pct'
        ]['importance'].values[0]
        
        peer_jitter = self.feature_importance[
            self.feature_importance['feature'] == 'peer_jitter_ms'
        ]['importance'].values[0]
        
        dns_jitter = self.feature_importance[
            self.feature_importance['feature'] == 'dns_jitter_ms'
        ]['importance'].values[0]
        
        hour_importance = self.feature_importance[
            self.feature_importance['feature'] == 'hour_of_day'
        ]['importance'].values[0]
        
        is_peak_importance = self.feature_importance[
            self.feature_importance['feature'] == 'is_peak_traffic'  # ✅ CORREGIDO
        ]['importance'].values[0]
        
        # ✅ NUEVO: Extraer importancia de degradation_cycle
        degradation_importance = self.feature_importance[
            self.feature_importance['feature'] == 'degradation_cycle'
        ]['importance'].values[0]
        
        # Normalizar pesos de latencia
        total_latency = peer_lat + dns_lat
        if total_latency > 0:
            peer_weight = peer_lat / total_latency
            dns_weight = dns_lat / total_latency
        else:
            peer_weight = 0.7
            dns_weight = 0.3
        
        # Interpretar
        logger.info("\n" + "-" * 80)
        logger.info("INTERPRETACIÓN DE PESOS:")
        logger.info("-" * 80)
        
        logger.info(f"\n🎯 PESOS DE LATENCIA OPTIMIZADOS:")
        logger.info(f"   Peer Latency:    {peer_weight:.2%} (actual: 70.00%)")
        logger.info(f"   DNS Latency:     {dns_weight:.2%} (actual: 30.00%)")
        
        if abs(peer_weight - 0.70) > 0.05:
            if peer_weight > 0.70:
                logger.info(f"   ➜ Peer latency es SIGNIFICATIVAMENTE más importante")
                logger.info(f"     Recomendación: Aumentar peer_weight de 0.70 a {peer_weight:.2f}")
            else:
                logger.info(f"   ➜ DNS latency es SIGNIFICATIVAMENTE más importante")
                logger.info(f"     Recomendación: Reducir peer_weight de 0.70 a {peer_weight:.2f}")
        else:
            logger.info(f"   ➜ Pesos actuales son aproximadamente óptimos")
        
        logger.info(f"\n⚠️ IMPORTANCIA DE PÉRDIDA:")
        loss_importance = max(peer_loss, dns_loss)
        logger.info(f"   Peer Loss Importance:  {peer_loss:.4f}")
        logger.info(f"   DNS Loss Importance:   {dns_loss:.4f}")
        logger.info(f"   Importancia máxima: {loss_importance*100:.1f}%")
        logger.info(f"   ➜ Pérdida es {loss_importance*100:.1f}% importante en decisión de failover")
        
        logger.info(f"\n⚡ IMPORTANCIA DE JITTER:")
        jitter_importance = max(peer_jitter, dns_jitter)
        logger.info(f"   Peer Jitter Importance: {peer_jitter:.4f}")
        logger.info(f"   DNS Jitter Importance:  {dns_jitter:.4f}")
        logger.info(f"   Importancia máxima: {jitter_importance*100:.1f}%")
        logger.info(f"   ➜ Jitter es {jitter_importance*100:.1f}% importante en decisión de failover")
        
        logger.info(f"\n🕐 IMPORTANCIA CONTEXTUAL (Hora del día):")
        logger.info(f"   Hour of Day Importance: {hour_importance:.4f}")
        logger.info(f"   Peak Hour Importance:   {is_peak_importance:.4f}")
        
        if is_peak_importance > 0.02:
            logger.info(f"   ➜ IMPORTANTE: Thresholds deberían VARIAR por hora del día")
            logger.info(f"     Recomendación: Crear thresholds específicos para peak vs off-peak")
        else:
            logger.info(f"   ➜ Thresholds pueden ser iguales durante el día")
        
        # ✅ NUEVO: Mostrar importancia de degradation_cycle
        logger.info(f"\n📊 IMPORTANCIA DE DEGRADACIÓN (degradation_cycle):")
        logger.info(f"   Degradation Cycle Importance: {degradation_importance:.4f}")
        logger.info(f"   Importancia: {degradation_importance*100:.1f}%")
        
        if degradation_importance > 0.15:
            logger.info(f"   ➜ CRÍTICA: degradation_cycle es MUY IMPORTANTE para failover")
            logger.info(f"     Insight: La fase de degradación es el predictor clave")
            logger.info(f"     El modelo aprende: ciclo 1,2 → no failover | ciclo 3 → failover")
        elif degradation_importance > 0.05:
            logger.info(f"   ➜ IMPORTANTE: degradation_cycle contribuye significativamente")
        else:
            logger.info(f"   ➜ NOTA: degradation_cycle tiene baja importancia")
        
        return {
            'peer_latency_weight': float(peer_weight),
            'dns_latency_weight': float(dns_weight),
            'loss_importance': float(loss_importance),
            'jitter_importance': float(jitter_importance),
            'context_importance': float(is_peak_importance),
            'degradation_importance': float(degradation_importance),  # ✅ NUEVO
            'all_importances': self.feature_importance.to_dict('list')
        }
    
    def predict_failover_probability(self, metrics_dict):
        """
        Predice la probabilidad de failover para nuevas métricas
        
        Args:
            metrics_dict: diccionario con métricas
        
        Returns:
            float: probabilidad de failover (0-1)
        """
        
        if self.model is None:
            raise ValueError("Debes entrenar el modelo primero")
        
        features_order = [
            'peer_latency_ms', 'dns_latency_ms', 'peer_loss_pct',
            'dns_loss_pct', 'peer_jitter_ms', 'dns_jitter_ms',
            'degradation_cycle',  # ✅ NUEVO
            'hour_of_day', 'is_peak_traffic', 'is_weekend'  # ✅ CORREGIDO
        ]
        
        X = pd.DataFrame([metrics_dict])[features_order]
        prob = self.model.predict_proba(X)[0, 1]
        
        return prob
    
    def get_confusion_matrix(self):
        """
        Retorna matriz de confusión
        """
        
        if self.y_test is None or self.y_pred is None:
            raise ValueError("Debes entrenar el modelo primero")
        
        return confusion_matrix(self.y_test, self.y_pred)
    
    def plot_feature_importance(self, save_path=None):
        """
        Plotea la importancia de features
        
        Args:
            save_path: ruta para guardar la imagen
        """
        
        if self.feature_importance is None:
            raise ValueError("Debes entrenar el modelo primero")
        
        plt.figure(figsize=(10, 6))
        
        importance_sorted = self.feature_importance.sort_values('importance', ascending=True)
        
        plt.barh(importance_sorted['feature'], importance_sorted['importance'], color='steelblue')
        plt.xlabel('Importance')
        plt.title('XGBoost Feature Importance for BGP Failover Scoring')
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"✅ Gráfico guardado: {save_path}")
        
        plt.close()
