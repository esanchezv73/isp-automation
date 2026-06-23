#!/usr/bin/env python3
"""
xgboost_optimizer.py - VERSIÓN MEJORADA
Módulo XGBoost para optimizar pesos de scoring en BGP Failover

✅ MEJORAS APLICADAS:
├─ Rolling Statistics (anomalías relativas)
├─ Features Derivadas (métricas compuestas)
├─ Features de Degradación (contexto del motor BGP)
└─ Interpretación expandida de feature importance

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
    - Estadísticas móviles (comportamiento reciente)
    - Features derivadas (relaciones entre métricas)
    - Contexto de degradación (fase del motor BGP)
    - Contexto temporal (hora del día, día de la semana)
    
    Feature importance → Pesos óptimos
    """
    
    def __init__(self):
        self.model = None
        self.feature_importance = None
        self.X_test = None
        self.y_test = None
        self.y_pred = None
        self.y_pred_proba = None
        self.features_used = None  # ✅ NUEVO: Guardar features usadas
    
    def prepare_features(self, df):
        """
        ✅ MEJORADO: Prepara features para el modelo incluyendo:
        - Métricas base (latencia, pérdida, jitter)
        - Rolling statistics (anomalías relativas)
        - Features derivadas (métricas compuestas)
        - Features de degradación (contexto BGP)
        - Contexto temporal
        """
        # === CATEGORÍA 1: Métricas Base ===
        base_features = [
            'peer_latency_ms',
            'dns_latency_ms',
            'peer_loss_pct',
            'dns_loss_pct',
            'peer_jitter_ms',
            'dns_jitter_ms',
        ]
        
        # === CATEGORÍA 2: Features Derivadas (métricas compuestas) ===
        derived_features = [
            'latency_ratio',       # Ratio peer/DNS
            'total_loss_pct',      # Pérdida promedio
            'quality_index',       # Índice de calidad 0-100
        ]
        
        # === CATEGORÍA 3: Rolling Statistics (anomalías relativas) ===
        rolling_features = [
            'peer_latency_mean_10',  # Media reciente
            'peer_latency_std_10',   # Variabilidad reciente
            'peer_latency_p95_10',   # Percentil 95 reciente
        ]
        
        # === CATEGORÍA 4: Features de Degradación (contexto BGP) ===
        degradation_features = [
            'score_difference',           # Diferencia de scores
            'margin_exceeds_threshold',   # Si supera switch_margin
            'degradation_cycle',          # Fase de degradación (0-3)
        ]
        
        # === CATEGORÍA 5: Contexto Temporal ===
        temporal_features = [
            'hour_of_day',
            'is_peak_traffic',
            'is_weekend'
        ]
        
        # ✅ Combinar todas las categorías
        all_features = (
            base_features + 
            derived_features + 
            rolling_features + 
            degradation_features + 
            temporal_features
        )
        
        # ✅ Validar que las features existan en el dataframe
        available_features = []
        missing_features = []
        
        for feature in all_features:
            if feature in df.columns:
                available_features.append(feature)
            else:
                missing_features.append(feature)
        
        if missing_features:
            logger.warning(f"⚠️ Faltan {len(missing_features)} features: {missing_features}")
            logger.warning(f"   Continuando con {len(available_features)} features disponibles")
        
        if not available_features:
            raise ValueError("❌ No hay features disponibles en el dataframe")
        
        # ✅ Preparar X e y
        X = df[available_features].copy()
        y = df['should_failover'].copy()
        
        # ✅ Manejar valores NULL (común en rolling stats al inicio)
        if X.isnull().any().any():
            null_counts = X.isnull().sum()
            null_features = null_counts[null_counts > 0]
            logger.warning(f"⚠️ Valores NULL detectados en {len(null_features)} features:")
            for feat, count in null_features.items():
                logger.warning(f"   - {feat}: {count} NULLs")
            X = X.fillna(0)
            logger.info("✅ NULLs rellenados con 0")
        
        # ✅ Convertir booleanos a int (margin_exceeds_threshold)
        for col in X.columns:
            if X[col].dtype == 'bool':
                X[col] = X[col].astype(int)
        
        # ✅ Guardar features usadas
        self.features_used = available_features
        
        # ✅ Log de categorías
        logger.info(f"\n📊 Features organizadas por categoría:")
        logger.info(f"   🔹 Base (métricas de red): {len([f for f in base_features if f in available_features])}")
        logger.info(f"   🔹 Derivadas (métricas compuestas): {len([f for f in derived_features if f in available_features])}")
        logger.info(f"   🔹 Rolling (estadísticas móviles): {len([f for f in rolling_features if f in available_features])}")
        logger.info(f"   🔹 Degradación (contexto BGP): {len([f for f in degradation_features if f in available_features])}")
        logger.info(f"   🔹 Temporal (contexto horario): {len([f for f in temporal_features if f in available_features])}")
        logger.info(f"   📈 Total: {len(available_features)} features")
        
        return X, y, available_features
    
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
        logger.info("🤖 XGBoost: Optimizando Weights de Scoring (VERSIÓN MEJORADA)")
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
        
        # Calcular ratio de clases para manejar desbalance
        scale_pos_weight = (y == 0).sum() / (y == 1).sum() if (y == 1).sum() > 0 else 1.0
        logger.info(f"   scale_pos_weight: {scale_pos_weight:.2f} (ratio desbalance)")
        
        self.model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
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
        ✅ MEJORADO: Extrae los pesos optimizados del modelo
        Ahora incluye interpretación de las nuevas categorías de features
        
        Returns:
            dict con pesos optimizados y análisis por categoría
        """
        if self.feature_importance is None:
            logger.error("❌ Debes entrenar el modelo primero")
            return None
        
        logger.info("\n" + "=" * 80)
        logger.info("📈 PESOS OPTIMIZADOS (Feature Importance)")
        logger.info("=" * 80)
        
        # === Mostrar importancia de features (Top 15) ===
        logger.info("\n🔍 Top 15 características más importantes:")
        for idx, (_, row) in enumerate(self.feature_importance.head(15).iterrows(), 1):
            feature = row['feature']
            importance = row['importance']
            pct = importance * 100
            bar = "█" * int(pct / 2)
            logger.info(f"   {idx:2d}. {feature:25s} {bar:30s} {pct:5.1f}%")
        
        # === Extraer importancias por categoría ===
        def get_importance(feature_name):
            """Helper para obtener importancia de una feature"""
            matches = self.feature_importance[
                self.feature_importance['feature'] == feature_name
            ]
            if len(matches) > 0:
                return matches['importance'].values[0]
            return 0.0
        
        # === CATEGORÍA 1: Latencia Base ===
        peer_lat = get_importance('peer_latency_ms')
        dns_lat = get_importance('dns_latency_ms')
        
        # === CATEGORÍA 2: Pérdida ===
        peer_loss = get_importance('peer_loss_pct')
        dns_loss = get_importance('dns_loss_pct')
        
        # === CATEGORÍA 3: Jitter ===
        peer_jitter = get_importance('peer_jitter_ms')
        dns_jitter = get_importance('dns_jitter_ms')
        
        # === CATEGORÍA 4: Rolling Statistics (NUEVO) ===
        rolling_mean = get_importance('peer_latency_mean_10')
        rolling_std = get_importance('peer_latency_std_10')
        rolling_p95 = get_importance('peer_latency_p95_10')
        rolling_total = rolling_mean + rolling_std + rolling_p95
        
        # === CATEGORÍA 5: Features Derivadas (NUEVO) ===
        latency_ratio = get_importance('latency_ratio')
        total_loss = get_importance('total_loss_pct')
        quality_index = get_importance('quality_index')
        derived_total = latency_ratio + total_loss + quality_index
        
        # === CATEGORÍA 6: Degradación (NUEVO) ===
        score_diff = get_importance('score_difference')
        margin_exceeds = get_importance('margin_exceeds_threshold')
        degradation_cycle = get_importance('degradation_cycle')
        degradation_total = score_diff + margin_exceeds + degradation_cycle
        
        # === CATEGORÍA 7: Contexto Temporal ===
        hour_importance = get_importance('hour_of_day')
        is_peak_importance = get_importance('is_peak_traffic')
        is_weekend_importance = get_importance('is_weekend')
        temporal_total = hour_importance + is_peak_importance + is_weekend_importance
        
        # === Normalizar pesos de latencia ===
        total_latency = peer_lat + dns_lat
        if total_latency > 0:
            peer_weight = peer_lat / total_latency
            dns_weight = dns_lat / total_latency
        else:
            peer_weight = 0.7
            dns_weight = 0.3
        
        # === Interpretación ===
        logger.info("\n" + "-" * 80)
        logger.info("📊 ANÁLISIS POR CATEGORÍA:")
        logger.info("-" * 80)
        
        # 1. Latencia
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
        
        # 2. Pérdida
        loss_importance = max(peer_loss, dns_loss)
        logger.info(f"\n⚠️ IMPORTANCIA DE PÉRDIDA:")
        logger.info(f"   Peer Loss Importance:  {peer_loss:.4f}")
        logger.info(f"   DNS Loss Importance:   {dns_loss:.4f}")
        logger.info(f"   Importancia máxima: {loss_importance*100:.1f}%")
        logger.info(f"   ➜ Pérdida es {loss_importance*100:.1f}% importante en decisión de failover")
        
        # 3. Jitter
        jitter_importance = max(peer_jitter, dns_jitter)
        logger.info(f"\n⚡ IMPORTANCIA DE JITTER:")
        logger.info(f"   Peer Jitter Importance: {peer_jitter:.4f}")
        logger.info(f"   DNS Jitter Importance:  {dns_jitter:.4f}")
        logger.info(f"   Importancia máxima: {jitter_importance*100:.1f}%")
        logger.info(f"   ➜ Jitter es {jitter_importance*100:.1f}% importante en decisión de failover")
        
        # 4. Rolling Statistics (NUEVO)
        logger.info(f"\n📈 IMPORTANCIA DE ROLLING STATISTICS (Anomalías Relativas):")
        logger.info(f"   peer_latency_mean_10: {rolling_mean*100:.2f}%")
        logger.info(f"   peer_latency_std_10:  {rolling_std*100:.2f}%")
        logger.info(f"   peer_latency_p95_10:  {rolling_p95*100:.2f}%")
        logger.info(f"   Total categoría:      {rolling_total*100:.2f}%")
        if rolling_total > 0.10:
            logger.info(f"   ➜ 🔥 ALTA IMPORTANCIA: Las anomalías relativas son CRÍTICAS")
            logger.info(f"     Recomendación: Considerar umbrales DINÁMICOS basados en Z-score")
        elif rolling_total > 0.05:
            logger.info(f"   ➜ ✅ IMPORTANCIA MODERADA: Las anomalías relativas aportan valor")
        else:
            logger.info(f"   ➜ ℹ️ BAJA IMPORTANCIA: Los umbrales estáticos son suficientes")
        
        # 5. Features Derivadas (NUEVO)
        logger.info(f"\n🔗 IMPORTANCIA DE FEATURES DERIVADAS:")
        logger.info(f"   latency_ratio:  {latency_ratio*100:.2f}%")
        logger.info(f"   total_loss_pct: {total_loss*100:.2f}%")
        logger.info(f"   quality_index:  {quality_index*100:.2f}%")
        logger.info(f"   Total categoría: {derived_total*100:.2f}%")
        if derived_total > 0.10:
            logger.info(f"   ➜ 🔥 Las métricas compuestas capturan patrones importantes")
        else:
            logger.info(f"   ➜ ℹ️ Las métricas individuales son suficientes")
        
        # 6. Degradación (NUEVO)
        logger.info(f"\n🎚️ IMPORTANCIA DE DEGRADACIÓN (Contexto BGP):")
        logger.info(f"   score_difference:         {score_diff*100:.2f}%")
        logger.info(f"   margin_exceeds_threshold: {margin_exceeds*100:.2f}%")
        logger.info(f"   degradation_cycle:        {degradation_cycle*100:.2f}%")
        logger.info(f"   Total categoría:          {degradation_total*100:.2f}%")
        if degradation_total > 0.15:
            logger.info(f"   ➜ 🔥 CRÍTICO: El contexto de degradación es ESENCIAL")
            logger.info(f"     El modelo está aprendiendo la lógica del motor BGP")
        elif degradation_total > 0.05:
            logger.info(f"   ➜ ✅ El contexto de degradación aporta información útil")
        else:
            logger.info(f"   ➜ ℹ️ Las métricas individuales son más importantes que el contexto")
        
        # 7. Contexto Temporal
        logger.info(f"\n🕐 IMPORTANCIA CONTEXTUAL (Hora del día):")
        logger.info(f"   Hour of Day Importance: {hour_importance*100:.2f}%")
        logger.info(f"   Peak Hour Importance:   {is_peak_importance*100:.2f}%")
        logger.info(f"   Weekend Importance:     {is_weekend_importance*100:.2f}%")
        logger.info(f"   Total categoría:        {temporal_total*100:.2f}%")
        if temporal_total > 0.05:
            logger.info(f"   ➜ IMPORTANTE: Thresholds deberían VARIAR por hora del día")
            logger.info(f"     Recomendación: Crear thresholds específicos para peak vs off-peak")
        else:
            logger.info(f"   ➜ Thresholds pueden ser iguales durante el día")
        
        # === Resumen ejecutivo ===
        logger.info("\n" + "-" * 80)
        logger.info("📋 RESUMEN EJECUTIVO:")
        logger.info("-" * 80)
        
        category_totals = {
            'Latencia Base': peer_lat + dns_lat,
            'Pérdida': peer_loss + dns_loss,
            'Jitter': peer_jitter + dns_jitter,
            'Rolling Stats': rolling_total,
            'Features Derivadas': derived_total,
            'Degradación (BGP)': degradation_total,
            'Contexto Temporal': temporal_total
        }
        
        sorted_categories = sorted(
            category_totals.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        for idx, (category, total) in enumerate(sorted_categories, 1):
            pct = total * 100
            bar = "█" * int(pct / 2)
            logger.info(f"   {idx}. {category:25s} {bar:25s} {pct:5.1f}%")
        
        return {
            # Pesos de latencia
            'peer_latency_weight': float(peer_weight),
            'dns_latency_weight': float(dns_weight),
            
            # Importancias por categoría
            'loss_importance': float(loss_importance),
            'jitter_importance': float(jitter_importance),
            'rolling_importance': float(rolling_total),
            'derived_importance': float(derived_total),
            'degradation_importance': float(degradation_total),
            'context_importance': float(temporal_total),
            
            # Importancias individuales
            'all_importances': self.feature_importance.to_dict('list'),
            
            # Recomendaciones
            'recommendations': {
                'dynamic_thresholds': rolling_total > 0.10,
                'time_based_thresholds': temporal_total > 0.05,
                'use_compound_metrics': derived_total > 0.10,
                'bgp_context_critical': degradation_total > 0.15
            }
        }
    
    def predict_failover_probability(self, metrics_dict):
        """
        ✅ MEJORADO: Predice la probabilidad de failover para nuevas métricas
        
        Args:
            metrics_dict: diccionario con métricas
            
        Returns:
            float: probabilidad de failover (0-1)
        """
        if self.model is None:
            raise ValueError("Debes entrenar el modelo primero")
        
        if self.features_used is None:
            raise ValueError("Debes entrenar el modelo primero para conocer las features")
        
        # ✅ Validar que todas las features necesarias estén presentes
        missing = [f for f in self.features_used if f not in metrics_dict]
        if missing:
            logger.warning(f"⚠️ Faltan {len(missing)} features en metrics_dict: {missing}")
            logger.warning(f"   Rellenando con 0")
            for f in missing:
                metrics_dict[f] = 0
        
        # ✅ Crear dataframe con el orden correcto
        X = pd.DataFrame([metrics_dict])[self.features_used]
        
        # ✅ Convertir booleanos a int
        for col in X.columns:
            if X[col].dtype == 'bool':
                X[col] = X[col].astype(int)
        
        prob = self.model.predict_proba(X)[0, 1]
        return prob
    
    def get_confusion_matrix(self):
        """Retorna matriz de confusión"""
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
        
        plt.figure(figsize=(12, 8))
        importance_sorted = self.feature_importance.sort_values('importance', ascending=True)
        
        # ✅ Colorear por categoría
        colors = []
        for feature in importance_sorted['feature']:
            if feature in ['peer_latency_ms', 'dns_latency_ms']:
                colors.append('#1f77b4')  # Azul - Latencia
            elif feature in ['peer_loss_pct', 'dns_loss_pct']:
                colors.append('#ff7f0e')  # Naranja - Pérdida
            elif feature in ['peer_jitter_ms', 'dns_jitter_ms']:
                colors.append('#2ca02c')  # Verde - Jitter
            elif 'mean_10' in feature or 'std_10' in feature or 'p95_10' in feature:
                colors.append('#d62728')  # Rojo - Rolling
            elif feature in ['latency_ratio', 'total_loss_pct', 'quality_index']:
                colors.append('#9467bd')  # Púrpura - Derivadas
            elif feature in ['score_difference', 'margin_exceeds_threshold', 'degradation_cycle']:
                colors.append('#8c564b')  # Marrón - Degradación
            else:
                colors.append('#7f7f7f')  # Gris - Temporal
        
        plt.barh(importance_sorted['feature'], importance_sorted['importance'], color=colors)
        plt.xlabel('Importance', fontsize=12)
        plt.title('XGBoost Feature Importance for BGP Failover Scoring\n(Colores por categoría)', 
                  fontsize=14)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"✅ Gráfico guardado: {save_path}")
        
        plt.close()
