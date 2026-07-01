#!/usr/bin/env python3
"""
xgboost_optimizer.py - VERSIÓN CON CROSS-VALIDATION
✅ CORRECCIONES APLICADAS:
├─ Eliminada feature 'degradation_cycle' (data leakage)
├─ Implementado Stratified K-Fold Cross-Validation (5 folds)
├─ Aumentada regularización (max_depth=3, reg_alpha, reg_lambda)
├─ Análisis de estabilidad de features entre folds
├─ Pesos promediados entre folds (más robustos)
└─ NUEVO: Usa failover_event como target (conteo correcto de failovers)

Objetivo: Encontrar pesos ÓPTIMOS y ESTABLES basado en datos históricos
"""
import xgboost as xgb
import pandas as pd
import numpy as np
import logging
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, roc_auc_score, roc_curve
)
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScoringWeightOptimizer:
    """
    Usa XGBoost con Cross-Validation para aprender pesos óptimos de scoring.
    ✅ CORRECCIÓN: Eliminado data leakage de degradation_cycle
    ✅ NUEVO: Cross-validation para pesos más estables
    ✅ NUEVO: Usa failover_event como target (conteo correcto)
    """
    
    def __init__(self):
        self.model = None
        self.feature_importance = None
        self.feature_importance_std = None
        self.X_test = None
        self.y_test = None
        self.y_pred = None
        self.y_pred_proba = None
        self.features_used = None
        self.cv_scores = None
        self.cv_importances = None
        self.target_column = None  # ✅ NUEVO: Guardar qué target se usó
    
    def prepare_features(self, df):
        """
        ✅ CORREGIDO: Prepara features SIN degradation_cycle (data leakage)
        ✅ NUEVO: Usa failover_event como target si existe
        """
        logger.info("\n🔧 Preparando features...")
        
        # === CATEGORÍA 1: Métricas Base ===
        base_features = [
            'peer_latency_ms',
            'dns_latency_ms',
            'peer_loss_pct',
            'dns_loss_pct',
            'peer_jitter_ms',
            'dns_jitter_ms',
        ]
        
        # === CATEGORÍA 2: Features Derivadas ===
        derived_features = [
            'latency_ratio',
            'total_loss_pct',
            'quality_index',
        ]
        
        # === CATEGORÍA 3: Rolling Statistics ===
        rolling_features = [
            'rolling_mean',
            'rolling_std',
            'rolling_p95',
        ]
        
        # === CATEGORÍA 4: Features de Degradación ===
        degradation_features = [
            'score_difference',
            'margin_exceeds_threshold',
        ]
        
        # === CATEGORÍA 5: Contexto Temporal ===
        temporal_features = [
            'hour_of_day',
            'is_peak_traffic',
            'is_weekend'
        ]
        
        # === CATEGORÍA 6: Detección Combinada ===
        combined_detection_features = [
            'z_score_peer',
            'z_score_severity',
            'absolute_severity',
            'relative_diff_ms',
            'relative_severity',
            'combined_severity',
            'is_combined_anomaly',
        ]
        
        # ✅ Combinar todas las categorías
        all_features = (
            base_features + 
            derived_features + 
            rolling_features + 
            degradation_features + 
            temporal_features +
            combined_detection_features
        )
        
        # ✅ Validar que las features existan
        available_features = []
        missing_features = []
        
        for feature in all_features:
            if feature in df.columns:
                available_features.append(feature)
            else:
                missing_features.append(feature)
        
        if missing_features:
            logger.warning(f"⚠️ Faltan {len(missing_features)} features: {missing_features}")
        
        if not available_features:
            raise ValueError("❌ No hay features disponibles en el dataframe")
        
        # ✅ Preparar X
        X = df[available_features].copy()
        
        # ✅ NUEVO: Usar failover_event como target si existe
        if 'failover_event' in df.columns:
            y = df['failover_event'].copy()
            self.target_column = 'failover_event'
            logger.info("✅ Usando 'failover_event' como target (eventos únicos)")
            logger.info(f"   - Total failovers: {y.sum()}")
        else:
            y = df['should_failover'].copy()
            self.target_column = 'should_failover'
            logger.warning("⚠️ Usando 'should_failover' como target (registros duplicados)")
            logger.warning(f"   - Total registros con failover: {y.sum()}")
            logger.warning(f"   - Considere ejecutar feature_engine para crear failover_event")
        
        # ✅ Manejar valores NULL
        if X.isnull().any().any():
            null_counts = X.isnull().sum()
            null_features = null_counts[null_counts > 0]
            logger.warning(f"⚠️ Valores NULL detectados en {len(null_features)} features")
            X = X.fillna(0)
        
        # ✅ Convertir booleanos a int
        for col in X.columns:
            if X[col].dtype == 'bool':
                X[col] = X[col].astype(int)
        
        # ✅ Codificar variables categóricas de severidad
        severity_map = {'normal': 0, 'warning': 1, 'degraded': 2, 'critical': 3}
        severity_cols = ['z_score_severity', 'absolute_severity', 
                        'relative_severity', 'combined_severity']
        
        for col in severity_cols:
            if col in X.columns:
                X[col] = X[col].map(severity_map).fillna(0).astype(int)
                logger.info(f"   ✓ {col}: encoded (normal=0, warning=1, degraded=2, critical=3)")
        
        # ✅ Guardar features usadas
        self.features_used = available_features
        
        # ✅ Log de categorías
        logger.info(f"\n📊 Features organizadas por categoría:")
        logger.info(f"   🔹 Base (métricas de red): {len([f for f in base_features if f in available_features])}")
        logger.info(f"   🔹 Derivadas (métricas compuestas): {len([f for f in derived_features if f in available_features])}")
        logger.info(f"   🔹 Rolling (estadísticas móviles): {len([f for f in rolling_features if f in available_features])}")
        logger.info(f"   🔹 Degradación (contexto BGP): {len([f for f in degradation_features if f in available_features])}")
        logger.info(f"   🔹 Temporal (contexto horario): {len([f for f in temporal_features if f in available_features])}")
        logger.info(f"   🔹 Detección Combinada: {len([f for f in combined_detection_features if f in available_features])}")
        logger.info(f"   📈 Total: {len(available_features)} features")
        logger.info(f"   ❌ ELIMINADAS (data leakage): degradation_cycle")
        
        return X, y, available_features
    
    def train_with_cv(self, df, n_splits=5, random_state=42):
        """
        ✅ NUEVO: Entrena con Stratified K-Fold Cross-Validation
        """
        logger.info("\n" + "=" * 80)
        logger.info("🤖 XGBoost: Cross-Validation (VERSIÓN CORREGIDA)")
        logger.info("=" * 80)
        
        # Preparar datos
        X, y, features = self.prepare_features(df)
        
        logger.info(f"\n📊 Dataset:")
        logger.info(f"   Total samples: {len(X)}")
        logger.info(f"   Features: {len(features)}")
        
        logger.info(f"\n⚖️ Balance de clases:")
        for label in [0, 1]:
            count = (y == label).sum()
            pct = count / len(y) * 100
            label_name = "No Failover" if label == 0 else "Failover"
            logger.info(f"   {label_name:15s}: {count:6d} ({pct:5.1f}%)")
        
        # ✅ Validar que hay suficientes failovers para CV
        n_failovers = (y == 1).sum()
        if n_failovers < n_splits:
            logger.warning(f"⚠️ Solo {n_failovers} failovers para {n_splits} folds")
            logger.warning(f"   Reduciendo n_splits a {n_failovers}")
            n_splits = max(2, n_failovers)
        
        # ✅ Configurar Stratified K-Fold
        skf = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=random_state)
        
        # ✅ Configurar modelo con regularización aumentada
        scale_pos_weight = (y == 0).sum() / (y == 1).sum() if (y == 1).sum() > 0 else 1.0
        
        logger.info(f"\n🔄 Entrenando con {n_splits}-Fold Cross-Validation...")
        logger.info(f"   scale_pos_weight: {scale_pos_weight:.2f}")
        logger.info(f"   max_depth: 3 (reducido de 6 para evitar overfitting)")
        logger.info(f"   learning_rate: 0.05 (reducido de 0.1)")
        logger.info(f"   reg_alpha: 0.1 (L1 regularization)")
        logger.info(f"   reg_lambda: 1.0 (L2 regularization)")
        
        # ✅ Almacenar resultados por fold
        fold_metrics = {
            'accuracy': [],
            'precision': [],
            'recall': [],
            'f1': [],
            'roc_auc': []
        }
        
        fold_importances = {feat: [] for feat in features}
        
        # ✅ Entrenar un modelo por fold
        for fold, (train_idx, test_idx) in enumerate(skf.split(X, y), 1):
            X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
            y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
            
            # Entrenar modelo
            model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=3,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.7,
                scale_pos_weight=scale_pos_weight,
                reg_alpha=0.1,
                reg_lambda=1.0,
                random_state=random_state,
                use_label_encoder=False,
                eval_metric='logloss',
                verbose=0
            )
            
            model.fit(X_train, y_train, verbose=False)
            
            # Evaluar
            y_pred = model.predict(X_test)
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            
            fold_metrics['accuracy'].append(accuracy_score(y_test, y_pred))
            fold_metrics['precision'].append(precision_score(y_test, y_pred, zero_division=0))
            fold_metrics['recall'].append(recall_score(y_test, y_pred, zero_division=0))
            fold_metrics['f1'].append(f1_score(y_test, y_pred, zero_division=0))
            
            try:
                roc_auc = roc_auc_score(y_test, y_pred_proba)
                fold_metrics['roc_auc'].append(roc_auc)
            except:
                fold_metrics['roc_auc'].append(0.0)
            
            # Guardar importancias
            for feat, imp in zip(features, model.feature_importances_):
                fold_importances[feat].append(imp)
            
            # Log del fold
            logger.info(
                f"   Fold {fold}/{n_splits}: "
                f"Accuracy={fold_metrics['accuracy'][-1]:.3f}, "
                f"F1={fold_metrics['f1'][-1]:.3f}, "
                f"ROC-AUC={fold_metrics['roc_auc'][-1]:.3f}"
            )
        
        # ✅ Guardar resultados de CV
        self.cv_scores = fold_metrics
        self.cv_importances = fold_importances
        
        # ✅ Calcular promedios y desviaciones
        logger.info(f"\n📊 Cross-Validation Results ({n_splits} folds):")
        logger.info("-" * 80)
        
        for metric, values in fold_metrics.items():
            mean_val = np.mean(values)
            std_val = np.std(values)
            logger.info(f"   {metric:12s}: {mean_val:.4f} ± {std_val:.4f}")
        
        # ✅ Calcular importancia promedio y desviación por feature
        avg_importances = {}
        std_importances = {}
        
        for feat in features:
            values = fold_importances[feat]
            avg_importances[feat] = np.mean(values)
            std_importances[feat] = np.std(values)
        
        # ✅ Crear DataFrame de importancia
        self.feature_importance = pd.DataFrame({
            'feature': features,
            'importance': [avg_importances[f] for f in features],
            'importance_std': [std_importances[f] for f in features]
        }).sort_values('importance', ascending=False)
        
        self.feature_importance_std = self.feature_importance['importance_std'].values
        
        # ✅ Entrenar modelo final con todos los datos (para predicción)
        logger.info(f"\n🔄 Entrenando modelo final con todos los datos...")
        self.model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.7,
            scale_pos_weight=scale_pos_weight,
            reg_alpha=0.1,
            reg_lambda=1.0,
            random_state=random_state,
            use_label_encoder=False,
            eval_metric='logloss',
            verbose=0
        )
        self.model.fit(X, y, verbose=False)
        
        # ✅ Analizar estabilidad de features
        self.analyze_feature_stability()
        
        return self.feature_importance
    
    def analyze_feature_stability(self):
        """
        ✅ NUEVO: Analiza la estabilidad de las features entre folds
        """
        logger.info(f"\n📊 Análisis de Estabilidad de Features:")
        logger.info("-" * 80)
        
        stability_data = []
        
        for _, row in self.feature_importance.iterrows():
            feat = row['feature']
            mean_imp = row['importance']
            std_imp = row['importance_std']
            
            if mean_imp > 0.001:
                cv = std_imp / mean_imp
            else:
                cv = 0.0
            
            if cv < 0.3:
                stability = "✅ ESTABLE"
            elif cv < 0.7:
                stability = "⚠️ MODERADA"
            else:
                stability = "❌ INESTABLE"
            
            stability_data.append({
                'feature': feat,
                'mean': mean_imp,
                'std': std_imp,
                'cv': cv,
                'stability': stability
            })
        
        stability_df = pd.DataFrame(stability_data).sort_values('mean', ascending=False)
        
        for _, row in stability_df.head(15).iterrows():
            bar = "█" * int(row['mean'] * 100 / 2)
            logger.info(
                f"   {row['feature']:25s} {bar:25s} "
                f"{row['mean']*100:5.1f}% ± {row['std']*100:4.1f}% "
                f"(CV={row['cv']:.2f}) {row['stability']}"
            )
        
        stable_count = sum(1 for d in stability_data if d['cv'] < 0.3)
        moderate_count = sum(1 for d in stability_data if 0.3 <= d['cv'] < 0.7)
        unstable_count = sum(1 for d in stability_data if d['cv'] >= 0.7)
        
        logger.info(f"\n📋 Resumen de Estabilidad:")
        logger.info(f"   ✅ Estables (CV < 0.3):     {stable_count:2d} features")
        logger.info(f"   ⚠️ Moderadas (0.3 ≤ CV < 0.7): {moderate_count:2d} features")
        logger.info(f"   ❌ Inestables (CV ≥ 0.7):   {unstable_count:2d} features")
        
        return stability_df
    
    def get_optimized_weights(self):
        """
        ✅ MEJORADO: Extrae pesos optimizados usando importancias PROMEDIADAS de CV
        """
        if self.feature_importance is None:
            logger.error("❌ Debes entrenar el modelo primero")
            return None
        
        logger.info("\n" + "=" * 80)
        logger.info("📈 PESOS OPTIMIZADOS (Promedio de Cross-Validation)")
        logger.info("=" * 80)
        
        # Mostrar Top 20 features
        logger.info("\n🔍 Top 20 características más importantes:")
        for idx, (_, row) in enumerate(self.feature_importance.head(20).iterrows(), 1):
            feature = row['feature']
            importance = row['importance']
            std = row['importance_std']
            pct = importance * 100
            std_pct = std * 100
            bar = "█" * int(pct / 2)
            logger.info(f"   {idx:2d}. {feature:25s} {bar:25s} {pct:5.1f}% ± {std_pct:4.1f}%")
        
        # Helper para obtener importancia
        def get_importance(feature_name):
            matches = self.feature_importance[
                self.feature_importance['feature'] == feature_name
            ]
            if len(matches) > 0:
                return matches['importance'].values[0]
            return 0.0
        
        # === Extraer importancias por categoría ===
        
        # CATEGORÍA 1: Latencia Base
        peer_lat = get_importance('peer_latency_ms')
        dns_lat = get_importance('dns_latency_ms')
        
        # CATEGORÍA 2: Pérdida
        peer_loss = get_importance('peer_loss_pct')
        dns_loss = get_importance('dns_loss_pct')
        
        # CATEGORÍA 3: Jitter
        peer_jitter = get_importance('peer_jitter_ms')
        dns_jitter = get_importance('dns_jitter_ms')
        
        # CATEGORÍA 4: Rolling Statistics
        rolling_mean = get_importance('rolling_mean')
        rolling_std = get_importance('rolling_std')
        rolling_p95 = get_importance('rolling_p95')
        rolling_total = rolling_mean + rolling_std + rolling_p95
        
        # CATEGORÍA 5: Features Derivadas
        latency_ratio = get_importance('latency_ratio')
        total_loss = get_importance('total_loss_pct')
        quality_index = get_importance('quality_index')
        derived_total = latency_ratio + total_loss + quality_index
        
        # CATEGORÍA 6: Degradación (SIN degradation_cycle)
        score_diff = get_importance('score_difference')
        margin_exceeds = get_importance('margin_exceeds_threshold')
        degradation_total = score_diff + margin_exceeds
        
        # CATEGORÍA 7: Contexto Temporal
        hour_importance = get_importance('hour_of_day')
        is_peak_importance = get_importance('is_peak_traffic')
        is_weekend_importance = get_importance('is_weekend')
        temporal_total = hour_importance + is_peak_importance + is_weekend_importance
        
        # CATEGORÍA 8: Detección Combinada
        z_score_peer = get_importance('z_score_peer')
        z_score_sev = get_importance('z_score_severity')
        absolute_sev = get_importance('absolute_severity')
        relative_diff = get_importance('relative_diff_ms')
        relative_sev = get_importance('relative_severity')
        combined_sev = get_importance('combined_severity')
        is_anomaly = get_importance('is_combined_anomaly')
        combined_detection_total = (
            z_score_peer + z_score_sev + absolute_sev + 
            relative_diff + relative_sev + combined_sev + is_anomaly
        )
        
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
            logger.info(f"   ➜ ✅ Pesos actuales son aproximadamente óptimos")
        
        # 2. Pérdida
        loss_importance = max(peer_loss, dns_loss)
        logger.info(f"\n⚠️ IMPORTANCIA DE PÉRDIDA:")
        logger.info(f"   Peer Loss: {peer_loss*100:.2f}%")
        logger.info(f"   DNS Loss:  {dns_loss*100:.2f}%")
        logger.info(f"   ➜ Pérdida es {loss_importance*100:.1f}% importante")
        
        # 3. Jitter
        jitter_importance = max(peer_jitter, dns_jitter)
        logger.info(f"\n⚡ IMPORTANCIA DE JITTER:")
        logger.info(f"   Peer Jitter: {peer_jitter*100:.2f}%")
        logger.info(f"   DNS Jitter:  {dns_jitter*100:.2f}%")
        logger.info(f"   ➜ Jitter es {jitter_importance*100:.1f}% importante")
        
        # 4. Rolling Statistics
        logger.info(f"\n📈 IMPORTANCIA DE ROLLING STATISTICS:")
        logger.info(f"   rolling_mean: {rolling_mean*100:.2f}%")
        logger.info(f"   rolling_std:  {rolling_std*100:.2f}%")
        logger.info(f"   rolling_p95:  {rolling_p95*100:.2f}%")
        logger.info(f"   Total:        {rolling_total*100:.2f}%")
        if rolling_total > 0.10:
            logger.info(f"   ➜ 🔥 ALTA IMPORTANCIA: Anomalías relativas son CRÍTICAS")
        elif rolling_total > 0.05:
            logger.info(f"   ➜ ✅ IMPORTANCIA MODERADA")
        else:
            logger.info(f"   ➜ ℹ️ BAJA IMPORTANCIA")
        
        # 5. Features Derivadas
        logger.info(f"\n🔗 IMPORTANCIA DE FEATURES DERIVADAS:")
        logger.info(f"   latency_ratio:  {latency_ratio*100:.2f}%")
        logger.info(f"   total_loss_pct: {total_loss*100:.2f}%")
        logger.info(f"   quality_index:  {quality_index*100:.2f}%")
        logger.info(f"   Total:          {derived_total*100:.2f}%")
        
        # 6. Degradación (SIN degradation_cycle)
        logger.info(f"\n🎚️ IMPORTANCIA DE DEGRADACIÓN (sin data leakage):")
        logger.info(f"   score_difference:         {score_diff*100:.2f}%")
        logger.info(f"   margin_exceeds_threshold: {margin_exceeds*100:.2f}%")
        logger.info(f"   Total:                    {degradation_total*100:.2f}%")
        logger.info(f"   ❌ ELIMINADO: degradation_cycle (data leakage)")
        
        # 7. Contexto Temporal
        logger.info(f"\n🕐 IMPORTANCIA CONTEXTUAL:")
        logger.info(f"   Hour of Day:  {hour_importance*100:.2f}%")
        logger.info(f"   Peak Traffic: {is_peak_importance*100:.2f}%")
        logger.info(f"   Weekend:      {is_weekend_importance*100:.2f}%")
        logger.info(f"   Total:        {temporal_total*100:.2f}%")
        
        # 8. Detección Combinada
        logger.info(f"\n🎯 IMPORTANCIA DE DETECCIÓN COMBINADA:")
        logger.info(f"   z_score_peer:        {z_score_peer*100:.2f}%")
        logger.info(f"   z_score_severity:    {z_score_sev*100:.2f}%")
        logger.info(f"   absolute_severity:   {absolute_sev*100:.2f}%")
        logger.info(f"   relative_diff_ms:    {relative_diff*100:.2f}%")
        logger.info(f"   relative_severity:   {relative_sev*100:.2f}%")
        logger.info(f"   combined_severity:   {combined_sev*100:.2f}%")
        logger.info(f"   is_combined_anomaly: {is_anomaly*100:.2f}%")
        logger.info(f"   Total:               {combined_detection_total*100:.2f}%")
        if combined_detection_total > 0.15:
            logger.info(f"   ➜ 🔥 CRÍTICO: La detección combinada es ESENCIAL")
        elif combined_detection_total > 0.05:
            logger.info(f"   ➜ ✅ IMPORTANTE: La detección combinada aporta valor")
        else:
            logger.info(f"   ➜ ℹ️ BAJA IMPORTANCIA")
        
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
            'Contexto Temporal': temporal_total,
            'Detección Combinada': combined_detection_total
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
        
        # === Resumen de CV ===
        if self.cv_scores:
            logger.info(f"\n📊 Cross-Validation Summary:")
            logger.info("-" * 80)
            for metric, values in self.cv_scores.items():
                mean_val = np.mean(values)
                std_val = np.std(values)
                logger.info(f"   {metric:12s}: {mean_val:.4f} ± {std_val:.4f}")
        
        # ✅ NUEVO: Información sobre target usado
        logger.info(f"\n🎯 Target usado: {self.target_column}")
        
        return {
            'peer_latency_weight': float(peer_weight),
            'dns_latency_weight': float(dns_weight),
            'loss_importance': float(loss_importance),
            'jitter_importance': float(jitter_importance),
            'rolling_importance': float(rolling_total),
            'derived_importance': float(derived_total),
            'degradation_importance': float(degradation_total),
            'context_importance': float(temporal_total),
            'combined_detection_importance': float(combined_detection_total),
            'all_importances': self.feature_importance.to_dict('list'),
            'cv_scores': self.cv_scores,
            'target_column': self.target_column,
            'recommendations': {
                'dynamic_thresholds': rolling_total > 0.10,
                'time_based_thresholds': temporal_total > 0.05,
                'use_compound_metrics': derived_total > 0.10,
                'bgp_context_critical': degradation_total > 0.15,
                'combined_detection_critical': combined_detection_total > 0.15
            }
        }
    
    def predict_failover_probability(self, metrics_dict):
        """Predice la probabilidad de failover para nuevas métricas"""
        if self.model is None:
            raise ValueError("Debes entrenar el modelo primero")
        
        if self.features_used is None:
            raise ValueError("Debes entrenar el modelo primero para conocer las features")
        
        # Validar features
        missing = [f for f in self.features_used if f not in metrics_dict]
        if missing:
            logger.warning(f"⚠️ Faltan {len(missing)} features: {missing}")
            for f in missing:
                metrics_dict[f] = 0
        
        X = pd.DataFrame([metrics_dict])[self.features_used]
        
        # Convertir booleanos a int
        for col in X.columns:
            if X[col].dtype == 'bool':
                X[col] = X[col].astype(int)
        
        prob = self.model.predict_proba(X)[0, 1]
        return prob


if __name__ == '__main__':
    # Ejemplo de uso
    logging.basicConfig(level=logging.INFO)
    logger.info("✅ xgboost_optimizer.py cargado correctamente")
    logger.info("Para usar: from xgboost_optimizer import ScoringWeightOptimizer")
