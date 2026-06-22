#!/usr/bin/env python3
"""
model_utils.py

Utilidades compartidas para todos los modelos ML
- Carga de datos desde ml_features
- Evaluación de modelos
- Reportes comunes
"""

import psycopg2
import pandas as pd
import numpy as np
import logging
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report, roc_auc_score
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MLDataLoader:
    """
    ✅ Carga datos desde ml_features para entrenamiento
    
    Características:
    ├─ Conecta a TimescaleDB
    ├─ Carga todas las features derivadas
    ├─ Incluye degradation_cycle (CRÍTICO)
    └─ Retorna X, y listos para ML
    """
    
    def __init__(self, timescaledb_host='timescaledb', timescaledb_port=5432,
                 timescaledb_db='bgp_failover_db', timescaledb_user='bgp_app',
                 timescaledb_password='bgp_app_password'):
        
        self.host = timescaledb_host
        self.port = timescaledb_port
        self.db = timescaledb_db
        self.user = timescaledb_user
        self.password = timescaledb_password
        self.conn = None
    
    def connect(self):
        """Conectar a TimescaleDB"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.db,
                user=self.user,
                password=self.password
            )
            logger.info(f"✅ Conectado a TimescaleDB en {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"❌ Error conectando: {e}")
            raise
    
    def load_ml_features(self, days=30):
        """
        ✅ Carga datos de ml_features para entrenamiento
        
        Args:
            days: Número de días históricos a cargar
        
        Returns:
            df: DataFrame con features + target
        """
        
        if not self.conn:
            self.connect()
        
        logger.info(f"📥 Cargando {days} días de datos de ml_features...")
        
        query = f"""
        SELECT
            time, provider,
            -- Raw metrics
            peer_latency_ms, dns_latency_ms,
            peer_loss_pct, dns_loss_pct,
            peer_jitter_ms, dns_jitter_ms,
            score,
            -- Derived features
            latency_ratio,
            total_loss_pct,
            quality_index,
            latency_trend_5min,
            latency_trend_15min,
            latency_velocity,
            latency_acceleration,
            loss_spike_detected,
            -- Rolling statistics
            peer_latency_mean_10,
            peer_latency_std_10,
            peer_latency_min_10,
            peer_latency_max_10,
            peer_latency_p95_10,
            -- Contextual features
            hour_of_day,
            day_of_week,
            is_business_hours,
            is_peak_traffic,
            is_weekend,
            provider_changes_last_hour,
            time_since_last_change_min,
            -- Provider features
            current_provider_score,
            alternative_provider_score,
            score_difference,
            margin_exceeds_threshold,
            -- ✅ Degradation (CRÍTICO)
            degradation_cycle,
            provider_changed,
            -- Target
            should_failover
        FROM ml_features
        WHERE time >= NOW() - INTERVAL '{days} days'
        ORDER BY time
        """
        
        try:
            df = pd.read_sql(query, self.conn)
            logger.info(f"✅ Cargados {len(df)} registros")
            logger.info(f"   Fecha: {df['time'].min()} a {df['time'].max()}")
            
            # Verificar degradation_cycle
            if 'degradation_cycle' in df.columns:
                logger.info(f"   ✓ degradation_cycle: {sorted(df['degradation_cycle'].unique())}")
            
            # Contar failovers únicos
            unique_failover_events = df[df['should_failover'] == 1]['time'].nunique()
            logger.info(f"   ✓ Failovers ÚNICOS: {unique_failover_events}")
            logger.info(f"   ✓ Registros clase 1: {(df['should_failover'] == 1).sum()}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}")
            raise
    
    def close(self):
        """Cerrar conexión"""
        if self.conn:
            self.conn.close()


class MLModelEvaluator:
    """
    ✅ Evaluación estándar de modelos clasificadores
    
    Características:
    ├─ Cálculo de métricas comunes
    ├─ Reporte de clasificación
    ├─ Matriz de confusión
    └─ Logueo estructurado
    """
    
    @staticmethod
    def evaluate_model(y_test, y_pred, y_pred_proba=None, model_name="Model"):
        """
        Evalúa modelo y retorna métricas
        
        Args:
            y_test: Labels reales
            y_pred: Predicciones
            y_pred_proba: Probabilidades (opcional)
            model_name: Nombre del modelo para logging
        
        Returns:
            dict: Diccionario con métricas
        """
        
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 EVALUACIÓN: {model_name}")
        logger.info(f"{'='*80}")
        
        # Métricas básicas
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        
        logger.info(f"\n✅ Métricas:")
        logger.info(f"   Accuracy:  {accuracy:.4f}")
        logger.info(f"   Precision: {precision:.4f}")
        logger.info(f"   Recall:    {recall:.4f}")
        logger.info(f"   F1 Score:  {f1:.4f}")
        
        # ROC-AUC si hay probabilidades
        metrics = {
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1': f1
        }
        
        if y_pred_proba is not None:
            try:
                auc = roc_auc_score(y_test, y_pred_proba)
                logger.info(f"   ROC-AUC:   {auc:.4f}")
                metrics['auc'] = auc
            except:
                logger.warning(f"   ROC-AUC:   N/A")
        
        # Matriz de confusión
        cm = confusion_matrix(y_test, y_pred)
        logger.info(f"\n📈 Matriz de Confusión:")
        logger.info(f"   TN={cm[0,0]}, FP={cm[0,1]}")
        logger.info(f"   FN={cm[1,0]}, TP={cm[1,1]}")
        
        # Reporte de clasificación
        logger.info(f"\n📋 Reporte de Clasificación:")
        report = classification_report(y_test, y_pred, 
                                      target_names=['No Failover', 'Failover'],
                                      zero_division=0)
        logger.info(f"\n{report}")
        
        return metrics
    
    @staticmethod
    def log_feature_importance(feature_importance, feature_names, model_name="Model", top_n=10):
        """
        Loguea feature importance
        
        Args:
            feature_importance: Array de importancias
            feature_names: Nombres de features
            model_name: Nombre del modelo
            top_n: Top N features a mostrar
        """
        
        logger.info(f"\n{'='*80}")
        logger.info(f"📊 FEATURE IMPORTANCE: {model_name}")
        logger.info(f"{'='*80}")
        
        # Crear DataFrame
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': feature_importance
        }).sort_values('importance', ascending=False)
        
        logger.info(f"\n🔍 Top {top_n} Features:")
        for idx, (_, row) in enumerate(importance_df.head(top_n).iterrows(), 1):
            feature = row['feature']
            importance = row['importance']
            pct = importance * 100 if importance <= 1 else importance
            
            # Barra visual
            bar = "█" * int(pct / 2)
            logger.info(f"   {idx:2d}. {feature:30s} {bar:40s} {pct:6.2f}%")
        
        # Verificar degradation_cycle
        if 'degradation_cycle' in feature_names:
            idx = feature_names.index('degradation_cycle')
            deg_importance = feature_importance[idx]
            logger.info(f"\n✅ degradation_cycle Importance: {deg_importance*100:.2f}%")
        
        return importance_df


class MLPipelineHelper:
    """
    ✅ Helpers para pipeline ML común
    """
    
    @staticmethod
    def prepare_data(df, exclude_cols=None):
        """
        Prepara X y y del dataframe
        
        Args:
            df: DataFrame con features y target
            exclude_cols: Columnas a excluir (time, provider, etc.)
        
        Returns:
            X, y, feature_names
        """
        
        if exclude_cols is None:
            exclude_cols = ['time', 'provider', 'should_failover']
        
        # Feature columns
        feature_cols = [col for col in df.columns if col not in exclude_cols]
        
        X = df[feature_cols].copy()
        y = df['should_failover'].copy()
        
        # Manejo de booleanos
        bool_cols = X.select_dtypes(include=['bool']).columns
        if len(bool_cols) > 0:
            X[bool_cols] = X[bool_cols].astype(int)
        
        # Rellenar NaN
        X = X.fillna(X.mean(numeric_only=True))
        
        logger.info(f"\n✅ Data prepared:")
        logger.info(f"   Features: {len(feature_cols)}")
        logger.info(f"   Samples: {len(X)}")
        logger.info(f"   Class distribution: {y.value_counts().to_dict()}")
        
        return X, y, feature_cols
    
    @staticmethod
    def split_data(X, y, test_size=0.2, random_state=42):
        """
        Split train/test con stratification
        """
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, 
            test_size=test_size, 
            random_state=random_state,
            stratify=y
        )
        
        logger.info(f"\n✅ Train/Test split:")
        logger.info(f"   Training: {len(X_train)} samples")
        logger.info(f"   Testing:  {len(X_test)} samples")
        
        return X_train, X_test, y_train, y_test
    
    @staticmethod
    def scale_features(X_train, X_test):
        """
        Escalar features (para modelos como Logistic Regression)
        """
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        logger.info(f"\n✅ Features scaled (StandardScaler)")
        
        return X_train_scaled, X_test_scaled, scaler


if __name__ == '__main__':
    # Test
    loader = MLDataLoader()
    df = loader.load_ml_features(days=1)
    X, y, feature_cols = MLPipelineHelper.prepare_data(df)
    logger.info(f"\n✅ Test completado: {X.shape}")
