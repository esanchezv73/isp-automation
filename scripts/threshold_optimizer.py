#!/usr/bin/env python3
"""
threshold_optimizer.py - VERSIÓN CORREGIDA
Optimiza thresholds usando análisis de degradación REAL
├─ Fuente: ml_features
├─ Método: Análisis de percentiles en ciclos PRE-failover
├─ Objetivo: Encontrar thresholds basados en latencias del provider DEGRADADO
└─ CORRECCIÓN: Conteo correcto de failovers únicos (no duplicados)
"""
import logging
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_curve, auc, confusion_matrix,
    precision_recall_curve, f1_score, precision_score, recall_score
)
from model_utils import MLDataLoader, MLPipelineHelper

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ThresholdOptimizerCorrected:
    """
    ✅ CORREGIDO: Analiza thresholds basándose en:
    1. Ciclos de degradación (degradation_cycle = 1, 2, 3)
    2. Provider que PERDIÓ (score_difference > 0)
    3. Diferencia REAL de scores (score_difference absoluta)
    4. Conteo correcto de failovers únicos (no duplicados)
    """
    
    def __init__(self):
        self.model = None
        self.scaler = None
        self.X_test = None
        self.y_test = None
        self.y_pred_proba = None
        self.fpr = None
        self.tpr = None
        self.thresholds = None
        self.roc_auc = None
        self.optimal_threshold = None
    
    def load_and_prepare_data(self, days=30):
        """Cargar datos con filtros correctos"""
        logger.info("=" * 80)
        logger.info("📊 THRESHOLD OPTIMIZATION (VERSIÓN CORREGIDA)")
        logger.info("=" * 80)
        logger.info("\nPASO 1: Cargar datos")
        logger.info("-" * 80)
        
        loader = MLDataLoader()
        df = loader.load_ml_features(days=days)
        
        # ✅ CORRECCIÓN: Crear columna failover_event (solo provider que PERDIÓ)
        # Un failover event ocurre cuando:
        # 1. provider_changed = True
        # 2. score_difference > 0 (este provider tiene score peor que el alternativo)
        df['failover_event'] = (
            (df['provider_changed'] == True) & 
            (df['score_difference'] > 0)
        ).astype(int)
        
        # Identificar degradación
        df['is_degraded'] = (df['degradation_cycle'] > 0).astype(int)
        
        # Identificar provider degradado
        df['is_provider_degraded'] = df.apply(
            lambda row: row['provider'] if row['score_difference'] > 0 else None,
            axis=1
        )
        
        # ✅ CORRECCIÓN: Conteo correcto de failovers
        total_failover_records = df['provider_changed'].sum()
        unique_failover_events = df['failover_event'].sum()
        unique_failover_times = df[df['failover_event'] == 1]['time'].nunique()
        
        logger.info(f"✅ Cargados {len(df)} registros")
        logger.info(f"   Período: {df['time'].min()} a {df['time'].max()}")
        logger.info(f"   Degradación detectada: {df['is_degraded'].sum()} registros")
        logger.info(f"   Registros con provider_changed=True: {total_failover_records} (2 por cada failover)")
        logger.info(f"   ✅ Failovers ÚNICOS (eventos): {unique_failover_events}")
        logger.info(f"   ✅ Failovers ÚNICOS (ciclos): {unique_failover_times}")
        
        return df
    
    def analyze_latency_thresholds_corrected(self, df):
        """
        ✅ CORREGIDO: Analiza latencias del provider DEGRADADO
        Solo en ciclos donde degradation_cycle >= 1
        """
        logger.info("\nPASO 2: Analizar thresholds de latencia (CORREGIDO)")
        logger.info("-" * 80)
        
        # ✅ FILTRO CORRECTO: Solo registros con degradación
        degraded_data = df[df['degradation_cycle'] > 0].copy()
        
        if len(degraded_data) == 0:
            logger.warning("⚠️ No hay datos de degradación para analizar")
            return {}
        
        logger.info(f"\n📊 Análisis de {len(degraded_data)} registros con degradación:")
        logger.info(f"   degradation_cycle distribution:")
        for cycle in sorted(degraded_data['degradation_cycle'].unique()):
            count = (degraded_data['degradation_cycle'] == cycle).sum()
            logger.info(f"     Ciclo {cycle}: {count} registros")
        
        # ✅ Separar por provider degradado vs saludable
        # Provider degradado = tiene score_difference > 0 (su score es peor)
        degraded_provider_mask = degraded_data['score_difference'] > 0
        degraded_records = degraded_data[degraded_provider_mask]
        
        logger.info(f"\n📈 Latencias del provider DEGRADADO (n={len(degraded_records)}):")
        logger.info(f"   peer_latency:")
        logger.info(f"     Media: {degraded_records['peer_latency_ms'].mean():.1f} ± {degraded_records['peer_latency_ms'].std():.1f} ms")
        logger.info(f"     p50: {degraded_records['peer_latency_ms'].quantile(0.50):.1f} ms")
        logger.info(f"     p75: {degraded_records['peer_latency_ms'].quantile(0.75):.1f} ms")
        logger.info(f"     p90: {degraded_records['peer_latency_ms'].quantile(0.90):.1f} ms")
        logger.info(f"     p95: {degraded_records['peer_latency_ms'].quantile(0.95):.1f} ms")
        logger.info(f"     Max: {degraded_records['peer_latency_ms'].max():.1f} ms")
        
        logger.info(f"\n   dns_latency:")
        logger.info(f"     Media: {degraded_records['dns_latency_ms'].mean():.1f} ± {degraded_records['dns_latency_ms'].std():.1f} ms")
        logger.info(f"     p50: {degraded_records['dns_latency_ms'].quantile(0.50):.1f} ms")
        logger.info(f"     p75: {degraded_records['dns_latency_ms'].quantile(0.75):.1f} ms")
        logger.info(f"     p90: {degraded_records['dns_latency_ms'].quantile(0.90):.1f} ms")
        logger.info(f"     p95: {degraded_records['dns_latency_ms'].quantile(0.95):.1f} ms")
        logger.info(f"     Max: {degraded_records['dns_latency_ms'].max():.1f} ms")
        
        # ✅ Calcular thresholds basados en percentiles
        peer_p95 = degraded_records['peer_latency_ms'].quantile(0.95)
        dns_p95 = degraded_records['dns_latency_ms'].quantile(0.95)
        peer_p90 = degraded_records['peer_latency_ms'].quantile(0.90)
        dns_p90 = degraded_records['dns_latency_ms'].quantile(0.90)
        
        logger.info(f"\n💡 RECOMENDACIONES (basadas en p95 de provider degradado):")
        logger.info(f"   peer_warning:  {peer_p90:.1f} ms (actual: 12 ms)")
        logger.info(f"   peer_critical: {peer_p95:.1f} ms (actual: 25 ms)")
        logger.info(f"   dns_warning:   {dns_p90:.1f} ms (actual: 15 ms)")
        logger.info(f"   dns_critical:  {dns_p95:.1f} ms (actual: 30 ms)")
        
        return {
            'peer_warning': peer_p90,
            'peer_critical': peer_p95,
            'dns_warning': dns_p90,
            'dns_critical': dns_p95
        }
    
    def analyze_switch_margin_corrected(self, df):
        """
        ✅ CORREGIDO: Analiza score_difference ABSOLUTA
        Solo en momentos de degradación
        """
        logger.info("\nPASO 3: Analizar switch_margin (CORREGIDO)")
        logger.info("-" * 80)
        
        # Solo ciclos de degradación
        degraded_data = df[df['degradation_cycle'] > 0].copy()
        
        if len(degraded_data) == 0:
            logger.warning("⚠️ No hay datos de degradación")
            return 5.0  # Default
        
        # ✅ Usar valor absoluto de score_difference
        degraded_data['score_diff_abs'] = degraded_data['score_difference'].abs()
        
        logger.info(f"\n📊 Diferencia de scores en degradación (n={len(degraded_data)}):")
        logger.info(f"   Media: {degraded_data['score_diff_abs'].mean():.2f} ± {degraded_data['score_diff_abs'].std():.2f}")
        logger.info(f"   Min: {degraded_data['score_diff_abs'].min():.2f}")
        logger.info(f"   p50: {degraded_data['score_diff_abs'].quantile(0.50):.2f}")
        logger.info(f"   p75: {degraded_data['score_diff_abs'].quantile(0.75):.2f}")
        logger.info(f"   p90: {degraded_data['score_diff_abs'].quantile(0.90):.2f}")
        
        # ✅ Recomendar switch_margin basado en p75
        switch_margin_p75 = degraded_data['score_diff_abs'].quantile(0.75)
        switch_margin_p50 = degraded_data['score_diff_abs'].quantile(0.50)
        
        logger.info(f"\n💡 RECOMENDACIÓN:")
        logger.info(f"   switch_margin: {switch_margin_p75:.2f} (p75)")
        logger.info(f"   Alternativa conservadora: {switch_margin_p50:.2f} (p50)")
        logger.info(f"   Actual: 5.0")
        
        # Validar que no sea demasiado bajo
        if switch_margin_p75 < 3:
            logger.warning(f"   ⚠️ Advertencia: switch_margin muy bajo ({switch_margin_p75:.2f})")
            logger.warning(f"   ➜ Puede causar FLAPPING. Usar mínimo 3.0")
            return max(switch_margin_p75, 3.0)
        
        return switch_margin_p75
    
    def train_logistic_regression(self, df):
        """
        Entrenar modelo para probabilidad de failover
        ✅ CORREGIDO: Usar failover_event como target (no provider_changed)
        """
        logger.info("\nPASO 4: Entrenar Logistic Regression")
        logger.info("-" * 80)
        
        # ✅ CORRECCIÓN: Usar failover_event como target
        X = df[['peer_latency_ms', 'dns_latency_ms', 'score_difference']].copy()
        y = df['failover_event'].copy()  # ✅ En lugar de is_degraded
        
        # Validar que hay suficientes eventos positivos
        if y.sum() < 10:
            logger.warning(f"⚠️ Solo {y.sum()} eventos de failover. Modelo puede ser inestable.")
        
        # Split
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Escalar
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        self.X_test = X_test_scaled
        
        # Entrenar
        logger.info("🔄 Entrenando modelo logístico...")
        self.model = LogisticRegression(max_iter=1000, random_state=42)
        self.model.fit(X_train_scaled, y_train)
        
        # Predicciones
        self.y_pred_proba = self.model.predict_proba(X_test_scaled)[:, 1]
        self.y_test = y_test
        
        # ROC Curve
        self.fpr, self.tpr, self.thresholds = roc_curve(y_test, self.y_pred_proba)
        self.roc_auc = auc(self.fpr, self.tpr)
        
        logger.info(f"✅ Modelo entrenado")
        logger.info(f"   ROC-AUC: {self.roc_auc:.4f}")
        
        # Encontrar threshold óptimo
        best_f1 = 0
        best_threshold = 0.5
        
        for thresh in np.arange(0.1, 0.9, 0.05):
            y_pred = (self.y_pred_proba >= thresh).astype(int)
            f1 = f1_score(y_test, y_pred, zero_division=0)
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = thresh
        
        self.optimal_threshold = best_threshold
        logger.info(f"   Threshold óptimo: {best_threshold:.2f}")
        logger.info(f"   F1-score: {best_f1:.4f}")
    
    def plot_roc_curve(self, save_path=None):
        """Plotear ROC Curve (con manejo de errores)"""
        logger.info("\nPASO 5: Generar visualizaciones")
        logger.info("-" * 80)
        
        try:
            plt.figure(figsize=(10, 8))
            plt.plot(self.fpr, self.tpr, 'b-', linewidth=2, 
                    label=f'ROC Curve (AUC = {self.roc_auc:.3f})')
            plt.plot([0, 1], [0, 1], 'k--', linewidth=1, label='Random Classifier')
            plt.xlabel('False Positive Rate', fontsize=12)
            plt.ylabel('True Positive Rate', fontsize=12)
            plt.title('ROC Curve - Detección de Degradación', fontsize=14)
            plt.legend(loc='lower right', fontsize=11)
            plt.grid(True, alpha=0.3)
            
            if save_path:
                try:
                    plt.savefig(save_path, dpi=150, bbox_inches='tight')
                    logger.info(f"✅ Gráfico guardado: {save_path}")
                except Exception as e:
                    logger.warning(f"⚠️ No se pudo guardar el gráfico: {e}")
                    logger.warning(f"   Continuando sin guardar...")
            
            plt.close()
        except Exception as e:
            logger.error(f"❌ Error generando gráfico: {e}")
    
    def generate_report(self, latency_thresholds, switch_margin):
        """Generar reporte final"""
        logger.info("\n" + "=" * 80)
        logger.info("📋 REPORTE FINAL: THRESHOLDS OPTIMIZADOS")
        logger.info("=" * 80)
        
        logger.info(f"\n🎯 THRESHOLDS RECOMENDADOS:")
        logger.info(f"   peer_warning:  {latency_thresholds.get('peer_warning', 12):.1f} ms")
        logger.info(f"   peer_critical: {latency_thresholds.get('peer_critical', 25):.1f} ms")
        logger.info(f"   dns_warning:   {latency_thresholds.get('dns_warning', 15):.1f} ms")
        logger.info(f"   dns_critical:  {latency_thresholds.get('dns_critical', 30):.1f} ms")
        logger.info(f"   switch_margin: {switch_margin:.2f} puntos")
        
        logger.info(f"\n📝 ACTUALIZAR EN bgp_failover_config.py:")
        logger.info(f"""
LATENCY_THRESHOLDS = {{
    'peer_warning':  {latency_thresholds.get('peer_warning', 12):.0f},
    'peer_critical': {latency_thresholds.get('peer_critical', 25):.0f},
    'dns_warning':   {latency_thresholds.get('dns_warning', 15):.0f},
    'dns_critical':  {latency_thresholds.get('dns_critical', 30):.0f},
    'switch_margin': {switch_margin:.0f}
}}
        """)
        
        logger.info(f"\n✅ VALIDACIÓN:")
        logger.info(f"   ROC-AUC: {self.roc_auc:.4f} (>0.8 = excelente)")
        logger.info(f"   Datos analizados: {len(self.X_test) if self.X_test is not None else 0} samples")


def main():
    """Ejecutar pipeline completo corregido"""
    optimizer = ThresholdOptimizerCorrected()
    
    # 1. Cargar datos
    df = optimizer.load_and_prepare_data(days=30)
    
    # 2. Analizar latencias (CORREGIDO)
    latency_thresholds = optimizer.analyze_latency_thresholds_corrected(df)
    
    # 3. Analizar switch_margin (CORREGIDO)
    switch_margin = optimizer.analyze_switch_margin_corrected(df)
    
    # 4. Entrenar modelo
    optimizer.train_logistic_regression(df)
    
    # 5. Plotear ROC
    optimizer.plot_roc_curve(save_path='/tmp/roc_curve.png')
    
    # 6. Reporte final
    optimizer.generate_report(latency_thresholds, switch_margin)
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ OPTIMIZACIÓN COMPLETADA (VERSIÓN CORREGIDA)")
    logger.info("=" * 80)
    
    return latency_thresholds, switch_margin


if __name__ == '__main__':
    main()
