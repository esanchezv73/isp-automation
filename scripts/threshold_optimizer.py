#!/usr/bin/env python3
"""
threshold_optimizer.py - VERSIÓN CON VALIDACIONES CORREGIDAS
Optimiza thresholds usando análisis de degradación REAL
├─ Fuente: ml_features
├─ Método: Análisis de percentiles en ciclos PRE-failover
├─ Objetivo: Encontrar thresholds basados en latencias del provider DEGRADADO
├─ ✅ Conteo correcto de failovers únicos
├─ ✅ Conteo correcto de degradaciones únicas
└─ ✅ CORRECCIÓN: Umbrales completos (warning/degraded/critical)
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


class ThresholdOptimizerValidated:
    """
    ✅ CORREGIDO: Analiza thresholds con validaciones de coherencia
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
        
        # ✅ Umbrales actuales (defaults)
        self.current_thresholds = {
            'peer_warning': 12.0,
            'peer_degraded': 15.0,
            'peer_critical': 25.0,
            'dns_warning': 15.0,
            'dns_degraded': 20.0,
            'dns_critical': 30.0,
            'z_warning': 2.0,
            'z_degraded': 2.5,
            'z_critical': 3.0,
            'rel_warning': 5.0,
            'rel_degraded': 10.0,
            'rel_critical': 15.0,
            'switch_margin': 5.0
        }
        
        # ✅ Mínimo de datos requeridos
        self.min_samples_per_threshold = 10
    
    def validate_threshold(self, proposed_value, current_value, metric_name):
        """
        ✅ Valida que el umbral propuesto sea coherente
        """
        # Validar que sea positivo
        if proposed_value <= 0:
            logger.warning(f"   ⚠️ {metric_name}: Valor negativo ({proposed_value:.2f})")
            logger.warning(f"      ➜ Manteniendo valor actual: {current_value:.2f}")
            return current_value
        
        # Validar que no se desvíe más del 50% del valor actual
        deviation = abs(proposed_value - current_value) / current_value
        if deviation > 0.5:
            logger.warning(f"   ⚠️ {metric_name}: Desviación muy alta ({deviation*100:.1f}%)")
            logger.warning(f"      ➜ Propuesto: {proposed_value:.2f}, Actual: {current_value:.2f}")
            logger.warning(f"      ➜ Manteniendo valor actual por seguridad")
            return current_value
        
        return proposed_value
    
    def validate_threshold_hierarchy(self, thresholds_dict, metric_prefix):
        """
        ✅ CORREGIDO: Valida que warning < degraded < critical
        Maneja el caso donde algún valor sea None
        """
        warning = thresholds_dict.get(f'{metric_prefix}_warning')
        degraded = thresholds_dict.get(f'{metric_prefix}_degraded')
        critical = thresholds_dict.get(f'{metric_prefix}_critical')
        
        # ✅ CORRECCIÓN: Si algún valor es None, mantener los valores actuales
        if warning is None or degraded is None or critical is None:
            logger.warning(f"   ⚠️ Jerarquía incompleta para {metric_prefix}")
            logger.warning(f"      ➜ Manteniendo valores actuales")
            return {
                f'{metric_prefix}_warning': self.current_thresholds[f'{metric_prefix}_warning'],
                f'{metric_prefix}_degraded': self.current_thresholds[f'{metric_prefix}_degraded'],
                f'{metric_prefix}_critical': self.current_thresholds[f'{metric_prefix}_critical']
            }
        
        # Validar jerarquía
        if warning >= degraded or degraded >= critical:
            logger.warning(f"   ⚠️ Jerarquía inválida para {metric_prefix}:")
            logger.warning(f"      warning={warning:.2f}, degraded={degraded:.2f}, critical={critical:.2f}")
            logger.warning(f"      ➜ Manteniendo valores actuales")
            return {
                f'{metric_prefix}_warning': self.current_thresholds[f'{metric_prefix}_warning'],
                f'{metric_prefix}_degraded': self.current_thresholds[f'{metric_prefix}_degraded'],
                f'{metric_prefix}_critical': self.current_thresholds[f'{metric_prefix}_critical']
            }
        
        return thresholds_dict
    
    def load_and_prepare_data(self, days=30):
        """Cargar datos con filtros correctos"""
        logger.info("=" * 80)
        logger.info("📊 THRESHOLD OPTIMIZATION (CON VALIDACIONES)")
        logger.info("=" * 80)
        logger.info("\nPASO 1: Cargar datos")
        logger.info("-" * 80)
        
        loader = MLDataLoader()
        df = loader.load_ml_features(days=days)
        
        # Crear columna failover_event (solo provider que PERDIÓ)
        df['failover_event'] = (
            (df['provider_changed'] == True) & 
            (df['score_difference'] > 0)
        ).astype(int)
        
        # Identificar degradación
        df['is_degraded'] = (df['degradation_cycle'] > 0).astype(int)
        
        # Conteos
        total_records = len(df)
        degraded_records = df['is_degraded'].sum()
        failover_records = df['provider_changed'].sum()
        unique_failovers = df[df['failover_event'] == 1]['time'].nunique()
        
        logger.info(f"✅ Cargados {total_records} registros")
        logger.info(f"   Período: {df['time'].min()} a {df['time'].max()}")
        logger.info(f"   Degradación detectada: {degraded_records} registros")
        logger.info(f"   Registros con provider_changed=True: {failover_records}")
        logger.info(f"   ✅ Failovers ÚNICOS: {unique_failovers}")
        
        # Validar que hay suficientes datos
        if unique_failovers < 10:
            logger.warning(f"\n⚠️ ADVERTENCIA: Solo {unique_failovers} failovers únicos")
            logger.warning(f"   ➜ Se requieren mínimo 10 failovers para análisis confiable")
            logger.warning(f"   ➜ Los umbrales propuestos pueden no ser confiables")
        
        return df
    
    def analyze_absolute_thresholds(self, df):
        """
        ✅ MEJORADO: Analiza umbrales absolutos con jerarquía completa
        warning = p95, degraded = p99, critical = max
        """
        logger.info("\nPASO 2: Analizar umbrales ABSOLUTOS")
        logger.info("-" * 80)
        
        # Solo registros con degradación
        degraded_data = df[df['degradation_cycle'] > 0].copy()
        
        if len(degraded_data) == 0:
            logger.warning("⚠️ No hay datos de degradación para analizar")
            return {
                'peer_warning': self.current_thresholds['peer_warning'],
                'peer_degraded': self.current_thresholds['peer_degraded'],
                'peer_critical': self.current_thresholds['peer_critical'],
                'dns_warning': self.current_thresholds['dns_warning'],
                'dns_degraded': self.current_thresholds['dns_degraded'],
                'dns_critical': self.current_thresholds['dns_critical']
            }
        
        # Conteo único de degradaciones
        unique_degradations = degraded_data.groupby('time')['degradation_cycle'].max()
        cycle_distribution = unique_degradations.value_counts().sort_index()
        
        logger.info(f"\n📊 Análisis de {len(degraded_data)} registros con degradación:")
        logger.info(f"   degradation_cycle distribution (degradaciones únicas):")
        for cycle, count in cycle_distribution.items():
            logger.info(f"     Ciclo {cycle}: {count} degradaciones únicas")
        
        # Provider degradado (score_difference > 0)
        degraded_provider_mask = degraded_data['score_difference'] > 0
        degraded_records = degraded_data[degraded_provider_mask]
        
        if len(degraded_records) < self.min_samples_per_threshold:
            logger.warning(f"\n⚠️ Insuficientes registros del provider degradado: {len(degraded_records)}")
            logger.warning(f"   ➜ Se requieren mínimo {self.min_samples_per_threshold}")
            logger.warning(f"   ➜ Manteniendo umbrales actuales")
            return {
                'peer_warning': self.current_thresholds['peer_warning'],
                'peer_degraded': self.current_thresholds['peer_degraded'],
                'peer_critical': self.current_thresholds['peer_critical'],
                'dns_warning': self.current_thresholds['dns_warning'],
                'dns_degraded': self.current_thresholds['dns_degraded'],
                'dns_critical': self.current_thresholds['dns_critical']
            }
        
        logger.info(f"\n📈 Latencias del provider DEGRADADO (n={len(degraded_records)}):")
        
        # ✅ CORRECCIÓN: Calcular p95, p99 y max
        peer_p95 = degraded_records['peer_latency_ms'].quantile(0.95)
        peer_p99 = degraded_records['peer_latency_ms'].quantile(0.99)
        peer_max = degraded_records['peer_latency_ms'].max()
        
        dns_p95 = degraded_records['dns_latency_ms'].quantile(0.95)
        dns_p99 = degraded_records['dns_latency_ms'].quantile(0.99)
        dns_max = degraded_records['dns_latency_ms'].max()
        
        logger.info(f"   peer_latency:")
        logger.info(f"     Media: {degraded_records['peer_latency_ms'].mean():.1f} ± {degraded_records['peer_latency_ms'].std():.1f} ms")
        logger.info(f"     p95: {peer_p95:.1f} ms")
        logger.info(f"     p99: {peer_p99:.1f} ms")
        logger.info(f"     Max: {peer_max:.1f} ms")
        
        logger.info(f"\n   dns_latency:")
        logger.info(f"     Media: {degraded_records['dns_latency_ms'].mean():.1f} ± {degraded_records['dns_latency_ms'].std():.1f} ms")
        logger.info(f"     p95: {dns_p95:.1f} ms")
        logger.info(f"     p99: {dns_p99:.1f} ms")
        logger.info(f"     Max: {dns_max:.1f} ms")
        
        # ✅ CORRECCIÓN: Umbrales completos (warning, degraded, critical)
        proposed_thresholds = {
            'peer_warning': peer_p95,
            'peer_degraded': peer_p99,  # ✅ NUEVO
            'peer_critical': peer_max,
            'dns_warning': dns_p95,
            'dns_degraded': dns_p99,    # ✅ NUEVO
            'dns_critical': dns_max
        }
        
        # Validar umbrales individuales
        validated_thresholds = {}
        for key, value in proposed_thresholds.items():
            validated_thresholds[key] = self.validate_threshold(
                value, 
                self.current_thresholds[key], 
                key
            )
        
        # Validar jerarquías
        validated_thresholds = self.validate_threshold_hierarchy(validated_thresholds, 'peer')
        validated_thresholds = self.validate_threshold_hierarchy(validated_thresholds, 'dns')
        
        logger.info(f"\n💡 RECOMENDACIONES (umbrales absolutos validados):")
        for key, value in validated_thresholds.items():
            current = self.current_thresholds[key]
            status = "✅" if value == current else "⚠️"
            logger.info(f"   {status} {key}: {value:.1f} ms (actual: {current:.0f} ms)")
        
        return validated_thresholds
    
    def analyze_zscore_thresholds(self, df):
        """Analizar umbrales de Z-score con validaciones"""
        logger.info("\nPASO 3: Analizar umbrales de Z-SCORE")
        logger.info("-" * 80)
        
        degraded_data = df[df['degradation_cycle'] > 0].copy()
        
        if len(degraded_data) == 0:
            logger.warning("⚠️ No hay datos de degradación")
            return {
                'z_warning': self.current_thresholds['z_warning'],
                'z_degraded': self.current_thresholds['z_degraded'],
                'z_critical': self.current_thresholds['z_critical']
            }
        
        degraded_provider_mask = degraded_data['score_difference'] > 0
        degraded_records = degraded_data[degraded_provider_mask]
        
        if len(degraded_records) < self.min_samples_per_threshold:
            logger.warning(f"\n⚠️ Insuficientes registros: {len(degraded_records)}")
            logger.warning(f"   ➜ Manteniendo umbrales actuales")
            return {
                'z_warning': self.current_thresholds['z_warning'],
                'z_degraded': self.current_thresholds['z_degraded'],
                'z_critical': self.current_thresholds['z_critical']
            }
        
        logger.info(f"\n📊 Z-score del provider DEGRADADO (n={len(degraded_records)}):")
        
        z_p75 = degraded_records['z_score_peer'].quantile(0.75)
        z_p90 = degraded_records['z_score_peer'].quantile(0.90)
        z_p95 = degraded_records['z_score_peer'].quantile(0.95)
        z_max = degraded_records['z_score_peer'].max()
        
        logger.info(f"   z_score_peer:")
        logger.info(f"     Media: {degraded_records['z_score_peer'].mean():.2f} ± {degraded_records['z_score_peer'].std():.2f}")
        logger.info(f"     p75: {z_p75:.2f}")
        logger.info(f"     p90: {z_p90:.2f}")
        logger.info(f"     p95: {z_p95:.2f}")
        logger.info(f"     Max: {z_max:.2f}")
        
        proposed_thresholds = {
            'z_warning': z_p90,
            'z_degraded': z_p95,
            'z_critical': z_max
        }
        
        validated_thresholds = {}
        for key, value in proposed_thresholds.items():
            validated_thresholds[key] = self.validate_threshold(
                value, 
                self.current_thresholds[key], 
                key
            )
        
        validated_thresholds = self.validate_threshold_hierarchy(validated_thresholds, 'z')
        
        logger.info(f"\n💡 RECOMENDACIONES (Z-score validados):")
        for key, value in validated_thresholds.items():
            current = self.current_thresholds[key]
            status = "✅" if value == current else "⚠️"
            logger.info(f"   {status} {key}: {value:.2f} (actual: {current:.1f})")
        
        return validated_thresholds
    
    def analyze_relative_thresholds(self, df):
        """Analizar umbrales de diferencia relativa con validaciones"""
        logger.info("\nPASO 4: Analizar umbrales de DIFERENCIA RELATIVA")
        logger.info("-" * 80)
        
        degraded_data = df[df['degradation_cycle'] > 0].copy()
        
        if len(degraded_data) == 0:
            logger.warning("⚠️ No hay datos de degradación")
            return {
                'rel_warning': self.current_thresholds['rel_warning'],
                'rel_degraded': self.current_thresholds['rel_degraded'],
                'rel_critical': self.current_thresholds['rel_critical']
            }
        
        degraded_provider_mask = degraded_data['score_difference'] > 0
        degraded_records = degraded_data[degraded_provider_mask]
        
        if len(degraded_records) < self.min_samples_per_threshold:
            logger.warning(f"\n⚠️ Insuficientes registros: {len(degraded_records)}")
            logger.warning(f"   ➜ Manteniendo umbrales actuales")
            return {
                'rel_warning': self.current_thresholds['rel_warning'],
                'rel_degraded': self.current_thresholds['rel_degraded'],
                'rel_critical': self.current_thresholds['rel_critical']
            }
        
        logger.info(f"\n📊 Diferencia relativa del provider DEGRADADO (n={len(degraded_records)}):")
        
        rel_p75 = degraded_records['relative_diff_ms'].quantile(0.75)
        rel_p90 = degraded_records['relative_diff_ms'].quantile(0.90)
        rel_p95 = degraded_records['relative_diff_ms'].quantile(0.95)
        rel_max = degraded_records['relative_diff_ms'].max()
        
        logger.info(f"   relative_diff_ms:")
        logger.info(f"     Media: {degraded_records['relative_diff_ms'].mean():.2f} ± {degraded_records['relative_diff_ms'].std():.2f} ms")
        logger.info(f"     p75: {rel_p75:.2f} ms")
        logger.info(f"     p90: {rel_p90:.2f} ms")
        logger.info(f"     p95: {rel_p95:.2f} ms")
        logger.info(f"     Max: {rel_max:.2f} ms")
        
        proposed_thresholds = {
            'rel_warning': rel_p90,
            'rel_degraded': rel_p95,
            'rel_critical': rel_max
        }
        
        validated_thresholds = {}
        for key, value in proposed_thresholds.items():
            validated_thresholds[key] = self.validate_threshold(
                value, 
                self.current_thresholds[key], 
                key
            )
        
        validated_thresholds = self.validate_threshold_hierarchy(validated_thresholds, 'rel')
        
        logger.info(f"\n💡 RECOMENDACIONES (diferencia relativa validada):")
        for key, value in validated_thresholds.items():
            current = self.current_thresholds[key]
            status = "✅" if value == current else "⚠️"
            logger.info(f"   {status} {key}: {value:.2f} ms (actual: {current:.1f} ms)")
        
        return validated_thresholds
    
    def analyze_switch_margin(self, df):
        """Analizar switch_margin con validaciones"""
        logger.info("\nPASO 5: Analizar switch_margin")
        logger.info("-" * 80)
        
        degraded_data = df[df['degradation_cycle'] > 0].copy()
        
        if len(degraded_data) == 0:
            logger.warning("⚠️ No hay datos de degradación")
            return self.current_thresholds['switch_margin']
        
        degraded_data['score_diff_abs'] = degraded_data['score_difference'].abs()
        
        logger.info(f"\n📊 Diferencia de scores en degradación (n={len(degraded_data)}):")
        logger.info(f"   Media: {degraded_data['score_diff_abs'].mean():.2f} ± {degraded_data['score_diff_abs'].std():.2f}")
        logger.info(f"   p50: {degraded_data['score_diff_abs'].quantile(0.50):.2f}")
        logger.info(f"   p75: {degraded_data['score_diff_abs'].quantile(0.75):.2f}")
        logger.info(f"   p90: {degraded_data['score_diff_abs'].quantile(0.90):.2f}")
        
        switch_margin_p75 = degraded_data['score_diff_abs'].quantile(0.75)
        
        validated_margin = self.validate_threshold(
            switch_margin_p75,
            self.current_thresholds['switch_margin'],
            'switch_margin'
        )
        
        logger.info(f"\n💡 RECOMENDACIÓN:")
        logger.info(f"   switch_margin: {validated_margin:.2f} (p75)")
        logger.info(f"   Actual: {self.current_thresholds['switch_margin']:.1f}")
        
        return validated_margin
    
    def train_logistic_regression(self, df):
        """Entrenar modelo logístico"""
        logger.info("\nPASO 6: Entrenar Logistic Regression")
        logger.info("-" * 80)
        
        X = df[['peer_latency_ms', 'dns_latency_ms', 'score_difference']].copy()
        y = df['failover_event'].copy()
        
        if y.sum() < 10:
            logger.warning(f"⚠️ Solo {y.sum()} eventos de failover. Modelo puede ser inestable.")
        
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        self.scaler = StandardScaler()
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        self.X_test = X_test_scaled
        
        logger.info("🔄 Entrenando modelo logístico...")
        self.model = LogisticRegression(max_iter=1000, random_state=42)
        self.model.fit(X_train_scaled, y_train)
        
        self.y_pred_proba = self.model.predict_proba(X_test_scaled)[:, 1]
        self.y_test = y_test
        
        self.fpr, self.tpr, self.thresholds = roc_curve(y_test, self.y_pred_proba)
        self.roc_auc = auc(self.fpr, self.tpr)
        
        logger.info(f"✅ Modelo entrenado")
        logger.info(f"   ROC-AUC: {self.roc_auc:.4f}")
        
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
        """Plotear ROC Curve"""
        logger.info("\nPASO 7: Generar visualizaciones")
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
            
            plt.close()
        except Exception as e:
            logger.error(f"❌ Error generando gráfico: {e}")
    
    def generate_report(self, absolute_thresholds, zscore_thresholds, relative_thresholds, switch_margin):
        """Generar reporte final con umbrales validados"""
        logger.info("\n" + "=" * 80)
        logger.info("📋 REPORTE FINAL: THRESHOLDS OPTIMIZADOS (VALIDADOS)")
        logger.info("=" * 80)
        
        # 1. Umbrales Absolutos
        logger.info(f"\n🎯 1. UMBRALES ABSOLUTOS:")
        for key, value in absolute_thresholds.items():
            current = self.current_thresholds[key]
            status = "✅" if value == current else "⚠️"
            logger.info(f"   {status} {key}: {value:.1f} ms (actual: {current:.0f} ms)")
        
        # 2. Umbrales de Z-score
        logger.info(f"\n🎯 2. UMBRALES DE Z-SCORE:")
        for key, value in zscore_thresholds.items():
            current = self.current_thresholds[key]
            status = "✅" if value == current else "⚠️"
            logger.info(f"   {status} {key}: {value:.2f} (actual: {current:.1f})")
        
        # 3. Umbrales de Diferencia Relativa
        logger.info(f"\n🎯 3. UMBRALES DE DIFERENCIA RELATIVA:")
        for key, value in relative_thresholds.items():
            current = self.current_thresholds[key]
            status = "✅" if value == current else "⚠️"
            logger.info(f"   {status} {key}: {value:.2f} ms (actual: {current:.1f} ms)")
        
        # 4. Switch Margin
        logger.info(f"\n🎯 4. SWITCH MARGIN:")
        current = self.current_thresholds['switch_margin']
        status = "✅" if switch_margin == current else "⚠️"
        logger.info(f"   {status} switch_margin: {switch_margin:.2f} puntos (actual: {current:.1f})")
        
        # Configuración recomendada
        logger.info(f"\n📝 CONFIGURACIÓN RECOMENDADA PARA bgp_failover_config.py:")
        logger.info(f"""
# Umbrales absolutos
LATENCY_THRESHOLDS = {{
    'peer_warning':  {absolute_thresholds.get('peer_warning', 12):.0f},
    'peer_degraded': {absolute_thresholds.get('peer_degraded', 15):.0f},
    'peer_critical': {absolute_thresholds.get('peer_critical', 25):.0f},
    'dns_warning':   {absolute_thresholds.get('dns_warning', 15):.0f},
    'dns_degraded':  {absolute_thresholds.get('dns_degraded', 20):.0f},
    'dns_critical':  {absolute_thresholds.get('dns_critical', 30):.0f},
    'switch_margin': {switch_margin:.0f}
}}

# Umbrales de Z-score
Z_SCORE_THRESHOLDS = {{
    'normal': 1.5,
    'warning': {zscore_thresholds.get('z_warning', 2.0):.1f},
    'degraded': {zscore_thresholds.get('z_degraded', 2.5):.1f},
    'critical': {zscore_thresholds.get('z_critical', 3.0):.1f}
}}

# Umbrales de diferencia relativa
RELATIVE_DIFF_THRESHOLDS = {{
    'warning': {relative_thresholds.get('rel_warning', 5.0):.1f},
    'degraded': {relative_thresholds.get('rel_degraded', 10.0):.1f},
    'critical': {relative_thresholds.get('rel_critical', 15.0):.1f}
}}
        """)
        
        logger.info(f"\n✅ VALIDACIÓN:")
        logger.info(f"   ROC-AUC: {self.roc_auc:.4f} (>0.8 = excelente)")
        logger.info(f"   Datos analizados: {len(self.X_test) if self.X_test is not None else 0} samples")
        
        # Resumen de cambios
        changes_count = sum([
            1 for key in ['peer_warning', 'peer_degraded', 'peer_critical', 
                         'dns_warning', 'dns_degraded', 'dns_critical']
            if absolute_thresholds.get(key) != self.current_thresholds[key]
        ]) + sum([
            1 for key in ['z_warning', 'z_degraded', 'z_critical']
            if zscore_thresholds.get(key) != self.current_thresholds[key]
        ]) + sum([
            1 for key in ['rel_warning', 'rel_degraded', 'rel_critical']
            if relative_thresholds.get(key) != self.current_thresholds[key]
        ]) + (1 if switch_margin != self.current_thresholds['switch_margin'] else 0)
        
        logger.info(f"\n📊 RESUMEN DE CAMBIOS:")
        logger.info(f"   Umbrales modificados: {changes_count}/13")
        logger.info(f"   Umbrales mantenidos: {13 - changes_count}/13")
        
        if changes_count > 5:
            logger.warning(f"\n⚠️ ADVERTENCIA: Se modificaron {changes_count} umbrales")
            logger.warning(f"   ➜ Considere acumular más datos antes de aplicar cambios")
        elif changes_count == 0:
            logger.info(f"\n✅ Los umbrales actuales son adecuados para el dataset disponible")


def main():
    """Ejecutar pipeline completo con validaciones"""
    optimizer = ThresholdOptimizerValidated()
    
    # 1. Cargar datos
    df = optimizer.load_and_prepare_data(days=30)
    
    # 2. Analizar umbrales absolutos
    absolute_thresholds = optimizer.analyze_absolute_thresholds(df)
    
    # 3. Analizar umbrales de Z-score
    zscore_thresholds = optimizer.analyze_zscore_thresholds(df)
    
    # 4. Analizar umbrales de diferencia relativa
    relative_thresholds = optimizer.analyze_relative_thresholds(df)
    
    # 5. Analizar switch_margin
    switch_margin = optimizer.analyze_switch_margin(df)
    
    # 6. Entrenar modelo
    optimizer.train_logistic_regression(df)
    
    # 7. Plotear ROC
    optimizer.plot_roc_curve(save_path='/tmp/roc_curve_validated.png')
    
    # 8. Reporte final
    optimizer.generate_report(absolute_thresholds, zscore_thresholds, relative_thresholds, switch_margin)
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ OPTIMIZACIÓN COMPLETADA (CON VALIDACIONES)")
    logger.info("=" * 80)
    
    return absolute_thresholds, zscore_thresholds, relative_thresholds, switch_margin


if __name__ == '__main__':
    main()
