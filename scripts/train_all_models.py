#!/usr/bin/env python3
"""
train_all_models.py

Orchestrator que ejecuta TODOS los modelos ML en secuencia
├─ train_xgboost_weights.py: Weight optimization
├─ train_random_forest.py: Binary classification
└─ (Próximo) train_logistic_regression.py: Probability estimation

Propósito: Coordinador centralizado de todo el pipeline ML
"""

import logging
import subprocess
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLOrchestrator:
    """
    ✅ Ejecuta todos los modelos ML en orden
    
    Arquitectura:
    ├─ Modular: Cada modelo es independiente
    ├─ Escalable: Agregar modelos es fácil
    ├─ Robusto: Si uno falla, puede continuarse manualmente
    └─ Reportable: Genera logs de cada ejecución
    """
    
    def __init__(self):
        self.models = []
        self.results = {}
        self.start_time = None
        self.end_time = None
    
    def add_model(self, name, script_path):
        """
        Agregar modelo al pipeline
        
        Args:
            name: Nombre del modelo
            script_path: Ruta al script Python
        """
        
        self.models.append({
            'name': name,
            'script': script_path,
            'status': 'pending'
        })
    
    def execute_model(self, model):
        """
        Ejecutar script de modelo individual
        
        Args:
            model: Diccionario con info del modelo
        
        Returns:
            bool: True si ejecutó exitosamente
        """
        
        name = model['name']
        script = model['script']
        
        logger.info(f"\n{'='*80}")
        logger.info(f"🚀 EJECUTANDO: {name}")
        logger.info(f"{'='*80}")
        logger.info(f"   Script: {script}")
        
        try:
            # Ejecutar script
            result = subprocess.run(
                [sys.executable, script],
                capture_output=False,
                text=True,
                timeout=600  # 10 minutos timeout
            )
            
            if result.returncode == 0:
                logger.info(f"✅ {name} completado exitosamente")
                model['status'] = 'success'
                return True
            else:
                logger.error(f"❌ {name} falló con código: {result.returncode}")
                model['status'] = 'failed'
                return False
        
        except subprocess.TimeoutExpired:
            logger.error(f"⏱️ {name} excedió timeout (10 min)")
            model['status'] = 'timeout'
            return False
        except Exception as e:
            logger.error(f"❌ Error ejecutando {name}: {e}")
            model['status'] = 'error'
            return False
    
    def run_all(self, skip_on_fail=False):
        """
        Ejecutar todos los modelos
        
        Args:
            skip_on_fail: Si True, continuar aunque uno falle
                         Si False, detener si uno falla
        
        Returns:
            dict: Resultados de ejecución
        """
        
        self.start_time = datetime.now()
        
        logger.info("=" * 80)
        logger.info("🚀 ML PIPELINE ORCHESTRATOR")
        logger.info("=" * 80)
        logger.info(f"\n📋 Modelos a ejecutar: {len(self.models)}")
        for i, model in enumerate(self.models, 1):
            logger.info(f"   {i}. {model['name']}")
        
        logger.info(f"\n⚙️ Configuración:")
        logger.info(f"   Skip on fail: {skip_on_fail}")
        logger.info(f"   Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Ejecutar modelos
        for model in self.models:
            success = self.execute_model(model)
            
            if not success and not skip_on_fail:
                logger.error(f"\n⛔ Pipeline detenido. {model['name']} falló.")
                logger.error(f"   Use skip_on_fail=True para continuar ignorando fallos.")
                break
        
        self.end_time = datetime.now()
        
        # Reporte final
        self.print_summary()
    
    def print_summary(self):
        """
        Imprimir resumen de ejecución
        """
        
        duration = (self.end_time - self.start_time).total_seconds()
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 REPORTE FINAL")
        logger.info("=" * 80)
        
        logger.info(f"\n⏱️ Duración total: {duration:.1f} segundos ({duration/60:.1f} min)")
        
        logger.info(f"\n📈 Estado de ejecución:")
        for i, model in enumerate(self.models, 1):
            status_icon = {
                'success': '✅',
                'failed': '❌',
                'error': '⚠️',
                'timeout': '⏱️',
                'pending': '⏳'
            }
            
            icon = status_icon.get(model['status'], '?')
            logger.info(f"   {i}. {icon} {model['name']:30s} [{model['status']}]")
        
        # Estadísticas
        total = len(self.models)
        success_count = sum(1 for m in self.models if m['status'] == 'success')
        fail_count = total - success_count
        
        logger.info(f"\n📊 Estadísticas:")
        logger.info(f"   Total: {total}")
        logger.info(f"   Exitosos: {success_count} ✅")
        logger.info(f"   Fallidos: {fail_count} ❌")
        
        if success_count == total:
            logger.info(f"\n🎉 TODOS LOS MODELOS EJECUTADOS EXITOSAMENTE 🚀")
        else:
            logger.warning(f"\n⚠️ {fail_count} modelo(s) fallido(s). Ver logs arriba.")
        
        logger.info(f"\n{'='*80}")


def main():
    """
    Configurar y ejecutar orchestrator
    """
    
    # Crear orchestrator
    orchestrator = MLOrchestrator()
    
    # Agregar modelos en orden
    orchestrator.add_model(
        name="1. XGBoost Weight Optimization",
        script_path="train_from_ml_features_FINAL.py"
    )
    
    orchestrator.add_model(
        name="2. Random Forest Classification",
        script_path="train_random_forest.py"
    )
    
    # Próximos modelos (cuando estén listos)
    # orchestrator.add_model(
    #     name="3. Logistic Regression Probability",
    #     script_path="train_logistic_regression.py"
    # )
    
    # Ejecutar
    orchestrator.run_all(skip_on_fail=False)
    
    return orchestrator


if __name__ == '__main__':
    orchestrator = main()
