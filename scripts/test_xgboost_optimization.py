#!/usr/bin/env python3
import sys
sys.path.insert(0, '/root/automation')

from data_generator import generate_training_dataset
from xgboost_optimizer import ScoringWeightOptimizer

# 1. Generar datos
print("📊 Generando dataset de entrenamiento...")
df = generate_training_dataset(
    use_synthetic=True,
    synthetic_samples=40000,
    use_historical=False
)

# 2. Entrenar XGBoost
print("\n🤖 Entrenando XGBoost...")
optimizer = ScoringWeightOptimizer()
y_test, y_pred, y_pred_proba = optimizer.train(df)

# 3. Obtener pesos optimizados
print("\n📈 Extrayendo pesos optimizados...")
weights = optimizer.get_optimized_weights()

# 4. Ver comparación
print("\n✅ RESULTADO:")
print(f"  Peer weight (actual: 0.70) → Optimizado: {weights['peer_latency_weight']:.2f}")
print(f"  DNS weight (actual: 0.30) → Optimizado: {weights['dns_latency_weight']:.2f}")
print(f"  Loss importance: {weights['loss_importance']:.4f}")
print(f"  Context importance: {weights['context_importance']:.4f}")
