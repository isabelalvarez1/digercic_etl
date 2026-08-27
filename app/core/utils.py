import os
import psutil
from typing import Dict, Any


def get_system_resources() -> Dict[str, Any]:
    """
    Obtiene los recursos del sistema para optimizar el procesamiento por lotes.
    
    Returns:
        Diccionario con información del sistema
    """
    cpu_count = os.cpu_count()
    cpu_count_physical = psutil.cpu_count(logical=False)
    memory = psutil.virtual_memory()
    
    return {
        "cpu_cores_logical": cpu_count,
        "cpu_cores_physical": cpu_count_physical,
        "memory_total_gb": round(memory.total / (1024**3), 2),
        "memory_available_gb": round(memory.available / (1024**3), 2),
        "memory_percent_used": memory.percent,
    }


def calculate_optimal_batch_size(total_rows: int, resources: Dict[str, Any], num_columns: int) -> Dict[str, Any]:
    """
    Calcula el tamaño de lote óptimo basado en recursos del sistema y numero de columnas.
    
    Formula: batch_size = (RAM_disponible_GB * 1024 * 1024 * 1024 * 0.3) / (num_columnas * 100)
    
    Args:
        total_rows: Total de registros a procesar
        resources: Recursos del sistema
        num_columns: Numero de columnas de la tabla (requerido)
        
    Returns:
        Diccionario con información de lotes
    """
    cpu_cores = resources.get("cpu_cores_logical")
    memory_available = resources.get("memory_available_gb")
    
    # ~100 bytes por celda en memoria (promedio)
    bytes_per_row = num_columns * 100
    
    # Usar 30% de RAM disponible para el batch (dejar buffer para Oracle + overhead)
    memory_for_batch = memory_available * 1024 * 1024 * 1024 * 0.3
    memory_based_batch = int(memory_for_batch / bytes_per_row)
    
    # Ajustar por CPU (mas cores = puede manejar batches mas grandes)
    cpu_based_batch = cpu_cores * 50000
    
    # Tomar el menor entre memoria y CPU
    optimal_batch = min(memory_based_batch, cpu_based_batch)
    
    # Limites实用
    optimal_batch = max(optimal_batch, 10000)     # Minimo 10K
    optimal_batch = min(optimal_batch, 500000)    # Maximo 500K
    
    # Redondear a multiplos de 10K para numeros limpios
    optimal_batch = (optimal_batch // 10000) * 10000
    
    # Calcular numero de lotes
    batch_count = (total_rows + optimal_batch - 1) // optimal_batch
    
    # Estimar tiempo (asumiendo ~2000 registros/seg con INSERT, ~50K con COPY)
    estimated_seconds_insert = total_rows / 2000
    estimated_seconds_copy = total_rows / 50000
    
    return {
        "total_rows": total_rows,
        "batch_size": optimal_batch,
        "batch_count": batch_count,
        "num_columns": num_columns,
        "bytes_per_row": bytes_per_row,
        "estimated_memory_mb": round((optimal_batch * bytes_per_row) / (1024 * 1024), 2),
        "estimated_time_insert_min": round(estimated_seconds_insert / 60, 1),
        "estimated_time_copy_min": round(estimated_seconds_copy / 60, 1),
    }
