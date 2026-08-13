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


def calculate_optimal_batch_size(total_rows: int, resources: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcula el tamaño de lote óptimo basado en los recursos del sistema.
    
    Args:
        total_rows: Total de registros a procesar
        resources: Recursos del sistema
        
    Returns:
        Diccionario con información de lotes
    """
    cpu_cores = resources.get("cpu_cores_logical", 4)
    memory_available = resources.get("memory_available_gb", 4)
    
    # Calcular batch size basado en memoria y CPU
    # Regla: ~1MB por 1000 registros en memoria
    memory_based_batch = int((memory_available * 1024 * 1000) / 2)  # Usar 50% de memoria disponible
    
    # Limitar por CPU cores
    optimal_batch = min(memory_based_batch, cpu_cores * 10000)
    
    # Asegurar que sea al menos 1000
    optimal_batch = max(optimal_batch, 1000)
    
    # Calcular número de lotes
    batch_count = (total_rows + optimal_batch - 1) // optimal_batch
    
    return {
        "total_rows": total_rows,
        "batch_size": optimal_batch,
        "batch_count": batch_count,
        "rows_per_core": total_rows // cpu_cores if cpu_cores > 0 else total_rows,
        "estimated_memory_mb": round((optimal_batch * 100) / 1024, 2),  # Estimación
    }
