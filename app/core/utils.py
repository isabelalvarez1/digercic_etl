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
    
    Variables de entorno configurables:
        BATCH_MEMORY_PERCENT: % de RAM disponible para batch (default: 0.5)
        BATCH_CPU_MULTIPLIER: Multiplicador por CPU core (default: 100000)
        BATCH_SIZE_MIN: Tamaño minimo de batch (default: 10000)
        BATCH_SIZE_MAX: Tamaño maximo de batch (default: 1000000)
        BATCH_BYTES_PER_CELL: Bytes estimados por celda (default: 100)
        
    Args:
        total_rows: Total de registros a procesar
        resources: Recursos del sistema
        num_columns: Numero de columnas de la tabla (requerido)
        
    Returns:
        Diccionario con información de lotes
    """
    cpu_cores = resources.get("cpu_cores_logical")
    memory_available = resources.get("memory_available_gb")
    
    # Parametros configurables desde variables de entorno
    memory_percent = float(os.getenv("BATCH_MEMORY_PERCENT", "0.5"))
    cpu_multiplier = int(os.getenv("BATCH_CPU_MULTIPLIER", "100000"))
    batch_min = int(os.getenv("BATCH_SIZE_MIN", "10000"))
    batch_max = int(os.getenv("BATCH_SIZE_MAX", "1000000"))
    bytes_per_cell = int(os.getenv("BATCH_BYTES_PER_CELL", "100"))
    
    # Calcular basado en memoria
    bytes_per_row = num_columns * bytes_per_cell
    memory_for_batch = memory_available * 1024 * 1024 * 1024 * memory_percent
    memory_based_batch = int(memory_for_batch / bytes_per_row)
    
    # Calcular basado en CPU
    cpu_based_batch = cpu_cores * cpu_multiplier
    
    # Tomar el menor entre memoria y CPU
    optimal_batch = min(memory_based_batch, cpu_based_batch)
    
    # Aplicar limites
    optimal_batch = max(optimal_batch, batch_min)
    optimal_batch = min(optimal_batch, batch_max)
    
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
        "config": {
            "memory_percent": memory_percent,
            "cpu_multiplier": cpu_multiplier,
            "batch_min": batch_min,
            "batch_max": batch_max,
            "bytes_per_cell": bytes_per_cell,
        }
    }
