import os
import psutil
import time
from datetime import datetime
from config.logging_config import logger


class ResourceMonitor:
    """
    Monitorea recursos del sistema y decide cuando ejecutar o esperar.
    
    Controla:
    - CPU usage
    - RAM available
    - Conexiones activas
    
    Soporta ajuste dinámico de chunks según recursos disponibles.
    """

    def __init__(self, config=None):
        self.config = config or {}
        
        # Configuración desde variables de entorno (prioridad) o config
        self.max_cpu_percent = int(os.getenv("RESOURCE_MAX_CPU_PERCENT", 
                                  str(self.config.get("max_cpu_percent", 80))))
        self.max_ram_percent = int(os.getenv("RESOURCE_MAX_RAM_PERCENT", 
                                  str(self.config.get("max_ram_percent", 70))))
        self.min_ram_mb = int(os.getenv("RESOURCE_MIN_RAM_MB", 
                              str(self.config.get("min_ram_mb", 512))))
        self.check_interval = int(os.getenv("RESOURCE_CHECK_INTERVAL", 
                                  str(self.config.get("check_interval", 5))))
        
        # Configuración de chunks adaptativos
        self.adaptive_chunks = os.getenv("ADAPTIVE_CHUNKS", "true").lower() == "true"
        self.chunk_recalc_interval = int(os.getenv("CHUNK_RECALC_INTERVAL", "5"))
        self.chunk_size_reduction_factor = float(os.getenv("CHUNK_SIZE_REDUCTION_FACTOR", "0.8"))
        self.chunk_size_increase_factor = float(os.getenv("CHUNK_SIZE_INCREASE_FACTOR", "1.2"))
        self.chunk_size_min = int(os.getenv("CHUNK_SIZE_MIN", "1000"))
        self.chunk_size_max = int(os.getenv("CHUNK_SIZE_MAX", "1000000"))
        
        self.active_connections = 0
        self.chunk_count = 0
        self.current_chunk_size = None

    def get_status(self):
        """Obtiene el estado actual de los recursos."""
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            "cpu_percent": cpu,
            "cpu_cores": psutil.cpu_count(),
            "ram_total_gb": round(ram.total / (1024**3), 2),
            "ram_available_gb": round(ram.available / (1024**3), 2),
            "ram_used_percent": ram.percent,
            "disk_free_gb": round(disk.free / (1024**3), 2),
            "active_connections": self.active_connections,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }
    
    def get_resource_level(self):
        """
        Determina el nivel de recursos disponibles.
        
        Returns:
            str: 'low', 'medium', 'high' según disponibilidad
        """
        status = self.get_status()
        ram_percent = status["ram_used_percent"]
        cpu_percent = status["cpu_percent"]
        
        # Si ambos están bajo los umbrales, hay recursos altos
        if ram_percent < 50 and cpu_percent < 50:
            return "high"
        # Si alguno está cerca del umbral, recursos medios
        elif ram_percent < self.max_ram_percent and cpu_percent < self.max_cpu_percent:
            return "medium"
        # Si alguno supera el umbral, recursos bajos
        else:
            return "low"

    def can_execute(self):
        """
        Determina si hay recursos suficientes para ejecutar.
        
        Returns:
            tuple: (can_run, reasons, status, resource_level)
        """
        status = self.get_status()
        
        reasons = []
        
        if status["cpu_percent"] > self.max_cpu_percent:
            reasons.append(f"CPU: {status['cpu_percent']:.1f}% > {self.max_cpu_percent}%")
        
        available_mb = status["ram_available_gb"] * 1024
        if available_mb < self.min_ram_mb:
            reasons.append(f"RAM: {available_mb:.0f}MB < {self.min_ram_mb}MB")
        
        if status["ram_used_percent"] > self.max_ram_percent:
            reasons.append(f"RAM uso: {status['ram_used_percent']:.1f}% > {self.max_ram_percent}%")
        
        can_run = len(reasons) == 0
        resource_level = self.get_resource_level()
        
        return can_run, reasons, status, resource_level

    def wait_for_resources(self, task_name=""):
        """
        Espera hasta que haya recursos disponibles.
        
        Returns:
            dict: Status con información de recursos
        """
        while True:
            can_run, reasons, status, resource_level = self.can_execute()
            
            if can_run:
                logger.info(f"[ResourceMonitor] {task_name} - Recursos OK: CPU={status['cpu_percent']:.1f}% RAM={status['ram_available_gb']:.1f}GB")
                return status
            
            logger.info(f"[ResourceMonitor] {task_name} - Esperando recursos: {', '.join(reasons)}")
            time.sleep(self.check_interval)
    
    def adjust_chunk_size(self, current_chunk_size):
        """
        Ajusta dinámicamente el tamaño del chunk según recursos disponibles.
        
        Args:
            current_chunk_size: Tamaño actual del chunk
            
        Returns:
            int: Nuevo tamaño del chunk ajustado
        """
        if not self.adaptive_chunks:
            return current_chunk_size
        
        self.chunk_count += 1
        self.current_chunk_size = current_chunk_size
        
        # Recalcular solo cada N chunks
        if self.chunk_count % self.chunk_recalc_interval != 0:
            return current_chunk_size
        
        resource_level = self.get_resource_level()
        status = self.get_status()
        
        new_chunk_size = current_chunk_size
        
        if resource_level == "low":
            # Recursos bajos: reducir chunk
            new_chunk_size = int(current_chunk_size * self.chunk_size_reduction_factor)
            new_chunk_size = max(new_chunk_size, self.chunk_size_min)
            logger.info(f"[ResourceMonitor] Recursos BAJOS - Reduciendo chunk: {current_chunk_size:,} → {new_chunk_size:,} (RAM: {status['ram_used_percent']:.1f}%)")
        
        elif resource_level == "high":
            # Recursos altos: aumentar chunk
            new_chunk_size = int(current_chunk_size * self.chunk_size_increase_factor)
            new_chunk_size = min(new_chunk_size, self.chunk_size_max)
            if new_chunk_size != current_chunk_size:
                logger.info(f"[ResourceMonitor] Recursos ALTOS - Aumentando chunk: {current_chunk_size:,} → {new_chunk_size:,} (RAM: {status['ram_used_percent']:.1f}%)")
        
        # Recursos medios: mantener chunk actual
        return new_chunk_size

    def log_status(self, task_name=""):
        """Registra el estado actual."""
        status = self.get_status()
        resource_level = self.get_resource_level()
        logger.info(f"[ResourceMonitor] {task_name} | CPU: {status['cpu_percent']:.1f}% | RAM: {status['ram_available_gb']:.1f}GB libre | Nivel: {resource_level} | Conexiones: {status['active_connections']}")
        return status

    def register_connection(self):
        """Registra una nueva conexion activa."""
        self.active_connections += 1

    def unregister_connection(self):
        """Registra cierre de conexion."""
        self.active_connections = max(0, self.active_connections - 1)
