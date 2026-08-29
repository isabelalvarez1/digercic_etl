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
                                  str(self.config.get("max_cpu_percent", 90))))
        self.max_ram_percent = int(os.getenv("RESOURCE_MAX_RAM_PERCENT", 
                                  str(self.config.get("max_ram_percent", 90))))
        self.min_ram_mb = int(os.getenv("RESOURCE_MIN_RAM_MB", 
                              str(self.config.get("min_ram_mb", 512))))
        self.check_interval = int(os.getenv("RESOURCE_CHECK_INTERVAL", 
                                  str(self.config.get("check_interval", 3))))
        
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
        
        # Si RAM está bajo 50% y CPU bajo 70%, recursos altos
        if ram_percent < 50 and cpu_percent < 70:
            return "high"
        # Si RAM está bajo umbral máximo y CPU bajo 90%, recursos medios
        elif ram_percent < self.max_ram_percent and cpu_percent < 90:
            return "medium"
        # Si RAM supera umbral o CPU muy alta, recursos bajos
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
        
        # Solo bloquear por RAM, no por CPU
        # La CPU se ajusta con chunks dinámicos, no con espera
        available_mb = status["ram_available_gb"] * 1024
        if available_mb < self.min_ram_mb:
            reasons.append(f"RAM: {available_mb:.0f}MB < {self.min_ram_mb}MB")
        
        if status["ram_used_percent"] > self.max_ram_percent:
            reasons.append(f"RAM uso: {status['ram_used_percent']:.1f}% > {self.max_ram_percent}%")
        
        # CPU alta es advertencia, no bloqueo
        if status["cpu_percent"] > self.max_cpu_percent:
            # Solo log en debug para no llenar el log
            pass
        
        can_run = len(reasons) == 0
        resource_level = self.get_resource_level()
        
        return can_run, reasons, status, resource_level

    def wait_for_resources(self, task_name=""):
        """
        Espera hasta que haya recursos disponibles.
        Solo bloquea cuando recursos están críticos.
        
        Returns:
            dict: Status con información de recursos
        """
        while True:
            can_run, reasons, status, resource_level = self.can_execute()
            
            if can_run:
                logger.info(f"[ResourceMonitor] {task_name} - Recursos OK: CPU={status['cpu_percent']:.1f}% RAM={status['ram_available_gb']:.1f}GB")
                return status
            
            # Solo bloquear si RAM está realmente baja (no CPU)
            ram_critical = status["ram_used_percent"] > self.max_ram_percent
            ram_low = (status["ram_available_gb"] * 1024) < self.min_ram_mb
            
            if ram_critical or ram_low:
                # Si RAM está entre 90-95%, reducir chunk en lugar de esperar
                if 90 <= status["ram_used_percent"] <= 95:
                    logger.info(f"[ResourceMonitor] {task_name} - RAM alta ({status['ram_used_percent']:.1f}%) pero continuando con chunks reducidos")
                    return status
                else:
                    # Si RAM > 95%, esperar menos tiempo
                    logger.info(f"[ResourceMonitor] {task_name} - Esperando recursos: {', '.join(reasons)}")
                    time.sleep(2)  # Reducido de 3-5 a 2 segundos
            else:
                # CPU alta pero RAM OK - no bloquear, solo advertir
                logger.info(f"[ResourceMonitor] {task_name} - CPU alta pero continuando: CPU={status['cpu_percent']:.1f}% RAM={status['ram_available_gb']:.1f}GB")
                return status
    
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
            # Recursos bajos: reducir chunk más agresivamente
            reduction = self.chunk_size_reduction_factor
            if status["ram_used_percent"] > 92:
                reduction = 0.5  # Reducir 50% si RAM > 92%
            elif status["ram_used_percent"] > 90:
                reduction = 0.6  # Reducir 40% si RAM > 90%
            elif status["cpu_percent"] > 90:
                reduction = 0.7  # Reducir 30% si CPU > 90%
            
            new_chunk_size = int(current_chunk_size * reduction)
            new_chunk_size = max(new_chunk_size, self.chunk_size_min)
            logger.info(f"[ResourceMonitor] Recursos BAJOS - Reduciendo chunk: {current_chunk_size:,} → {new_chunk_size:,} (CPU: {status['cpu_percent']:.1f}%, RAM: {status['ram_used_percent']:.1f}%)")
        
        elif resource_level == "high":
            # Recursos altos: aumentar chunk
            new_chunk_size = int(current_chunk_size * self.chunk_size_increase_factor)
            new_chunk_size = min(new_chunk_size, self.chunk_size_max)
            if new_chunk_size != current_chunk_size:
                logger.info(f"[ResourceMonitor] Recursos ALTOS - Aumentando chunk: {current_chunk_size:,} → {new_chunk_size:,} (CPU: {status['cpu_percent']:.1f}%, RAM: {status['ram_used_percent']:.1f}%)")
        
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
