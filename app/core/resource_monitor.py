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
    """

    def __init__(self, config=None):
        self.config = config or {}
        self.max_cpu_percent = self.config.get("max_cpu_percent", 80)
        self.max_ram_percent = self.config.get("max_ram_percent", 80)
        self.min_ram_mb = self.config.get("min_ram_mb", 1024)
        self.check_interval = self.config.get("check_interval", 5)
        self.active_connections = 0

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

    def can_execute(self):
        """Determina si hay recursos suficientes para ejecutar."""
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
        
        return can_run, reasons, status

    def wait_for_resources(self, task_name=""):
        """Espera hasta que haya recursos disponibles."""
        while True:
            can_run, reasons, status = self.can_execute()
            
            if can_run:
                logger.info(f"[ResourceMonitor] {task_name} - Recursos OK: CPU={status['cpu_percent']:.1f}% RAM={status['ram_available_gb']:.1f}GB")
                return status
            
            logger.info(f"[ResourceMonitor] {task_name} - Esperando recursos: {', '.join(reasons)}")
            time.sleep(self.check_interval)

    def log_status(self, task_name=""):
        """Registra el estado actual."""
        status = self.get_status()
        logger.info(f"[ResourceMonitor] {task_name} | CPU: {status['cpu_percent']:.1f}% | RAM: {status['ram_available_gb']:.1f}GB libre | Conexiones: {status['active_connections']}")
        return status

    def register_connection(self):
        """Registra una nueva conexion activa."""
        self.active_connections += 1

    def unregister_connection(self):
        """Registra cierre de conexion."""
        self.active_connections = max(0, self.active_connections - 1)
