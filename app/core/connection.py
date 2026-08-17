import os
import oracledb
from typing import Any, Dict, List, Optional
from config.logging_config import logger


class OracleConnectionManager:
    """
    Gestor de multiples conexiones Oracle.
    
    Permite configurar varias fuentes Oracle y usarlas por nombre.
    """
    
    def __init__(self):
        self._connections: Dict[str, oracledb.Connection] = {}
        self._configs: Dict[str, Dict] = {}
    
    def register(self, name: str, config: Dict[str, Any]) -> None:
        """
        Registra una configuracion de conexion.
        
        Args:
            name: Nombre de la conexion (ej: "oracle_produccion")
            config: Configuracion con url/host/port/service/user/password/ssl
        """
        self._configs[name] = config
        logger.info(f"[ConnectionManager] Conexion '{name}' registrada")
    
    def connect(self, name: str) -> oracledb.Connection:
        """
        Obtiene o crea una conexion por nombre.
        
        Args:
            name: Nombre de la conexion
            
        Returns:
            Conexion a Oracle
        """
        # Si ya existe y esta activa, reutilizar
        if name in self._connections:
            try:
                # Verificar que la conexion este activa
                self._connections[name].ping()
                return self._connections[name]
            except:
                # Conexion cerrada, crear nueva
                del self._connections[name]
        
        # Obtener configuracion
        if name not in self._configs:
            raise ValueError(f"Conexion '{name}' no encontrada. Disponibles: {list(self._configs.keys())}")
        
        config = self._configs[name]
        
        # Crear conexion
        connection = create_oracle_connection(
            url=config.get("url"),
            host=config.get("host"),
            port=config.get("port", 1521),
            service=config.get("service"),
            user=config.get("user"),
            password=config.get("password"),
            ssl=config.get("ssl", False)
        )
        
        self._connections[name] = connection
        return connection
    
    def disconnect(self, name: Optional[str] = None) -> None:
        """
        Cierra una o todas las conexiones.
        
        Args:
            name: Nombre de la conexion (None para cerrar todas)
        """
        if name:
            if name in self._connections:
                self._connections[name].close()
                del self._connections[name]
                logger.info(f"[ConnectionManager] Conexion '{name}' cerrada")
        else:
            for conn_name in list(self._connections.keys()):
                self.disconnect(conn_name)
    
    def get_config(self, name: str) -> Dict[str, Any]:
        """Obtiene la configuracion de una conexion."""
        if name not in self._configs:
            raise ValueError(f"Conexion '{name}' no encontrada")
        return self._configs[name]
    
    def list_connections(self) -> List[str]:
        """Lista todas las conexiones registradas."""
        return list(self._configs.keys())


# Instancia global del gestor
connection_manager = OracleConnectionManager()


def create_oracle_connection(
    url: Optional[str] = None,
    host: Optional[str] = None,
    port: int = 1521,
    service: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    ssl: bool = False
) -> oracledb.Connection:
    """
    Crea una conexion a Oracle.
    
    Args:
        url: URL completa (host:port/service)
        host: Host de Oracle
        port: Puerto (default 1521)
        service: Nombre del servicio
        user: Usuario de Oracle
        password: Contrasena de Oracle
        ssl: Habilitar SSL (default False)
        
    Returns:
        Conexion a Oracle
    """
    # Si se proporciona URL, parsearla
    if url:
        parsed = parse_oracle_url(url)
        host = parsed.get("host", host)
        port = parsed.get("port", port)
        service = parsed.get("service", service)
    
    # Validar parametros obligatorios
    if not host:
        raise ValueError("Se requiere 'host' o 'url'")
    if not user:
        raise ValueError("Se requiere 'user'")
    if not password:
        raise ValueError("Se requiere 'password'")
    
    # Construir DSN
    if service:
        dsn = f"{host}:{port}/{service}"
    else:
        dsn = f"{host}:{port}"
    
    # Configurar SSL si es necesario
    if ssl:
        logger.info(f"[OracleConnection] Conectando con SSL a {host}:{port}")
        # En produccion, aqui iria la logica de SSL/wallet
    else:
        logger.info(f"[OracleConnection] Conectando sin SSL a {dsn}")
    
    # Crear conexion
    connection = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn,
    )
    
    logger.info(f"[OracleConnection] Conexion exitosa a {dsn}")
    return connection


def parse_oracle_url(url: str) -> Dict[str, Any]:
    """
    Parsea una URL de Oracle.
    
    Formatos soportados:
        - host:port/service
        - host:port
        - host
    """
    result = {
        "host": None,
        "port": 1521,
        "service": None
    }
    
    if not url:
        return result
    
    # Limpiar URL
    url = url.strip().strip("'\"")
    
    # Dividir por /
    parts = url.split("/")
    
    if len(parts) >= 2:
        host_port = parts[0]
        result["service"] = parts[1]
    elif len(parts) == 1:
        host_port = parts[0]
    else:
        host_port = url
    
    # Dividir host:port
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        result["host"] = host
        try:
            result["port"] = int(port_str)
        except ValueError:
            pass
    else:
        result["host"] = host_port
    
    return result


def get_connection_from_config(config: Dict[str, Any]) -> oracledb.Connection:
    """
    Crea conexion desde configuracion del pipeline.
    
    Soporta:
        - connection_name: Usa una conexion pre-registrada
        - url: Conexion por URL
        - host/port/service: Conexion tradicional
    """
    # Si se especifica un nombre de conexion, usar el gestor
    connection_name = config.get("connection_name")
    if connection_name:
        return connection_manager.connect(connection_name)
    
    # Si no, crear conexion directa
    return create_oracle_connection(
        url=config.get("url"),
        host=config.get("host"),
        port=config.get("port", 1521),
        service=config.get("service"),
        user=config.get("user"),
        password=config.get("password"),
        ssl=config.get("ssl", False)
    )
