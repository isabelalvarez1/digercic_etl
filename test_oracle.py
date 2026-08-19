import sys
from pathlib import Path

# Agregar carpeta app al path
app_dir = Path(__file__).parent / "app"
sys.path.insert(0, str(app_dir))

from dotenv import load_dotenv
load_dotenv()

import os
import oracledb

def test_oracle():
    """Prueba de conexion a Oracle 11g (Banner)"""
    host = os.getenv("ORACLE_HOST")
    port = os.getenv("ORACLE_PORT")
    service = os.getenv("ORACLE_SERVICE")
    user = os.getenv("ORACLE_USER")
    password = os.getenv("ORACLE_PASSWORD")
    instant_client_dir = os.getenv("ORACLE_INSTANT_CLIENT_DIR", "/opt/oracle/instantclient_21_12")

    print("=" * 50)
    print("PRUEBA DE CONEXION ORACLE 11g (BANNER)")
    print("=" * 50)
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Service: {service}")
    print(f"User: {user}")
    print(f"Instant Client: {instant_client_dir}")
    print("=" * 50)

    # Activar thick mode para Oracle 11g
    try:
        oracledb.init_oracle_client(lib_dir=instant_client_dir)
        print("OK - Thick mode activado")
    except Exception as e:
        print(f"WARNING - Thick mode no disponible: {e}")
        print("Intentando thin mode (puede fallar con Oracle 11g)...")

    try:
        dsn = f"{host}:{port}/{service}"
        print(f"\nConectando a: {user}@{dsn}")
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        cur = conn.cursor()

        # Ver version de Oracle
        cur.execute("SELECT * FROM v$version WHERE ROWNUM = 1")
        version = cur.fetchone()[0]
        print(f"OK - Version Oracle: {version}")

        # Probar tabla CEDULADO
        print(f"\nProbando tabla INTEROPERABILIDAD.CEDULADO_MIN_DESARROLLO_HUMANO...")
        cur.execute("SELECT COUNT(*) FROM INTEROPERABILIDAD.CEDULADO_MIN_DESARROLLO_HUMANO")
        count = cur.fetchone()[0]
        print(f"OK - Registros encontrados: {count}")

        # Obtener columnas
        cur.execute("SELECT * FROM (SELECT * FROM INTEROPERABILIDAD.CEDULADO_MIN_DESARROLLO_HUMANO WHERE ROWNUM <= 0)")
        columns = [desc[0] for desc in cur.description]
        print(f"OK - Columnas ({len(columns)}): {', '.join(columns[:10])}{'...' if len(columns) > 10 else ''}")

        # Obtener primeras 3 filas como prueba
        cur.execute("SELECT * FROM INTEROPERABILIDAD.CEDULADO_MIN_DESARROLLO_HUMANO WHERE ROWNUM <= 3")
        rows = cur.fetchall()
        print(f"\nPrimeras {len(rows)} filas:")
        for i, row in enumerate(rows):
            print(f"  Fila {i+1}: {row[:3]}...")

        cur.close()
        conn.close()

        print("\n" + "=" * 50)
        print("CONEXION ORACLE EXITOSA")
        print("=" * 50)
        return True

    except Exception as e:
        print(f"\nERROR de conexion: {e}")
        print("\nVerifica:")
        print("1. IP y puerto correctos")
        print("2. Service name correcto")
        print("3. Credenciales correctas")
        print("4. Firewall abierto (puerto 1521)")
        print("5. Oracle Instant Client instalado correctamente")
        return False

if __name__ == "__main__":
    test_oracle()
