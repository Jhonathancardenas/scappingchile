import mysql.connector
from mysql.connector import Error
import json
import datetime
import logging
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class AmandaDatabase:
    """Clase para manejar la conexión y operaciones con MySQL para Amanda"""

    def __init__(self):
        """Inicializa la conexión a la base de datos"""
        self.connection = None
        self.db_config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'database': os.getenv('MYSQL_DATABASE', 'livetrade_encartes'),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', '')
        }
        self.connect()
        self.crear_tablas()

    def connect(self):
        """Establece conexión con la base de datos"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            if self.connection.is_connected():
                logging.info(f"Conectado a MySQL - Base de datos: {self.db_config['database']}")
                return True
        except Error as e:
            logging.error(f"Error al conectar a MySQL: {e}")
            return False

    def crear_tablas(self):
        """Crea las tablas necesarias si no existen"""
        if not self.connection or not self.connection.is_connected():
            self.connect()

        cursor = self.connection.cursor()

        # Tabla para RUTs generados
        tabla_ruts = """
        CREATE TABLE IF NOT EXISTS amanda_ruts_generados (
            id INT AUTO_INCREMENT PRIMARY KEY,
            rut VARCHAR(20) UNIQUE NOT NULL,
            fecha_generacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            procesado BOOLEAN DEFAULT FALSE,
            tiene_datos BOOLEAN DEFAULT FALSE,
            fecha_procesamiento DATETIME NULL,
            INDEX idx_rut (rut),
            INDEX idx_procesado (procesado),
            INDEX idx_tiene_datos (tiene_datos)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """

        # Tabla para datos del formulario
        tabla_formularios = """
        CREATE TABLE IF NOT EXISTS amanda_formularios (
            id INT AUTO_INCREMENT PRIMARY KEY,
            rut_consulta VARCHAR(20) NOT NULL,
            rut_formulario VARCHAR(20),
            genero VARCHAR(10),
            nombre_apellido VARCHAR(255),
            fecha_nacimiento VARCHAR(20),
            edad VARCHAR(10),
            whatsapp VARCHAR(50),
            email VARCHAR(255),
            instagram VARCHAR(255),
            asistencias_previas INT,
            grab VARCHAR(255),
            rrpp VARCHAR(255),
            mensajes TEXT,
            errores TEXT,
            texto_completo TEXT,
            screenshot_path VARCHAR(500),
            fecha_consulta DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (rut_consulta) REFERENCES amanda_ruts_generados(rut) ON DELETE CASCADE,
            INDEX idx_rut_consulta (rut_consulta),
            INDEX idx_rut_formulario (rut_formulario),
            INDEX idx_email (email),
            INDEX idx_whatsapp (whatsapp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """

        try:
            cursor.execute(tabla_ruts)
            cursor.execute(tabla_formularios)
            self.connection.commit()
            logging.info("Tablas creadas/verificadas exitosamente")
        except Error as e:
            logging.error(f"Error al crear tablas: {e}")
        finally:
            cursor.close()

    def rut_existe(self, rut):
        """
        Verifica si un RUT ya existe en la base de datos
        Args:
            rut (str): RUT a verificar
        Returns:
            bool: True si existe, False si no existe
        """
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                logging.error(f"No se pudo conectar a la base de datos para verificar RUT {rut}")
                return False

        cursor = None
        try:
            cursor = self.connection.cursor()
            query = "SELECT COUNT(*) FROM amanda_ruts_generados WHERE rut = %s"
            cursor.execute(query, (rut,))
            result = cursor.fetchone()

            if result is None:
                logging.warning(f"Resultado None al verificar RUT {rut}")
                return False

            count = result[0]
            logging.debug(f"Verificación RUT {rut}: COUNT = {count}")
            return count > 0

        except Error as e:
            logging.error(f"Error verificando RUT {rut}: {e}")
            return False
        finally:
            if cursor:
                cursor.close()

    def guardar_rut_generado(self, rut):
        """
        Guarda un RUT generado en la base de datos
        Args:
            rut (str): RUT a guardar
        Returns:
            bool: True si se guardó exitosamente, False si ya existe o hubo error
        """
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                logging.error(f"No se pudo conectar a la base de datos para guardar RUT {rut}")
                return False

        cursor = None
        try:
            cursor = self.connection.cursor()
            query = "INSERT INTO amanda_ruts_generados (rut) VALUES (%s)"
            cursor.execute(query, (rut,))
            self.connection.commit()
            logging.info(f"RUT {rut} guardado exitosamente en la base de datos")
            return True
        except mysql.connector.IntegrityError as e:
            # Error de clave duplicada (el RUT ya existe)
            if e.errno == 1062:  # Duplicate entry
                logging.info(f"RUT {rut} ya existe en la base de datos (duplicate key)")
                return False
            else:
                logging.error(f"Error de integridad guardando RUT {rut}: {e}")
                return False
        except Error as e:
            logging.error(f"Error guardando RUT {rut}: {e}")
            self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def marcar_rut_procesado(self, rut, tiene_datos=False):
        """
        Marca un RUT como procesado
        Args:
            rut (str): RUT a marcar
            tiene_datos (bool): Si el formulario tenía datos
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()

        cursor = self.connection.cursor()
        query = """
            UPDATE amanda_ruts_generados
            SET procesado = TRUE,
                tiene_datos = %s,
                fecha_procesamiento = %s
            WHERE rut = %s
        """

        try:
            cursor.execute(query, (tiene_datos, datetime.datetime.now(), rut))
            self.connection.commit()
        except Error as e:
            logging.error(f"Error marcando RUT {rut} como procesado: {e}")
        finally:
            cursor.close()

    def guardar_formulario(self, datos):
        """
        Guarda los datos del formulario en la base de datos
        Args:
            datos (dict): Diccionario con los datos del formulario
        Returns:
            bool: True si se guardó exitosamente, False en caso contrario
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()

        cursor = self.connection.cursor()

        # Extraer datos del formulario
        formulario = datos.get('datos_extraidos', {}).get('formulario', {}) or {}
        datos_extraidos = datos.get('datos_extraidos', {})

        query = """
            INSERT INTO amanda_formularios (
                rut_consulta, rut_formulario, genero, nombre_apellido,
                fecha_nacimiento, edad, whatsapp, email, instagram,
                asistencias_previas, grab, rrpp, mensajes, errores,
                texto_completo, screenshot_path
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        valores = (
            datos.get('rut'),
            formulario.get('rut'),
            formulario.get('genero'),
            formulario.get('nombre_apellido'),
            formulario.get('fecha_nacimiento'),
            formulario.get('edad'),
            formulario.get('whatsapp'),
            formulario.get('email'),
            formulario.get('instagram'),
            formulario.get('asistencias_previas'),
            formulario.get('grab'),
            formulario.get('rrpp'),
            json.dumps(datos_extraidos.get('mensajes', []), ensure_ascii=False),
            json.dumps(datos_extraidos.get('errores', []), ensure_ascii=False),
            datos_extraidos.get('texto_completo', '')[:5000],  # Limitar a 5000 caracteres
            datos.get('screenshot')
        )

        try:
            cursor.execute(query, valores)
            self.connection.commit()
            logging.info(f"Formulario guardado para RUT {datos.get('rut')}")
            return True
        except Error as e:
            logging.error(f"Error guardando formulario para RUT {datos.get('rut')}: {e}")
            return False
        finally:
            cursor.close()

    def obtener_ruts_no_procesados(self, limite=None):
        """
        Obtiene RUTs que aún no han sido procesados
        Args:
            limite (int): Número máximo de RUTs a obtener
        Returns:
            list: Lista de RUTs no procesados
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()

        cursor = self.connection.cursor()
        query = "SELECT rut FROM amanda_ruts_generados WHERE procesado = FALSE"

        if limite:
            query += f" LIMIT {limite}"

        try:
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]
        except Error as e:
            logging.error(f"Error obteniendo RUTs no procesados: {e}")
            return []
        finally:
            cursor.close()

    def obtener_estadisticas(self):
        """
        Obtiene estadísticas de los RUTs procesados
        Returns:
            dict: Diccionario con estadísticas
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()

        cursor = self.connection.cursor(dictionary=True)

        stats = {
            'total_ruts': 0,
            'procesados': 0,
            'con_datos': 0,
            'sin_procesar': 0
        }

        queries = {
            'total_ruts': "SELECT COUNT(*) as count FROM amanda_ruts_generados",
            'procesados': "SELECT COUNT(*) as count FROM amanda_ruts_generados WHERE procesado = TRUE",
            'con_datos': "SELECT COUNT(*) as count FROM amanda_ruts_generados WHERE tiene_datos = TRUE",
            'sin_procesar': "SELECT COUNT(*) as count FROM amanda_ruts_generados WHERE procesado = FALSE"
        }

        try:
            for key, query in queries.items():
                cursor.execute(query)
                result = cursor.fetchone()
                stats[key] = result['count'] if result else 0

            return stats
        except Error as e:
            logging.error(f"Error obteniendo estadísticas: {e}")
            return stats
        finally:
            cursor.close()

    def cerrar(self):
        """Cierra la conexión a la base de datos"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("Conexión a MySQL cerrada")
