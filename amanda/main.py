from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import random
import time
import datetime
import logging
import pandas as pd
import json
import os
import sys
from database import AmandaDatabase

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='amanda_scraping.log',
    filemode='w'
)

class RutGenerator:
    """Generador de RUTs chilenos válidos y aleatorios"""

    @staticmethod
    def calcular_digito_verificador(rut):
        """
        Calcula el dígito verificador de un RUT chileno
        Args:
            rut (int): RUT sin dígito verificador
        Returns:
            str: Dígito verificador (puede ser número o 'K')
        """
        reversed_digits = str(rut)[::-1]
        factors = [2, 3, 4, 5, 6, 7]
        s = 0

        for i, digit in enumerate(reversed_digits):
            s += int(digit) * factors[i % 6]

        remainder = s % 11
        dv = 11 - remainder

        if dv == 11:
            return '0'
        elif dv == 10:
            return 'K'
        else:
            return str(dv)

    @staticmethod
    def generar_rut_valido():
        """
        Genera un RUT chileno válido aleatorio
        Returns:
            str: RUT formateado (ej: 12.345.678-9)
        """
        # Generar número base aleatorio entre 18.000 y 20.000
        rut_base = random.randint(18000000, 20000000)

        # Calcular dígito verificador
        dv = RutGenerator.calcular_digito_verificador(rut_base)

        # Formatear RUT según su longitud
        rut_str = str(rut_base)

        # Para RUTs de 5 dígitos (18.000-20.000): formato 12.345-6
        if len(rut_str) <= 10000:
            if len(rut_str) <= 3:
                rut_formateado = f"{rut_str}-{dv}"
            else:
                rut_formateado = f"{rut_str[:-3]}.{rut_str[-3:]}-{dv}"
        # Para RUTs de 6 dígitos: formato 123.456-7
        elif len(rut_str) == 6:
            rut_formateado = f"{rut_str[:-3]}.{rut_str[-3:]}-{dv}"
        # Para RUTs de 7+ dígitos: formato 12.345.678-9
        else:
            rut_formateado = f"{rut_str[:-6]}.{rut_str[-6:-3]}.{rut_str[-3:]}-{dv}"

        return rut_formateado

    @staticmethod
    def generar_multiples_ruts(cantidad):
        """
        Genera múltiples RUTs válidos
        Args:
            cantidad (int): Número de RUTs a generar
        Returns:
            list: Lista de RUTs formateados
        """
        return [RutGenerator.generar_rut_valido() for _ in range(cantidad)]


class AmandaScraper:
    """Scraper para la página de Club Amanda"""

    def __init__(self, driver, chrome_options, usar_db=True):
        self.driver = driver or webdriver.Chrome(options=chrome_options)
        self.url = "https://amanda.reservame.cl/e/?I=marjorie&e=QRYU"
        self.resultados = []
        self.usar_db = usar_db
        self.db = AmandaDatabase() if usar_db else None

        # Mostrar estadísticas iniciales si se usa la base de datos
        if self.usar_db and self.db:
            stats = self.db.obtener_estadisticas()
            print(f"\n📊 Estadísticas de base de datos:")
            print(f"  • Total RUTs registrados: {stats['total_ruts']}")
            print(f"  • RUTs procesados: {stats['procesados']}")
            print(f"  • RUTs con datos: {stats['con_datos']}")
            print(f"  • RUTs sin procesar: {stats['sin_procesar']}\n")

    def procesar_ruts(self, lista_ruts):
        """
        Procesa una lista de RUTs y extrae la información
        Args:
            lista_ruts (list): Lista de RUTs a procesar
        """
        print(f"Iniciando procesamiento de {len(lista_ruts)} RUTs...")

        # Guardar RUTs en la base de datos y filtrar duplicados
        if self.usar_db and self.db:
            ruts_nuevos = []
            for rut in lista_ruts:
                # Intentar guardar directamente; retorna False si ya existe
                if self.db.guardar_rut_generado(rut):
                    ruts_nuevos.append(rut)
                    print(f"✓ RUT {rut} agregado a la base de datos")
                else:
                    print(f"⚠ RUT {rut} ya existe en la base de datos, se omite")

            lista_ruts = ruts_nuevos
            print(f"\nRUTs nuevos a procesar: {len(lista_ruts)}")

        for i, rut in enumerate(lista_ruts, 1):
            try:
                print(f"\nProcesando RUT {i}/{len(lista_ruts)}: {rut}")
                resultado = self.consultar_rut(rut)

                if resultado:
                    self.resultados.append(resultado)
                    print(f"✓ Datos extraídos exitosamente")

                    # Guardar en MySQL
                    if self.usar_db and self.db:
                        self.db.guardar_formulario(resultado)
                        self.db.marcar_rut_procesado(rut, tiene_datos=True)
                else:
                    print(f"✗ No se pudieron extraer datos")

                    # Marcar como procesado sin datos
                    if self.usar_db and self.db:
                        self.db.marcar_rut_procesado(rut, tiene_datos=False)

                # Espera entre consultas para no sobrecargar el servidor
                #time.sleep(random.uniform(2, 4))

            except Exception as e:
                logging.error(f"Error procesando RUT {rut}: {e}")
                print(f"✗ Error: {e}")

                # Marcar como procesado sin datos en caso de error
                if self.usar_db and self.db:
                    self.db.marcar_rut_procesado(rut, tiene_datos=False)
                continue

        print(f"\n{'='*60}")
        print(f"Procesamiento completado: {len(self.resultados)}/{len(lista_ruts)} exitosos")
        print(f"{'='*60}")

    def consultar_rut(self, rut):
        """
        Consulta un RUT específico y extrae los datos
        Args:
            rut (str): RUT a consultar
        Returns:
            dict: Diccionario con los datos extraídos
        """
        try:
            # Navegar a la URL
            self.driver.get(self.url)

            # Esperar a que el campo RUT esté disponible
            rut_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "usrRut"))
            )

            # Limpiar y llenar el campo
            rut_input.clear()
            rut_input.send_keys(rut)
            time.sleep(1)

            # Buscar el botón de envío
            submit_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "sumbit2"))
            )

            # Click en el botón
            submit_button.click()

            # Esperar a que se cargue la respuesta
            #time.sleep(3)

            # Extraer todos los datos visibles de la página
            datos_extraidos = self.extraer_datos_pagina()

            # Verificar si hay datos REALES en el formulario (no solo género checkeado)
            # Verificamos campos importantes como RUT, nombre o fecha de nacimiento
            formulario = datos_extraidos.get('formulario') or {}
            tiene_datos = bool(
                formulario.get('rut') or
                formulario.get('nombre_apellido') or
                formulario.get('fecha_nacimiento') or
                formulario.get('email') or
                formulario.get('whatsapp')
            )

            # Tomar screenshot SOLO si hay datos REALES en el formulario
            screenshot_path = None
            if tiene_datos:
                screenshot_path = self.tomar_screenshot(rut)
                #logging.info(f"Screenshot tomado para RUT {rut} - Datos encontrados: {list(formulario.keys())}")
            else:
                logging.info(f"No se tomó screenshot para RUT {rut} - Sin datos relevantes (solo género o vacío)")

            # Solo retornar resultado si hay datos
            if not tiene_datos:
                return None

            return {
                'rut': rut,
                'fecha_consulta': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'datos_extraidos': datos_extraidos,
                'screenshot': screenshot_path,
                'html_content': self.driver.page_source[:1000]  # Primeros 1000 caracteres
            }

        except TimeoutException:
            logging.error(f"Timeout esperando elementos para RUT {rut}")
            return None
        except Exception as e:
            logging.error(f"Error en consultar_rut: {e}")
            return None

    def extraer_datos_formulario(self):
        """
        Extrae datos específicos del formulario cuando hay valores prellenados
        Returns:
            dict: Datos del formulario o None si no hay datos
        """
        datos_formulario = {}

        try:
            # Extraer RUT
            try:
                rut_input = self.driver.find_element(By.ID, "RUTOK")
                rut_value = rut_input.get_attribute('value')
                if rut_value:
                    datos_formulario['rut'] = rut_value
            except NoSuchElementException:
                pass

            # Extraer Género
            try:
                genero_h = self.driver.find_element(By.ID, "genero_0")
                genero_m = self.driver.find_element(By.ID, "genero_1")

                if genero_h.is_selected():
                    datos_formulario['genero'] = 'Hombre'
                elif genero_m.is_selected():
                    datos_formulario['genero'] = 'Mujer'
            except NoSuchElementException:
                pass

            # Extraer Nombre y Apellido
            try:
                nombre_input = self.driver.find_element(By.ID, "nombre")
                nombre_value = nombre_input.get_attribute('value')
                if nombre_value:
                    datos_formulario['nombre_apellido'] = nombre_value
            except NoSuchElementException:
                pass

            # Extraer Fecha de Nacimiento
            try:
                fn_input = self.driver.find_element(By.ID, "FNtime")
                fn_value = fn_input.get_attribute('value')
                if fn_value:
                    datos_formulario['fecha_nacimiento'] = fn_value

                # Intentar extraer la edad del texto siguiente
                try:
                    edad_text = fn_input.find_element(By.XPATH, "following-sibling::text()[1]")
                    if edad_text:
                        datos_formulario['edad'] = edad_text.strip()
                except:
                    pass
            except NoSuchElementException:
                pass

            # Extraer WhatsApp
            try:
                wssp_input = self.driver.find_element(By.ID, "wssp")
                wssp_value = wssp_input.get_attribute('value')
                if wssp_value:
                    datos_formulario['whatsapp'] = f"+{wssp_value}"
            except NoSuchElementException:
                pass

            # Extraer Email
            try:
                email_input = self.driver.find_element(By.ID, "email")
                email_value = email_input.get_attribute('value')
                if email_value:
                    datos_formulario['email'] = email_value
            except NoSuchElementException:
                pass

            # Extraer Instagram
            try:
                instagram_input = self.driver.find_element(By.ID, "instagram")
                instagram_value = instagram_input.get_attribute('value')
                if instagram_value:
                    datos_formulario['instagram'] = instagram_value
            except NoSuchElementException:
                pass

            # Extraer campos ocultos
            try:
                grab_input = self.driver.find_element(By.ID, "grab")
                grab_value = grab_input.get_attribute('value')
                if grab_value:
                    datos_formulario['grab'] = grab_value
            except NoSuchElementException:
                pass

            try:
                rrpp_input = self.driver.find_element(By.ID, "rrpp")
                rrpp_value = rrpp_input.get_attribute('value')
                if rrpp_value:
                    datos_formulario['rrpp'] = rrpp_value
            except NoSuchElementException:
                pass

            # Extraer información de asistencias anteriores
            try:
                body_text = self.driver.find_element(By.TAG_NAME, "body").text
                if "Ha asistido" in body_text:
                    import re
                    match = re.search(r'Ha asistido (\d+) veces', body_text)
                    if match:
                        datos_formulario['asistencias_previas'] = int(match.group(1))
            except:
                pass

            # Si no hay datos del formulario, retornar None
            if not datos_formulario:
                return None

            return datos_formulario

        except Exception as e:
            logging.error(f"Error extrayendo datos del formulario: {e}")
            return None

    def extraer_datos_pagina(self):
        """
        Extrae todos los datos visibles de la página actual
        Returns:
            dict: Datos extraídos de la página
        """
        datos = {
            'texto_completo': '',
            'mensajes': [],
            'errores': [],
            'tablas': [],
            'links': [],
            'formulario': None
        }

        try:
            # Extraer todo el texto visible
            body = self.driver.find_element(By.TAG_NAME, "body")
            datos['texto_completo'] = body.text

            # Extraer datos específicos del formulario
            datos['formulario'] = self.extraer_datos_formulario()

            # Buscar mensajes de error
            try:
                error_element = self.driver.find_element(By.ID, "txtError")
                if error_element.text:
                    datos['errores'].append(error_element.text)
            except NoSuchElementException:
                pass

            # Buscar mensajes de éxito o información
            try:
                mensajes = self.driver.find_elements(By.CSS_SELECTOR, ".alert, .message, .notification, .success, .info")
                for msg in mensajes:
                    if msg.text:
                        datos['mensajes'].append(msg.text)
            except NoSuchElementException:
                pass

            # Buscar tablas de datos
            try:
                tablas = self.driver.find_elements(By.TAG_NAME, "table")
                for i, tabla in enumerate(tablas):
                    datos['tablas'].append({
                        'tabla_index': i,
                        'contenido': tabla.text
                    })
            except NoSuchElementException:
                pass

            # Extraer links importantes
            try:
                links = self.driver.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute('href')
                    text = link.text
                    if href and text:
                        datos['links'].append({
                            'texto': text,
                            'url': href
                        })
            except NoSuchElementException:
                pass

        except Exception as e:
            logging.error(f"Error extrayendo datos: {e}")

        return datos

    def tomar_screenshot(self, rut):
        """
        Toma un screenshot de la página actual
        Args:
            rut (str): RUT para nombrar el archivo
        Returns:
            str: Ruta del screenshot guardado
        """
        try:
            # Crear directorio para screenshots
            screenshots_dir = os.path.join(os.path.dirname(__file__), 'screenshots')
            os.makedirs(screenshots_dir, exist_ok=True)

            # Generar nombre de archivo
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            rut_limpio = rut.replace('.', '').replace('-', '')
            filename = f"amanda_{rut_limpio}_{timestamp}.png"
            filepath = os.path.join(screenshots_dir, filename)

            # Tomar screenshot
            self.driver.save_screenshot(filepath)

            return filepath
        except Exception as e:
            logging.error(f"Error tomando screenshot: {e}")
            return None

    def guardar_resultados(self, formato='json'):
        """
        Guarda los resultados en archivo
        Args:
            formato (str): Formato de salida ('json', 'csv', 'excel')
        """
        if not self.resultados:
            print("No hay resultados para guardar")
            return

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        # Crear directorio para resultados
        results_dir = os.path.join(os.path.dirname(__file__), 'resultados')
        os.makedirs(results_dir, exist_ok=True)

        if formato == 'json':
            filename = f"amanda_resultados_{timestamp}.json"
            filepath = os.path.join(results_dir, filename)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.resultados, f, ensure_ascii=False, indent=2)

            print(f"\n✓ Resultados guardados en JSON: {filepath}")

        elif formato == 'csv':
            filename = f"amanda_resultados_{timestamp}.csv"
            filepath = os.path.join(results_dir, filename)

            # Convertir a DataFrame y aplanar datos anidados
            df_data = []
            for r in self.resultados:
                formulario = r['datos_extraidos'].get('formulario', {}) or {}

                flat_data = {
                    'rut_consulta': r['rut'],
                    'fecha_consulta': r['fecha_consulta'],
                    'rut_formulario': formulario.get('rut', ''),
                    'genero': formulario.get('genero', ''),
                    'nombre_apellido': formulario.get('nombre_apellido', ''),
                    'fecha_nacimiento': formulario.get('fecha_nacimiento', ''),
                    'edad': formulario.get('edad', ''),
                    'whatsapp': formulario.get('whatsapp', ''),
                    'email': formulario.get('email', ''),
                    'instagram': formulario.get('instagram', ''),
                    'asistencias_previas': formulario.get('asistencias_previas', ''),
                    'grab': formulario.get('grab', ''),
                    'rrpp': formulario.get('rrpp', ''),
                    'mensajes': str(r['datos_extraidos'].get('mensajes', [])),
                    'errores': str(r['datos_extraidos'].get('errores', [])),
                    'screenshot': r['screenshot']
                }
                df_data.append(flat_data)

            df = pd.DataFrame(df_data)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')

            print(f"\n✓ Resultados guardados en CSV: {filepath}")

        elif formato == 'excel':
            filename = f"amanda_resultados_{timestamp}.xlsx"
            filepath = os.path.join(results_dir, filename)

            # Convertir a DataFrame
            df_data = []
            for r in self.resultados:
                formulario = r['datos_extraidos'].get('formulario', {}) or {}

                flat_data = {
                    'RUT Consulta': r['rut'],
                    'Fecha Consulta': r['fecha_consulta'],
                    'RUT Formulario': formulario.get('rut', ''),
                    'Género': formulario.get('genero', ''),
                    'Nombre y Apellido': formulario.get('nombre_apellido', ''),
                    'Fecha Nacimiento': formulario.get('fecha_nacimiento', ''),
                    'Edad': formulario.get('edad', ''),
                    'WhatsApp': formulario.get('whatsapp', ''),
                    'Email': formulario.get('email', ''),
                    'Instagram': formulario.get('instagram', ''),
                    'Asistencias Previas': formulario.get('asistencias_previas', ''),
                    'Grab': formulario.get('grab', ''),
                    'RRPP': formulario.get('rrpp', ''),
                    'Mensajes': str(r['datos_extraidos'].get('mensajes', [])),
                    'Errores': str(r['datos_extraidos'].get('errores', [])),
                    'Screenshot': r['screenshot']
                }
                df_data.append(flat_data)

            df = pd.DataFrame(df_data)
            df.to_excel(filepath, index=False, engine='openpyxl')

            print(f"\n✓ Resultados guardados en Excel: {filepath}")

    def cerrar(self):
        """Cierra el navegador y la conexión a la base de datos"""
        if self.driver:
            self.driver.quit()

        # Cerrar conexión a la base de datos
        if self.usar_db and self.db:
            # Mostrar estadísticas finales
            stats = self.db.obtener_estadisticas()
            print(f"\n📊 Estadísticas finales:")
            print(f"  • Total RUTs registrados: {stats['total_ruts']}")
            print(f"  • RUTs procesados: {stats['procesados']}")
            print(f"  • RUTs con datos: {stats['con_datos']}")
            print(f"  • RUTs sin procesar: {stats['sin_procesar']}")

            self.db.cerrar()


def main():
    """Función principal"""
    print("="*60)
    print("SCRAPER CLUB AMANDA - GENERACIÓN Y CONSULTA DE RUTS")
    print("="*60)

    # Configurar opciones de Chrome
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument('--headless=new')  # Descomentar para modo headless
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--incognito')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-notifications')
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # Cantidad de RUTs a generar y procesar
    CANTIDAD_RUTS = 50000 # Cambiar este número según necesidad

    # Generar RUTs válidos
    print(f"\nGenerando {CANTIDAD_RUTS} RUTs válidos aleatorios...")
    ruts_generados = RutGenerator.generar_multiples_ruts(CANTIDAD_RUTS)

    print("\nRUTs generados:")
    for i, rut in enumerate(ruts_generados, 1):
        print(f"  {i}. {rut}")

    # Iniciar scraper
    scraper = AmandaScraper(driver=None, chrome_options=chrome_options)

    try:
        # Procesar RUTs
        scraper.procesar_ruts(ruts_generados)

        # Guardar resultados en múltiples formatos
        scraper.guardar_resultados(formato='json')
        scraper.guardar_resultados(formato='csv')
        scraper.guardar_resultados(formato='excel')

    except Exception as e:
        logging.error(f"Error en main: {e}")
        print(f"\n✗ Error general: {e}")

    finally:
        scraper.cerrar()
        print("\n✓ Proceso completado")


if __name__ == "__main__":
    main()
