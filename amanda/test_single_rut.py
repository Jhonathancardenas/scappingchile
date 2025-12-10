from selenium import webdriver
import sys
import os

# Agregar el directorio padre al path
sys.path.append(os.path.dirname(__file__))

from main import AmandaScraper

def test_single_rut():
    """Prueba con un RUT específico"""
    print("="*60)
    print("TEST SCRAPER CLUB AMANDA - RUT ESPECÍFICO")
    print("="*60)

    # RUT a probar
    rut_prueba = "15.452.243-3"

    print(f"\nProbando con RUT: {rut_prueba}")

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

    # Iniciar scraper
    scraper = AmandaScraper(driver=None, chrome_options=chrome_options)

    try:
        # Procesar el RUT
        scraper.procesar_ruts([rut_prueba])

        # Mostrar resultados en consola
        if scraper.resultados:
            print("\n" + "="*60)
            print("RESULTADOS EXTRAÍDOS:")
            print("="*60)

            resultado = scraper.resultados[0]

            print(f"\nRUT Consulta: {resultado['rut']}")
            print(f"Fecha Consulta: {resultado['fecha_consulta']}")

            formulario = resultado['datos_extraidos'].get('formulario')

            if formulario:
                print("\n--- DATOS DEL FORMULARIO ---")
                print(f"RUT: {formulario.get('rut', 'N/A')}")
                print(f"Género: {formulario.get('genero', 'N/A')}")
                print(f"Nombre y Apellido: {formulario.get('nombre_apellido', 'N/A')}")
                print(f"Fecha Nacimiento: {formulario.get('fecha_nacimiento', 'N/A')}")
                print(f"Edad: {formulario.get('edad', 'N/A')}")
                print(f"WhatsApp: {formulario.get('whatsapp', 'N/A')}")
                print(f"Email: {formulario.get('email', 'N/A')}")
                print(f"Instagram: {formulario.get('instagram', 'N/A')}")
                print(f"Asistencias Previas: {formulario.get('asistencias_previas', 'N/A')}")
                print(f"Grab: {formulario.get('grab', 'N/A')}")
                print(f"RRPP: {formulario.get('rrpp', 'N/A')}")
            else:
                print("\n⚠ No se encontraron datos del formulario (el RUT podría no estar registrado)")

            if resultado['datos_extraidos'].get('errores'):
                print(f"\nErrores: {resultado['datos_extraidos']['errores']}")

            if resultado['datos_extraidos'].get('mensajes'):
                print(f"\nMensajes: {resultado['datos_extraidos']['mensajes']}")

            print(f"\nScreenshot guardado en: {resultado['screenshot']}")

        # Guardar resultados en múltiples formatos
        print("\n" + "="*60)
        print("GUARDANDO RESULTADOS...")
        print("="*60)

        scraper.guardar_resultados(formato='json')
        scraper.guardar_resultados(formato='csv')
        scraper.guardar_resultados(formato='excel')

    except Exception as e:
        print(f"\n✗ Error general: {e}")
        import traceback
        traceback.print_exc()

    finally:
        scraper.cerrar()
        print("\n✓ Proceso completado")


if __name__ == "__main__":
    test_single_rut()
