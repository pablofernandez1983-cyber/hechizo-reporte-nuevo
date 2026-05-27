"""
bootstrap_tn_session.py — Regenera tiendanube_state.json para GitHub Actions

USO (una sola vez cuando la sesión vence):
  cd Hechizo/Reporte/hechizo-reporte-nuevo
  python bootstrap_tn_session.py

Qué hace:
  1. Abre Chromium CON interfaz gráfica (headed)
  2. Navega al admin de TiendaNube
  3. Vos te logeás manualmente (o ya estás logueado)
  4. Presionás ENTER en la terminal cuando el admin esté abierto
  5. Guarda cookies + localStorage en tiendanube_state.json
  6. Imprime el contenido listo para pegar en el GitHub Secret TN_STATE_JSON

Requisitos:
  pip install playwright
  playwright install chromium
"""

import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

ADMIN_URL   = "https://hechizobijou.mitiendanube.com/admin/v2/dashboard/"
OUTPUT_FILE = Path("tiendanube_state.json")

def main():
    print("=" * 60)
    print("BOOTSTRAP — Sesión TiendaNube para GitHub Actions")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,          # ← CON ventana, para que puedas logearte
            args=["--start-maximized"],
        )
        context = browser.new_context(
            viewport=None,           # respeta el tamaño maximizado
            accept_downloads=True,
        )
        page = context.new_page()

        print(f"\n[→] Abriendo {ADMIN_URL} ...")
        page.goto(ADMIN_URL, wait_until="domcontentloaded", timeout=30000)

        print("\n[!] Si aparece el login, ingresá con tu usuario y contraseña.")
        print("[!] Una vez que el dashboard esté visible, volvé acá y presioná ENTER.")
        input("\n  >>> Presioná ENTER cuando estés en el dashboard de TiendaNube: ")

        # Verificar que estamos en el dashboard y no en el login
        if "login" in page.url.lower():
            print("\n[ERROR] Todavía estás en el login. Logeate primero y volvé a correr.")
            browser.close()
            return

        print(f"[OK] URL actual: {page.url}")
        print("[...] Guardando sesión...")

        # Guardar estado (cookies + storage)
        state = context.storage_state()
        OUTPUT_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        browser.close()

    # Leer el JSON guardado
    raw = OUTPUT_FILE.read_text(encoding="utf-8")
    data = json.loads(raw)
    cookies = data.get("cookies", [])

    print(f"\n[OK] Sesión guardada en: {OUTPUT_FILE.resolve()}")
    print(f"     Cookies guardadas: {len(cookies)}")

    print("\n" + "=" * 60)
    print("PRÓXIMO PASO: actualizar el GitHub Secret")
    print("=" * 60)
    print("""
  1. Abrí: https://github.com/pablofernandez070/hechizo-reporte-nuevo/settings/secrets/actions
     (o el repo correcto si el nombre es diferente)

  2. Buscá el secret: TN_STATE_JSON

  3. Hacé click en "Update" y pegá el contenido de:
""")
    print(f"       {OUTPUT_FILE.resolve()}")
    print("""
     (el archivo completo, tal cual — es un JSON)

  4. Guardá el secret.

  5. Ejecutá el workflow manualmente desde Actions para verificar.
""")

    # Intentar copiar al portapapeles (Windows)
    try:
        import subprocess
        subprocess.run(
            ["powershell", "-Command", f"Get-Content '{OUTPUT_FILE.resolve()}' | Set-Clipboard"],
            check=True, capture_output=True
        )
        print("[OK] Contenido copiado al portapapeles — pegalo directamente en GitHub.")
    except Exception:
        print("[INFO] No se pudo copiar al portapapeles — copialo manualmente del archivo.")

    print("\n✅ Bootstrap completado.")


if __name__ == "__main__":
    main()
