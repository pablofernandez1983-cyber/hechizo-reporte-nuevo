"""
categorias_producto.py — Clasifica cada producto vendido en grupo (Bijou / Home / Ropa / Otros) y
subcategoría a partir de su NOMBRE.

Por qué por nombre y no por las categorías de Tiendanube: ~40% de lo facturado en la historia es de
productos que ya se borraron de la tienda (no tienen categoría consultable), y las categorías de TN
mezclan tipo de producto con "líneas" (Protección, Piedras, Plata 925...). El nombre siempre empieza
por el tipo ("Collar ...", "Pulsera ...", "Porta sahumerio ..."), así que es consistente en toda la
historia. Las reglas van en orden: gana la primera que coincide.
"""

import re
import unicodedata

BIJOU, HOME, ROPA, OTROS = "Bijou", "Home", "Ropa", "Otros"

# (grupo, subcategoría, regex sobre el nombre normalizado: minúsculas, sin tildes)
REGLAS = [
    (ROPA, "Remeras", r"^(remera|musculosa|top)\b"),
    (ROPA, "Buzos", r"^(buzo|buzito|campera|hoodie)"),
    (OTROS, "Gift cards", r"gift ?card|tarjeta de regalo"),
    (OTROS, "Stickers y escarapelas", r"^(stickers?|dtf|plancha dtf|escarapela|pin)\b"),
    (OTROS, "Bolsos y neceser", r"^(mega bolso|bolso|neceser|valijita|bolsita|carpita|correas?)\b"),

    (HOME, "Kits y Hechizo Box", r"^(kit|hechizo|mini hechizo box|box|sobre box|pack|combo|ritual)\b"),
    (BIJOU, "Anillos", r"^anillo"),
    (BIJOU, "Aros", r"^(aros?|aritos|trio (de )?aros|argollas?|abridor)\b"),
    (BIJOU, "Tobilleras", r"^(tobillera)"),
    (BIJOU, "Pulseras", r"^(pulsera|esclava|duo|set|tarjeta|trio de pulseras|denario)"),
    (BIJOU, "Llaveros", r"^llavero"),
    (BIJOU, "Collares", r"^(collar|trio de collares|triple collar|gargantilla|cadena|dije|relicario|cho(c)?ker|conjunto|rosario)"),
    (BIJOU, "Collares", r"^colgante\b.*(plata|acero|gold|dije|cadena)"),

    (HOME, "Altares y religiosos", r"^(mini altar|altar|altarcito|virgen|san |imagen|sagrada)"),
    (HOME, "Platitos", r"^(platito|plato|bandeja)"),
    (HOME, "Hornitos y calderos", r"^(hornito|caldero|maxi caldero|aguamanil)"),
    (HOME, "Porta sahumerios y sahumadores",
     r"^(porta|portasahumerio|sahumador|cuencos?|cuenquito|tablita|cascada|mini casita|casita)"),
    (HOME, "Sahumerios y hierbas", r"^(sahumerio|caja sahumerios|mix para sahumar|mix|hierbas|incienso|palo santo|frasco (de )?(hierbas|carbon|incienso)|pack de \d+ hierbas)"),
    (HOME, "Velas", r"^(vela|fusta|velita|velon|latita|candelabro|apaga)"),
    (HOME, "Aromas y áuricos", r"^(spray|agua florida|rocio|perfume|bruma|brisa|esencia|aceite|roll|anforas?|difusor|humidificador|sales|botella)"),
    (HOME, "Tazas y mates", r"^(taza|tazon|mate|matera|juego de taza)"),
    (HOME, "Oráculos", r"^oraculo"),
    (HOME, "Deco y amuletos",
     r"^(gatito|mini gatito|mini neko|neko|budas?|buditas?|trio de bud|colgante|atrapa|alfombra|esfera|ganesha|mini ganesha|mini maceta|frasquito|caja piedras|cajita|unicornio|monje|worry|piedras|punta|frasco|luminaria|alhajero|princesas)"),
]
_REGLAS = [(g, s, re.compile(rx)) for g, s, rx in REGLAS]

SIN_CLASIFICAR = "Sin clasificar"


def normalizar(nombre):
    s = nombre or ""
    try:  # algunos nombres viejos quedaron con doble encoding (ProtecciÃ³n)
        s = s.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    s = unicodedata.normalize("NFD", s.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip(" -:\"'“”")


def clasificar(nombre):
    """Devuelve (grupo, subcategoría)."""
    n = normalizar(nombre)
    for g, s, rx in _REGLAS:
        if rx.search(n):
            return g, s
    return OTROS, SIN_CLASIFICAR
