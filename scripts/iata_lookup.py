# scripts/iata_lookup.py

"""
Havalimanı IATA kodu sözlüğü.
AI bulamazsa bu sözlükten tamamlar.
"""


# Dünya genelinde önemli havalimanları
IATA_DATABASE = {
    # ── TÜRKİYE ──
    "istanbul airport": "IST",
    "istanbul havalimani": "IST",
    "istanbul havalimanı": "IST",
    "ataturk airport": "ISL",
    "atatürk havalimanı": "ISL",
    "sabiha gokcen": "SAW",
    "sabiha gökçen": "SAW",
    "ankara esenboga": "ESB",
    "ankara esenboğa": "ESB",
    "antalya airport": "AYT",
    "antalya havalimanı": "AYT",
    "izmir adnan menderes": "ADB",
    "dalaman airport": "DLM",
    "bodrum milas": "BJV",
    "trabzon airport": "TZX",
    "gaziantep airport": "GZT",
    "adana airport": "ADA",
    "diyarbakir airport": "DIY",
    "diyarbakır havalimanı": "DIY",
    "kayseri airport": "ASR",
    "konya airport": "KYA",

    # ── ORTADOĞU ──
    "dubai airport": "DXB",
    "dubai international": "DXB",
    "al maktoum airport": "DWC",
    "abu dhabi airport": "AUH",
    "abu dhabi international": "AUH",
    "sharjah airport": "SHJ",
    "doha airport": "DOH",
    "hamad international": "DOH",
    "riyadh airport": "RUH",
    "king khalid airport": "RUH",
    "jeddah airport": "JED",
    "king abdulaziz": "JED",
    "medina airport": "MED",
    "dammam airport": "DMM",
    "muscat airport": "MCT",
    "bahrain airport": "BAH",
    "kuwait airport": "KWI",
    "kuwait international": "KWI",
    "baghdad airport": "BGW",
    "baghdad international": "BGW",
    "erbil airport": "EBL",
    "basra airport": "BSR",
    "beirut airport": "BEY",
    "rafic hariri": "BEY",
    "amman airport": "AMM",
    "queen alia": "AMM",
    "ben gurion": "TLV",
    "tel aviv airport": "TLV",
    "damascus airport": "DAM",
    "aleppo airport": "ALP",
    "tehran airport": "IKA",
    "imam khomeini": "IKA",
    "mehrabad airport": "THR",
    "isfahan airport": "IFN",
    "sanaa airport": "SAH",
    "aden airport": "ADE",

    # ── AFRİKA ──
    "cairo airport": "CAI",
    "cairo international": "CAI",
    "hurghada airport": "HRG",
    "sharm el sheikh": "SSH",
    "casablanca airport": "CMN",
    "mohammed v airport": "CMN",
    "marrakech airport": "RAK",
    "algiers airport": "ALG",
    "tunis airport": "TUN",
    "tripoli airport": "TIP",
    "mitiga airport": "MJI",
    "khartoum airport": "KRT",
    "addis ababa airport": "ADD",
    "bole airport": "ADD",
    "nairobi airport": "NBO",
    "jomo kenyatta": "NBO",
    "mogadishu airport": "MGQ",
    "aden abdulle": "MGQ",
    "entebbe airport": "EBB",
    "dar es salaam": "DAR",
    "lagos airport": "LOS",
    "murtala muhammed": "LOS",
    "abuja airport": "ABV",
    "johannesburg airport": "JNB",
    "or tambo": "JNB",
    "cape town airport": "CPT",

    # ── AVRUPA ──
    "heathrow": "LHR",
    "london heathrow": "LHR",
    "gatwick": "LGW",
    "london gatwick": "LGW",
    "stansted": "STN",
    "luton airport": "LTN",
    "manchester airport": "MAN",
    "edinburgh airport": "EDI",
    "charles de gaulle": "CDG",
    "paris cdg": "CDG",
    "orly airport": "ORY",
    "paris orly": "ORY",
    "schiphol": "AMS",
    "amsterdam airport": "AMS",
    "frankfurt airport": "FRA",
    "munich airport": "MUC",
    "berlin airport": "BER",
    "berlin brandenburg": "BER",
    "brussels airport": "BRU",
    "zaventem": "BRU",
    "madrid airport": "MAD",
    "madrid barajas": "MAD",
    "barcelona airport": "BCN",
    "el prat": "BCN",
    "rome fiumicino": "FCO",
    "fiumicino airport": "FCO",
    "milan malpensa": "MXP",
    "zurich airport": "ZRH",
    "vienna airport": "VIE",
    "dublin airport": "DUB",
    "lisbon airport": "LIS",
    "athens airport": "ATH",
    "moscow sheremetyevo": "SVO",
    "sheremetyevo": "SVO",
    "domodedovo": "DME",
    "vnukovo": "VKO",
    "copenhagen airport": "CPH",
    "oslo airport": "OSL",
    "stockholm arlanda": "ARN",
    "helsinki airport": "HEL",
    "warsaw airport": "WAW",
    "prague airport": "PRG",
    "budapest airport": "BUD",

    # ── AMERİKA ──
    "jfk airport": "JFK",
    "john f kennedy": "JFK",
    "newark airport": "EWR",
    "laguardia": "LGA",
    "los angeles airport": "LAX",
    "lax airport": "LAX",
    "chicago ohare": "ORD",
    "ohare airport": "ORD",
    "midway airport": "MDW",
    "atlanta airport": "ATL",
    "hartsfield jackson": "ATL",
    "hartsfield-jackson": "ATL",
    "dallas fort worth": "DFW",
    "dfw airport": "DFW",
    "denver airport": "DEN",
    "san francisco airport": "SFO",
    "seattle airport": "SEA",
    "miami airport": "MIA",
    "orlando airport": "MCO",
    "boston logan": "BOS",
    "logan airport": "BOS",
    "phoenix airport": "PHX",
    "houston airport": "IAH",
    "george bush airport": "IAH",
    "detroit airport": "DTW",
    "minneapolis airport": "MSP",
    "fort lauderdale": "FLL",
    "savannah airport": "SAV",
    "savannah/hilton head": "SAV",
    "vero beach airport": "VRB",
    "washington dulles": "IAD",
    "dulles airport": "IAD",
    "reagan airport": "DCA",
    "ronald reagan": "DCA",
    "toronto pearson": "YYZ",
    "vancouver airport": "YVR",
    "mexico city airport": "MEX",
    "bogota airport": "BOG",
    "el dorado airport": "BOG",
    "lima airport": "LIM",
    "sao paulo guarulhos": "GRU",
    "buenos aires ezeiza": "EZE",

    # ── ASYA ──
    "beijing airport": "PEK",
    "beijing capital": "PEK",
    "beijing daxing": "PKX",
    "shanghai pudong": "PVG",
    "shanghai hongqiao": "SHA",
    "guangzhou airport": "CAN",
    "shenzhen airport": "SZX",
    "hong kong airport": "HKG",
    "chek lap kok": "HKG",
    "tokyo narita": "NRT",
    "narita airport": "NRT",
    "tokyo haneda": "HND",
    "haneda airport": "HND",
    "incheon airport": "ICN",
    "seoul incheon": "ICN",
    "gimpo airport": "GMP",
    "singapore changi": "SIN",
    "changi airport": "SIN",
    "bangkok suvarnabhumi": "BKK",
    "suvarnabhumi": "BKK",
    "don mueang": "DMK",
    "kuala lumpur airport": "KUL",
    "klia airport": "KUL",
    "mumbai airport": "BOM",
    "chhatrapati shivaji": "BOM",
    "delhi airport": "DEL",
    "indira gandhi airport": "DEL",
    "chennai airport": "MAA",
    "bangalore airport": "BLR",
    "colombo airport": "CMB",
    "bandaranaike": "CMB",
    "islamabad airport": "ISB",
    "karachi airport": "KHI",
    "lahore airport": "LHE",
    "kabul airport": "KBL",
    "hamid karzai": "KBL",
    "kathmandu airport": "KTM",
    "dhaka airport": "DAC",
    "yangon airport": "RGN",
    "hanoi airport": "HAN",
    "noi bai": "HAN",
    "ho chi minh airport": "SGN",
    "tan son nhat": "SGN",
    "manila airport": "MNL",
    "ninoy aquino": "MNL",
    "jakarta airport": "CGK",
    "soekarno-hatta": "CGK",
    "bali airport": "DPS",
    "ngurah rai": "DPS",

    # ── OKYANUSYA ──
    "sydney airport": "SYD",
    "kingsford smith": "SYD",
    "melbourne airport": "MEL",
    "brisbane airport": "BNE",
    "auckland airport": "AKL",
}


def find_iata(airport_name: str, city: str = "", country: str = "") -> str:
    """
    Havalimanı adından IATA kodunu bul.
    Birden fazla strateji dener.
    """
    if not airport_name or airport_name.lower() in ("unknown", "null", ""):
        # Şehir adından dene
        if city and city.lower() not in ("unknown", "null", ""):
            return _search(f"{city} airport")
        return None

    name = airport_name.lower().strip()

    # 1. Tam eşleşme
    if name in IATA_DATABASE:
        return IATA_DATABASE[name]

    # 2. Kısmi eşleşme (sözlükteki key, ismin içinde geçiyor mu)
    for pattern, iata in IATA_DATABASE.items():
        if pattern in name:
            return iata

    # 3. İsim sözlükteki key'in içinde geçiyor mu
    for pattern, iata in IATA_DATABASE.items():
        if name in pattern:
            return iata

    # 4. Şehir adıyla dene
    if city and city.lower() not in ("unknown", "null", ""):
        result = _search(f"{city.lower()} airport")
        if result:
            return result

    return None


def _search(query: str) -> str:
    """Basit arama."""
    query = query.lower().strip()
    if query in IATA_DATABASE:
        return IATA_DATABASE[query]
    for pattern, iata in IATA_DATABASE.items():
        if pattern in query or query in pattern:
            return iata
    return None
