import streamlit as st
import pandas as pd
import pydeck as pdk
import json

# Sayfa ayarları
st.set_page_config(page_title="Security Incident Monitor", layout="wide")
st.title("🌍 Küresel Güvenlik Olayları Paneli")

# 1. Veriyi Okuma ve İşleme
@st.cache_data
def load_data():
    # Dosya yolu repodaki yeni yapınıza göre ayarlandı
    with open("data/incidents.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    # Sadece "incidents" dizisini DataFrame'e çeviriyoruz
    df = pd.DataFrame(raw_data["incidents"])
    
    # Haritada hata almamak için koordinatı eksik (null) olan satırları temizliyoruz
    map_df = df.dropna(subset=["geo_lat", "geo_lon"]).copy()
    
    # Risk seviyelerine göre renk kodlaması
    def get_color(severity):
        if severity == "critical": return [255, 0, 0, 200]       
        elif severity == "high": return [255, 140, 0, 200]      
        elif severity == "medium": return [255, 255, 0, 200]    
        else: return [0, 150, 255, 200]                         
        
    map_df["color"] = map_df["severity"].apply(get_color)
    
    return map_df, df, raw_data["metadata"]

# Verileri yüklüyoruz (map_df harita için, raw_df tüm veriler için)
map_df, raw_df, metadata = load_data()

# 2. Yan Menü (Filtreleme ve Metadata Gösterimi)
st.sidebar.header("İstihbarat Filtreleri")
st.sidebar.caption(f"Proje: {metadata.get('project', 'N/A')} v{metadata.get('version', 'N/A')}")
st.sidebar.caption(f"Son Güncelleme: {metadata.get('last_updated', 'N/A')}")
st.sidebar.markdown("---")

secilen_tur = st.sidebar.multiselect(
    "Olay Türü (Incident Type)",
    options=map_df["incident_type"].unique(),
    default=map_df["incident_type"].unique()
)

secilen_risk = st.sidebar.multiselect(
    "Risk Seviyesi (Severity)",
    options=map_df["severity"].unique(),
    default=map_df["severity"].unique()
)

# Filtreleri sadece koordinatı olan harita verisine uyguluyoruz
filtrelenmis_map_df = map_df[(map_df["incident_type"].isin(secilen_tur)) & (map_df["severity"].isin(secilen_risk))]

# 3. Özet İstatistikler ve GPU Destekli WebGL Harita Çizimi (PyDeck)
st.subheader("İstihbarat Özeti ve Harita")

# Eğer filtreler sonucu veri boş değilse haritayı ve metrikleri çiz
if not filtrelenmis_map_df.empty:
    
    # Özet İstatistik Metrikleri (KPI)
    col1, col2, col3 = st.columns(3)
    col1.metric("Filtrelenen Olay", len(filtrelenmis_map_df))
    col2.metric("Kritik/Yüksek Riskli", len(filtrelenmis_map_df[filtrelenmis_map_df['severity'].isin(['critical', 'high'])]))
    col3.metric("Etkilenen Ülke Sayısı", filtrelenmis_map_df['country'].nunique())
    
    st.markdown("---")
    
    # PyDeck Katmanı
    layer = pdk.Layer(
        'ScatterplotLayer',
        data=filtrelenmis_map_df,
        get_position='[geo_lon, geo_lat]', 
        get_color='color',                 
        get_radius=50000,                  
        pickable=True,                     
        opacity=0.8,
        stroked=True,
        filled=True,
        radius_scale=1,
        radius_min_pixels=6,
        radius_max_pixels=25,
    )

    # Başlangıç kamera açısı (Türkiye ve Avrupa merkezli, hafif açılı 3D görünüm)
    view_state = pdk.ViewState(
        latitude=40.0,
        longitude=30.0,
        zoom=3,
        pitch=45 
    )

    # Harita Objesi ve Tooltip
    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": """
                <b>Tür:</b> {incident_type} <br/>
                <b>Risk:</b> {severity} <br/>
                <b>Konum:</b> {city}, {country} <br/>
                <b>Özet:</b> {summary_tr} <br/>
                <b>Tarih:</b> {date} <br/>
                <b>Hedef:</b> {venue_name}
            """,
            "style": {"backgroundColor": "#222222", "color": "white", "borderRadius": "5px", "padding": "10px"}
        },
        map_style='mapbox://styles/mapbox/dark-v10', 
    )

    # Haritayı bas
    st.pydeck_chart(r)

else:
    # Kullanıcı tüm filtreleri kaldırdığında haritanın çökmesini engelleyen uyarı
    st.warning("⚠️ Lütfen haritada veri görebilmek için yan menüden en az bir Olay Türü ve Risk Seviyesi seçin.")

# 4. Veri Doğrulama ve Ham Tablo
st.markdown("---")
if st.checkbox("Detaylı İstihbarat Verilerini Göster (Koordinatsızlar Dahil)"):
    # Tabloda her şeyi göstermek için filtreyi ana veriye de uygulayalım
    tablo_df = raw_df[(raw_df["incident_type"].isin(secilen_tur)) & (raw_df["severity"].isin(secilen_risk))]
    
    # Ekrana sığması için sadece en önemli sütunları seçiyoruz
    st.dataframe(
        tablo_df[["date", "incident_type", "severity", "country", "city", "summary_tr", "quality_score", "source_urls"]],
        use_container_width=True
    )
