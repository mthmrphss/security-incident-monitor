import streamlit as st
import pandas as pd
import pydeck as pdk
import json

# --- 1. SAYFA VE ARAYÜZ AYARLARI ---
st.set_page_config(page_title="Security Incident Monitor", page_icon="🛡️", layout="wide")

# Sağ taraftaki haber akışını güzelleştirmek için ufak bir CSS dokunuşu
st.markdown("""
<style>
    .incident-date { font-size: 0.8rem; color: #888888; margin-bottom: 5px;}
    .incident-tags { font-size: 0.75rem; background-color: #333; padding: 2px 6px; border-radius: 4px; margin-right: 5px;}
</style>
""", unsafe_allow_html=True)

# --- 2. VERİ YÜKLEME VE İŞLEME ---
@st.cache_data
def load_data():
    with open("data/incidents.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    df = pd.DataFrame(raw_data["incidents"])
    
    # Harita için koordinatı olanları alıyoruz
    map_df = df.dropna(subset=["geo_lat", "geo_lon"]).copy()
    
    # Haber akışında en yeni olay en üstte çıksın diye tarihe göre sıralıyoruz
    map_df['parsed_date'] = pd.to_datetime(map_df['created_at'], errors='coerce')
    map_df = map_df.sort_values(by='parsed_date', ascending=False)
    
    # Risk seviyelerine göre renk kodlaması (RGBA)
    def get_color(severity):
        if severity == "critical": return [255, 50, 50, 200]       # Kırmızı
        elif severity == "high": return [255, 140, 0, 200]         # Turuncu
        elif severity == "medium": return [255, 215, 0, 200]       # Sarı
        else: return [50, 150, 255, 200]                           # Mavi (low)
        
    map_df["color"] = map_df["severity"].apply(get_color)
    
    return map_df, df, raw_data["metadata"]

map_df, raw_df, metadata = load_data()

# --- 3. YAN MENÜ (FİLTRELER) ---
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/c/ca/Shield_blank.svg/120px-Shield_blank.svg.png", width=60) # Temsili logo
    st.title("İstihbarat Kontrolü")
    st.caption(f"Veritabanı: v{metadata.get('version', 'N/A')} | Toplam Kayıt: {len(raw_df)}")
    st.markdown("---")

    secilen_tur = st.multiselect(
        "🎯 Olay Türü",
        options=map_df["incident_type"].unique(),
        default=map_df["incident_type"].unique()
    )

    secilen_risk = st.multiselect(
        "⚠️ Risk Seviyesi",
        options=map_df["severity"].unique(),
        default=map_df["severity"].unique()
    )

filtrelenmis_df = map_df[(map_df["incident_type"].isin(secilen_tur)) & (map_df["severity"].isin(secilen_risk))]


# --- 4. ANA EKRAN: KPI METRİKLERİ ---
st.title("🌍 Küresel Güvenlik Olayları Paneli")

if not filtrelenmis_df.empty:
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("📌 Filtrelenen Vaka", len(filtrelenmis_df))
    kpi2.metric("🔴 Kritik / Yüksek Risk", len(filtrelenmis_df[filtrelenmis_df['severity'].isin(['critical', 'high'])]))
    kpi3.metric("🏴 Etkilenen Ülke", filtrelenmis_df['country'].nunique())
    kpi4.metric("💀 Toplam Can Kaybı", int(filtrelenmis_df['casualties_dead'].sum()))
    st.markdown("---")

    # EKRANI İKİYE BÖLÜYORUZ: Harita (Sol) ve Haber Akışı (Sağ)
    col_map, col_feed = st.columns([2, 1], gap="large")

    # ---- SOL KOLON: HARİTA ----
    with col_map:
        st.subheader("🗺️ Operasyonel Harita")
        
        # PyDeck Katmanı (getColor Uyarısı Çözüldü -> get_fill_color)
        layer = pdk.Layer(
            'ScatterplotLayer',
            data=filtrelenmis_df,
            get_position='[geo_lon, geo_lat]', 
            get_fill_color='color',          # UYARI DÜZELTİLDİ
            get_line_color=[255, 255, 255, 80], # Dış çizgi rengi eklendi (Beyazımsı)
            get_radius=40000,                  
            pickable=True,                     
            opacity=0.8,
            stroked=True,
            filled=True,
            radius_scale=1,
            radius_min_pixels=6,
            radius_max_pixels=25,
            line_width_min_pixels=1,
        )

        view_state = pdk.ViewState(latitude=39.0, longitude=35.0, zoom=2.5, pitch=35)

        # Harita Objesi (Token Hatası Çözüldü -> Carto sağlayıcısına geçildi)
        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            map_provider="carto",             # HATA DÜZELTİLDİ: Token gerektirmeyen CartoDB
            map_style="dark",                 # HATA DÜZELTİLDİ: Carto Dark Matter teması
            tooltip={
                "html": "<b>{city}, {country}</b><br/><b>Tür:</b> {incident_type} | <b>Risk:</b> {severity}<br/><b>Özet:</b> {summary_tr}",
                "style": {"backgroundColor": "#1E1E1E", "color": "white", "borderRadius": "8px", "padding": "10px", "border": "1px solid #444"}
            }
        )
        st.pydeck_chart(r, use_container_width=True)

    # ---- SAĞ KOLON: İNTERAKTİF HABER AKIŞI ----
    with col_feed:
        st.subheader("📰 İstihbarat Akışı")
        
        # Akış panelini kaydırılabilir (scrollable) yapmak için sabit yükseklik veriyoruz
        feed_container = st.container(height=500)
        
        with feed_container:
            for index, row in filtrelenmis_df.iterrows():
                # Risk rengine göre ikon belirleme
                icon = "🔴" if row['severity'] == "critical" else "🟠" if row['severity'] == "high" else "🟡" if row['severity'] == "medium" else "🔵"
                
                # Streamlit expander (genişletilebilir menü) kullanarak şık bir kart tasarımı yapıyoruz
                with st.expander(f"{icon} {row['city']}, {row['country']} - {row['incident_type']}", expanded=False):
                    
                    st.markdown(f"<div class='incident-date'>🗓️ Tarih: {row['date']} | 📍 Hedef: {row['venue_name']}</div>", unsafe_allow_html=True)
                    
                    # Türkçe ve İngilizce özetleri göster
                    if pd.notna(row.get('summary_tr')):
                        st.write(f"**🇹🇷 Özet:** {row['summary_tr']}")
                    if pd.notna(row.get('summary_en')):
                        st.write(f"**🇬🇧 Detay:** {row['summary_en']}")
                    
                    # Kayıplar ve Saldırgan Bilgisi
                    if row.get('casualties_dead', 0) > 0 or row.get('casualties_injured', 0) > 0:
                        st.error(f"💀 Ölü: {row.get('casualties_dead', 0)} | 🚑 Yaralı: {row.get('casualties_injured', 0)}")
                    
                    if pd.notna(row.get('perpetrator')) and row['perpetrator'] != "unknown":
                        st.warning(f"🥷 Şüpheli/Fail: {row['perpetrator']}")
                    
                    # Tag'leri (Etiketleri) yan yana dizme
                    if isinstance(row.get('tags'), list) and len(row['tags']) > 0:
                        tags_html = "".join([f"<span class='incident-tags'>#{tag}</span>" for tag in row['tags']])
                        st.markdown(tags_html, unsafe_allow_html=True)
                        st.write("") # Boşluk
                        
                    # Kaynak URL'lerini tıklanabilir link olarak ekleme
                    if isinstance(row.get('source_urls'), list) and len(row['source_urls']) > 0:
                        st.markdown("**🔗 Kaynaklar:**")
                        for i, url in enumerate(row['source_urls']):
                            st.markdown(f"- [Haber Kaynağına Git {i+1}]({url})")

else:
    st.warning("⚠️ Lütfen haritada veri görebilmek için yan menüden en az bir filtre seçin.")
