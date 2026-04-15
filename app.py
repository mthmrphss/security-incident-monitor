import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import json

# --- 1. SAYFA VE ARAYÜZ AYARLARI ---
st.set_page_config(page_title="Global Security Monitor", page_icon="🛡️", layout="wide")

# Daha temiz bir UI için Custom CSS
st.markdown("""
<style>
    .kpi-card { background-color: #1e1e2f; padding: 20px; border-radius: 10px; border-left: 5px solid #4da6ff; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
    .kpi-title { color: #8bb9e5; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 5px; }
    .kpi-value { color: #ffffff; font-size: 2rem; font-weight: bold; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: transparent; border-radius: 4px 4px 0px 0px; gap: 1px; padding-top: 10px; padding-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

# --- 2. VERİ YÜKLEME VE İŞLEME ---
@st.cache_data
def load_data():
    with open("data/incidents.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    df = pd.DataFrame(raw_data["incidents"])
    map_df = df.dropna(subset=["geo_lat", "geo_lon"]).copy()
    map_df['parsed_date'] = pd.to_datetime(map_df['created_at'], errors='coerce')
    map_df = map_df.sort_values(by='parsed_date', ascending=False)
    
    def get_color(severity):
        if severity == "critical": return [255, 50, 50, 200]
        elif severity == "high": return [255, 140, 0, 200]
        elif severity == "medium": return [255, 215, 0, 200]
        else: return [50, 150, 255, 200]
        
    map_df["color"] = map_df["severity"].apply(get_color)
    
    # Arama motoru için birleştirilmiş bir metin sütunu (küçük harfe çevrilmiş)
    map_df["search_text"] = (map_df["country"].fillna("") + " " + 
                             map_df["city"].fillna("") + " " + 
                             map_df["summary_tr"].fillna("") + " " + 
                             map_df["summary_en"].fillna("")).str.lower()
    return map_df, df, raw_data["metadata"]

map_df, raw_df, metadata = load_data()

# --- 3. YAN MENÜ (KONTROL PANELİ) ---
with st.sidebar:
    st.markdown("<h1 style='text-align: center; color: #4da6ff;'>🛡️ Global Security Monitor.</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: gray; font-size: 0.8rem;'>Global Security Monitor v2.0</p>", unsafe_allow_html=True)
    st.markdown("---")

    # Yeni: Metin Arama
    arama_metni = st.text_input("🔍 Serbest Arama", placeholder="Ülke, şehir, veya anahtar kelime...")
    
    secilen_tur = st.multiselect("🎯 Olay Türü", options=map_df["incident_type"].unique(), default=map_df["incident_type"].unique())
    secilen_risk = st.multiselect("⚠️ Risk Seviyesi", options=map_df["severity"].unique(), default=map_df["severity"].unique())

    st.markdown("---")
    
    # Yeni: CSV Dışa Aktarma Butonu
    @st.cache_data
    def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')

# --- 4. FİLTRELEME MANTIĞI ---
# Arama metni varsa ona göre de filtrele
if arama_metni:
    filtrelenmis_df = map_df[
        (map_df["incident_type"].isin(secilen_tur)) & 
        (map_df["severity"].isin(secilen_risk)) &
        (map_df["search_text"].str.contains(arama_metni.lower()))
    ]
else:
    filtrelenmis_df = map_df[(map_df["incident_type"].isin(secilen_tur)) & (map_df["severity"].isin(secilen_risk))]

# --- 5. ANA EKRAN: KPI METRİKLERİ ---
st.title("🌐 Security Incident Monitor")

if not filtrelenmis_df.empty:
    # Özel CSS ile daha şık KPI kartları
    k1, k2, k3, k4 = st.columns(4)
    with k1: st.markdown(f"<div class='kpi-card'><div class='kpi-title'>Filtrelenen Vaka</div><div class='kpi-value'>{len(filtrelenmis_df)}</div></div>", unsafe_allow_html=True)
    with k2: st.markdown(f"<div class='kpi-card' style='border-left-color: #ff4d4d;'><div class='kpi-title'>Kritik & Yüksek</div><div class='kpi-value'>{len(filtrelenmis_df[filtrelenmis_df['severity'].isin(['critical', 'high'])])}</div></div>", unsafe_allow_html=True)
    with k3: st.markdown(f"<div class='kpi-card' style='border-left-color: #ffd11a;'><div class='kpi-title'>Etkilenen Ülke</div><div class='kpi-value'>{filtrelenmis_df['country'].nunique()}</div></div>", unsafe_allow_html=True)
    with k4: st.markdown(f"<div class='kpi-card' style='border-left-color: #a64dff;'><div class='kpi-title'>Can Kaybı</div><div class='kpi-value'>{int(filtrelenmis_df['casualties_dead'].sum())}</div></div>", unsafe_allow_html=True)
    
    st.write("") # Boşluk

    # --- 6. SEKME (TAB) MİMARİSİ ---
    tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Operasyon Haritası", "📰 Genişletilmiş Haber Akışı", "📈 Olay Trendleri", "🗄️ Veri Dışa Aktar"])

    # SEKME 1: DEVASA HARİTA
    with tab1:
        layer = pdk.Layer(
            'ScatterplotLayer',
            data=filtrelenmis_df,
            get_position='[geo_lon, geo_lat]', 
            get_fill_color='color',
            get_line_color=[255, 255, 255, 80],
            get_radius=50000,                  
            pickable=True, opacity=0.8, stroked=True, filled=True, radius_scale=1, radius_min_pixels=8, radius_max_pixels=30, line_width_min_pixels=1,
        )
        view_state = pdk.ViewState(latitude=39.0, longitude=35.0, zoom=2.5, pitch=25)
        r = pdk.Deck(
            layers=[layer], initial_view_state=view_state, map_provider="carto", map_style="dark",
            tooltip={"html": "<b>{city}, {country}</b><br/><b>Tür:</b> {incident_type} | <b>Risk:</b> {severity}<br/><b>Özet:</b> {summary_tr}", "style": {"backgroundColor": "#1E1E1E", "color": "white", "borderRadius": "8px", "padding": "10px"}}
        )
        # Haritaya 600px yükseklik verdik, sekmenin içini tam dolduracak
        st.pydeck_chart(r, use_container_width=True)

    # SEKME 2: DAHA TEMİZ VE OKUNAKLI HABER AKIŞI
    with tab2:
        # Haberleri yan yana 2 kolon halinde dizelim ki çok aşağı inmek gerekmesin
        cols = st.columns(2)
        for index, row in enumerate(filtrelenmis_df.itertuples()):
            col = cols[index % 2] # Sağ-Sol kolon dağıtımı
            icon = "🔴" if row.severity == "critical" else "🟠" if row.severity == "high" else "🟡" if row.severity == "medium" else "🔵"
            
            with col.expander(f"{icon} {row.date} | {row.city}, {row.country}", expanded=True):
                if pd.notna(row.summary_tr): st.markdown(f"**Özet:** {row.summary_tr}")
                
                # Etiketler (Tags)
                if isinstance(row.tags, list):
                    st.caption(" • ".join([f"#{t}" for t in row.tags]))
                
                # Linkleri yan yana şık butonlar gibi dizme
                if isinstance(row.source_urls, list) and len(row.source_urls) > 0:
                    links = " | ".join([f"[Kaynak {i+1}]({url})" for i, url in enumerate(row.source_urls)])
                    st.markdown(f"🔗 {links}")

    # SEKME 3: YENİ TREND ANALİZİ GRAFİĞİ
    with tab3:
        st.markdown("### Zaman İçindeki Vaka Dağılımı")
        # Plotly ile etkileşimli çubuk grafik (Tarihe ve Riske göre)
        trend_df = filtrelenmis_df.groupby([filtrelenmis_df['parsed_date'].dt.date, 'severity']).size().reset_index(name='count')
        
        # Risk renklerini haritayla uyumlu yapalım
        color_map = {"critical": "#ff3333", "high": "#ff8c00", "medium": "#ffd700", "low": "#3399ff"}
        
        if not trend_df.empty:
            fig = px.bar(trend_df, x="parsed_date", y="count", color="severity", 
                         color_discrete_map=color_map,
                         labels={"parsed_date": "Tarih", "count": "Olay Sayısı", "severity": "Risk Seviyesi"},
                         title="Günlük İstihbarat Raporu Yoğunluğu")
            
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Bu tarih aralığı için trend grafiği oluşturulacak yeterli veri yok.")

    # SEKME 4: VERİ TABANI VE İNDİRME
    with tab4:
        st.markdown("### Ham Veri İzleme ve Dışa Aktarma")
        st.dataframe(filtrelenmis_df[["date", "incident_type", "severity", "country", "city", "summary_tr"]], use_container_width=True)
        
        csv = convert_df(filtrelenmis_df)
        st.download_button(
            label="📥 Filtrelenmiş Veriyi CSV Olarak İndir",
            data=csv,
            file_name='security_incidents_export.csv',
            mime='text/csv',
        )

else:
    st.error("⚠️ Filtrelerinize veya arama teriminize uygun hiçbir güvenlik olayı bulunamadı. Lütfen filtreleri esnetin.")
