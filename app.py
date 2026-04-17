import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import json

# --- 1. KOMUTA MERKEZİ AYARLARI ---
st.set_page_config(page_title="Security Intelligence Command", page_icon="🛡️", layout="wide")

# Mobil Uyumlu, Esnek (Fluid) ve Animasyonlu CSS
st.markdown("""
<style>
    /* Ortak Responsive Ayarlar */
    .reportview-container { background: #0d1117; }
    
    /* Mobil Öncelikli KPI Kartları */
    .kpi-card { 
        background-color: #161b22; 
        padding: clamp(10px, 2vw, 20px); 
        border-radius: 10px; 
        border-left: 5px solid #1f6feb; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        margin-bottom: 10px; 
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    
    /* İkonlar ve Metinlerin Ezilmemesi İçin Esnek Boyutlandırma */
    .kpi-title { 
        color: #8bb9e5; 
        font-size: clamp(0.7rem, 1.5vw, 0.9rem); 
        text-transform: uppercase; 
        letter-spacing: 1px; 
        margin-bottom: 5px; 
        white-space: nowrap; 
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .kpi-value { 
        color: #ffffff; 
        font-size: clamp(1.5rem, 5vw, 2.5rem); 
        font-weight: bold; 
    }

    /* 🔥 YENİ: Kritik Durumlar İçin Pulse (Nefes Alan) Animasyonu */
    .pulse-alert {
        border-left: 5px solid #ff4d4d;
        animation: pulse-red 2s infinite;
    }
    @keyframes pulse-red {
        0% { box-shadow: 0 0 0 0 rgba(255, 77, 77, 0.4); }
        70% { box-shadow: 0 0 0 10px rgba(255, 77, 77, 0); }
        100% { box-shadow: 0 0 0 0 rgba(255, 77, 77, 0); }
    }

    /* 🔥 YENİ: Kayan İstihbarat Bandı (Ticker) */
    .ticker-wrapper { 
        width: 100%; overflow: hidden; background-color: #161b22; 
        border-top: 1px solid #30363d; border-bottom: 1px solid #30363d; 
        padding: 8px 0; margin-bottom: 20px; border-radius: 5px;
    }
    .ticker { 
        display: inline-block; white-space: nowrap; padding-right: 100%; 
        box-sizing: content-box; animation: ticker 25s linear infinite; 
    }
    .ticker:hover { animation-play-state: paused; cursor: default; }
    @keyframes ticker { 0% { transform: translate3d(0, 0, 0); } 100% { transform: translate3d(-100%, 0, 0); } }
    .ticker-item { display: inline-block; padding: 0 2rem; font-size: 0.95rem; color: #e6edf3; border-right: 1px solid #30363d;}
    .ticker-alert { color: #ff4d4d; font-weight: bold; }

    /* Mobilde Sekmelerin (Tabs) Yatayda Kaydırılabilir Olması */
    .stTabs [data-baseweb="tab-list"] { 
        gap: 5px; overflow-x: auto; -webkit-overflow-scrolling: touch; padding-bottom: 5px;
    }
    .stTabs [data-baseweb="tab"] { 
        background-color: #161b22; border-radius: 8px 8px 0 0; 
        padding: clamp(8px, 2vw, 15px); color: #8b949e; font-size: clamp(0.8rem, 2vw, 1rem); white-space: nowrap; 
    }
    .stTabs [aria-selected="true"] { background-color: #1f6feb !important; color: white !important; }
    
    .news-card { 
        border-left: 4px solid #1f6feb; padding: clamp(10px, 2vw, 15px); background: #161b22; 
        margin-bottom: 10px; border-radius: 0 8px 8px 0; word-wrap: break-word; 
    }
</style>
""", unsafe_allow_html=True)

# --- 2. AKILLI VERİ YÜKLEME ---
@st.cache_data(ttl=300)
def load_and_clean_data():
    with open("data/incidents.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    df = pd.DataFrame(raw_data["incidents"])
    
    # Koordinat Doğrulama
    df = df.dropna(subset=["geo_lat", "geo_lon"]).copy()
    
    # Tarih dönüşümü
    df['parsed_date'] = pd.to_datetime(df['created_at'], errors='coerce')
    df = df.sort_values(by='parsed_date', ascending=False)
    
    # "unknown" veya "null" ülke sorununu düzeltme
    df['country'] = df['country'].replace(['unknown', '', None], 'Bilinmeyen Ülke (Sistem Hatası)')
    df['country'] = df['country'].str.title() 
    
    # Risk renkleri (WebGL uyumlu)
    color_map = {
        "critical": [255, 0, 0, 210],
        "high": [255, 120, 0, 210],
        "medium": [255, 210, 0, 210],
        "low": [0, 180, 255, 210]
    }
    
    # Fillna yerine lambda ile güvenli renk ataması
    df["color"] = df["severity"].apply(lambda x: color_map.get(str(x).lower(), [150, 150, 150, 150]))
    
    return df, raw_data["metadata"]

try:
    data_df, metadata = load_and_clean_data()
except Exception as e:
    st.error(f"Veri yüklenirken kritik hata oluştu: {e}")
    st.stop()

# --- 3. GELİŞMİŞ SOL PANEL (KONTROL MENÜSÜ) ---
with st.sidebar:
    st.markdown("""
        <div style='background: linear-gradient(45deg, #1f6feb, #4da6ff); padding: 20px; border-radius: 10px; text-align: center; margin-bottom: 20px;'>
            <h2 style='color: white; margin: 0;'>S.I.C.</h2>
            <p style='color: #e6edf3; font-size: 0.8rem; margin: 0;'>Security Intelligence Command</p>
        </div>
    """, unsafe_allow_html=True)

    st.subheader("🔍 Akıllı Filtreleme")
    
    search_query = st.text_input("Anahtar Kelime Ara", placeholder="Havalimanı, saldırı, otel...")

    all_countries = sorted(data_df["country"].unique())
    selected_countries = st.multiselect("🏴 Ülke Seçimi", options=all_countries, default=all_countries)

    selected_types = st.multiselect("🎯 Olay Türü", options=data_df["incident_type"].unique(), default=data_df["incident_type"].unique())
    selected_severity = st.select_slider("⚠️ Minimum Risk", options=["low", "medium", "high", "critical"], value="low")

    st.markdown("---")
    st.caption(f"DB Versiyon: {metadata.get('version')} | Toplam Kayıt: {len(data_df)}")

# --- 4. FİLTRELEME MANTIĞI ---
severity_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
filtered_df = data_df[
    (data_df["country"].isin(selected_countries)) &
    (data_df["incident_type"].isin(selected_types)) &
    (data_df["severity"].map(severity_order) >= severity_order[selected_severity])
]

if search_query:
    filtered_df = filtered_df[
        filtered_df["summary_tr"].str.contains(search_query, case=False, na=False) |
        filtered_df["city"].str.contains(search_query, case=False, na=False)
    ]

# --- 5. ANA PANEL VE DASHBOARD ---
st.title("🛡️ Küresel Havacılık ve Güvenlik Monitörü")

# 🔥 YENİ: KAYAN İSTİHBARAT BANDI (TİCKER)
if not data_df.empty:
    latest_incidents = data_df.head(5) # En son 5 olayı al
    ticker_items = []
    for _, row in latest_incidents.iterrows():
        icon = "🔴" if row['severity'] in ['critical', 'high'] else "🟡"
        alert_class = "ticker-alert" if row['severity'] in ['critical', 'high'] else ""
        ticker_items.append(f"<span class='ticker-item {alert_class}'>{icon} {row['city'].upper()}, {row['country'].upper()}: {row['summary_tr'] or row['summary_en']} ({row['date']})</span>")
    
    ticker_html = f"<div class='ticker-wrapper'><div class='ticker'>{''.join(ticker_items)}</div></div>"
    st.markdown(ticker_html, unsafe_allow_html=True)

if not filtered_df.empty:
    # KPI Şeridi
    k1, k2, k3, k4 = st.columns(4)
    
    # Eğer kritik olay varsa 2. karta 'pulse-alert' animasyonunu ekliyoruz
    critical_count = len(filtered_df[filtered_df['severity'] == 'critical'])
    pulse_class = "pulse-alert" if critical_count > 0 else ""
    
    with k1: st.markdown(f"<div class='kpi-card'><div class='kpi-title'>📌 Aktif Olay</div><div class='kpi-value'>{len(filtered_df)}</div></div>", unsafe_allow_html=True)
    with k2: st.markdown(f"<div class='kpi-card {pulse_class}' style='border-left-color: #ff4d4d;'><div class='kpi-title'>🔴 Kritik Seviye</div><div class='kpi-value'>{critical_count}</div></div>", unsafe_allow_html=True)
    with k3: st.markdown(f"<div class='kpi-card' style='border-left-color: #ffd11a;'><div class='kpi-title'>🏴 Etkilenen Şehir</div><div class='kpi-value'>{filtered_df['city'].nunique()}</div></div>", unsafe_allow_html=True)
    
    # Check if quality_score column exists and isn't entirely null before calculating mean
    if 'quality_score' in filtered_df.columns and not filtered_df['quality_score'].isnull().all():
        quality_mean_str = f"{filtered_df['quality_score'].mean():.2f}"
    else:
        quality_mean_str = "N/A"
        
    with k4: st.markdown(f"<div class='kpi-card' style='border-left-color: #a64dff;'><div class='kpi-title'>✅ Veri Kalitesi (Ort.)</div><div class='kpi-value'>{quality_mean_str}</div></div>", unsafe_allow_html=True)

    st.write("") 
    
    # Sekmeli Yapı
    t_map, t_feed, t_stats = st.tabs(["🌐 İnteraktif Operasyon Haritası", "🗞️ İstihbarat Akışı", "📊 Analitik Raporlar"])

    with t_map:
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=filtered_df,
            get_position="[geo_lon, geo_lat]",
            get_fill_color="color",
            get_line_color=[255, 255, 255, 50],
            get_radius=50000,
            pickable=True,
            opacity=0.8,
            stroked=True,
            filled=True,
            radius_min_pixels=8,
            radius_max_pixels=40,
            line_width_min_pixels=1
        )

        view_state = pdk.ViewState(
            latitude=filtered_df["geo_lat"].mean(),
            longitude=filtered_df["geo_lon"].mean(),
            zoom=2.5,
            pitch=35
        )

        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            map_provider="carto",
            map_style="dark",
            tooltip={
                "html": "<b>{city}, {country}</b><br><b>Tür:</b> {incident_type}<br><b>Özet:</b> {summary_tr}",
                "style": {"backgroundColor": "#0d1117", "color": "white", "border": "1px solid #30363d", "borderRadius": "5px"}
            }
        )
        st.pydeck_chart(r, height=600, use_container_width=True)

    with t_feed:
        st.subheader("📅 Kronolojik Olay Akışı")
        for i, row in filtered_df.iterrows():
            with st.container():
                risk_color = "#ff4d4d" if row['severity'] == "critical" else "#ff8c00" if row['severity'] == "high" else "#ffd700" if row['severity'] == "medium" else "#4da6ff"
                
                st.markdown(f"""
                <div class='news-card' style='border-left-color: {risk_color};'>
                    <h4 style='margin:0;'>{row['city']}, {row['country']} | {row['incident_type']}</h4>
                    <p style='color:#8b949e; font-size:0.8rem; margin-top:3px;'>🗓️ {row['date']} • ⚠️ Risk: <b>{row['severity'].upper()}</b></p>
                    <p style='font-size: 1.05rem;'>{row['summary_tr'] or row['summary_en']}</p>
                </div>
                """, unsafe_allow_html=True)
                
                if row.get('source_urls'):
                    links = " | ".join([f"[{'Kaynak '+str(idx+1)}]({url})" for idx, url in enumerate(row['source_urls'])])
                    st.caption(f"🔗 {links}")
                st.write("---")

    with t_stats:
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("### Türlere Göre Dağılım")
            fig_pie = px.pie(filtered_df, names='incident_type', hole=0.4, template="plotly_dark")
            fig_pie.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_pie, use_container_width=True)
        with col_b:
            st.markdown("### Risk Yoğunluğu")
            color_map_px = {"critical": "#ff4d4d", "high": "#ff8c00", "medium": "#ffd700", "low": "#4da6ff"}
            fig_bar = px.bar(filtered_df, x='severity', color='severity', color_discrete_map=color_map_px, template="plotly_dark")
            fig_bar.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_bar, use_container_width=True)

    # Veri İndirme Alanı
    st.markdown("---")
    st.download_button(
        "📥 Filtrelenmiş Veriyi CSV Olarak İndir",
        data=filtered_df.to_csv(index=False).encode('utf-8'),
        file_name='security_intelligence_report.csv',
        mime='text/csv'
    )

else:
    st.warning("⚠️ Filtrelerinize uygun hiçbir güvenlik olayı bulunamadı. Lütfen sol menüden ayarları esnetin.")
