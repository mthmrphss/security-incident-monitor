import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import pycountry
import os
import json

# --- 1. KOMUTA MERKEZİ AYARLARI ---
st.set_page_config(page_title="S.I.C. Dashboard", page_icon="🛡️", layout="wide", initial_sidebar_state="expanded")

# Premium UI CSS (Glassmorphism & Modern Styling)
st.markdown("""
<style>
    /* Global Background & Fonts */
    .stApp {
        background-color: #0e1117;
    }
    
    /* Sleek Sidebar */
    [data-testid="stSidebar"] {
        background-color: #161b22;
        border-right: 1px solid #30363d;
    }
    
    /* Logo Box */
    .logo-container {
        background: linear-gradient(135deg, #2e7cf6 0%, #1f4287 100%);
        padding: 25px 20px;
        border-radius: 12px;
        text-align: center;
        margin-bottom: 25px;
        box-shadow: 0 8px 16px rgba(0,0,0,0.4);
        border: 1px solid rgba(255,255,255,0.1);
    }
    .logo-container h2 {
        color: white; margin: 0; font-weight: 800; font-size: 2rem; letter-spacing: 2px;
    }
    .logo-container p {
        color: #a3c2f2; font-size: 0.85rem; margin: 5px 0 0 0; text-transform: uppercase; letter-spacing: 1px;
    }
    
    /* Glassmorphism KPI Cards */
    .kpi-card { 
        background: rgba(22, 27, 34, 0.7);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        padding: 20px; 
        border-radius: 12px; 
        border: 1px solid rgba(255,255,255,0.05);
        border-left: 4px solid #2e7cf6; 
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        margin-bottom: 15px; 
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.3);
    }
    .kpi-title { 
        color: #8b949e; 
        font-size: 0.85rem; 
        text-transform: uppercase; 
        letter-spacing: 1px; 
        margin-bottom: 8px; 
        font-weight: 600;
    }
    .kpi-value { 
        color: #ffffff; 
        font-size: 2.2rem; 
        font-weight: 700; 
        line-height: 1;
    }

    /* Pulse Alert for Critical KPI */
    .pulse-alert {
        border-left-color: #ff4d4d;
        animation: pulse-red 2s infinite;
    }
    @keyframes pulse-red {
        0% { box-shadow: 0 0 0 0 rgba(255, 77, 77, 0.3); }
        70% { box-shadow: 0 0 0 10px rgba(255, 77, 77, 0); }
        100% { box-shadow: 0 0 0 0 rgba(255, 77, 77, 0); }
    }

    /* Modern Ticker */
    .ticker-wrapper { 
        width: 100%; overflow: hidden; 
        background: rgba(22, 27, 34, 0.8);
        border: 1px solid rgba(255,255,255,0.05);
        padding: 10px 0; margin-bottom: 25px; border-radius: 8px;
    }
    .ticker { 
        display: inline-block; white-space: nowrap; padding-right: 100%; 
        box-sizing: content-box; animation: ticker 30s linear infinite; 
    }
    .ticker:hover { animation-play-state: paused; cursor: default; }
    @keyframes ticker { 0% { transform: translate3d(0, 0, 0); } 100% { transform: translate3d(-100%, 0, 0); } }
    .ticker-item { display: inline-block; padding: 0 2rem; font-size: 0.95rem; color: #c9d1d9; border-right: 1px solid #30363d;}
    .ticker-alert { color: #ff6b6b; font-weight: 600; }

    /* News Feed Cards */
    .news-card { 
        background: #161b22; 
        border: 1px solid #30363d;
        border-left: 4px solid #2e7cf6; 
        padding: 18px; 
        margin-bottom: 15px; 
        border-radius: 8px; 
        transition: background-color 0.2s;
    }
    .news-card:hover {
        background: #1c2128;
    }
    .news-header {
        display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;
    }
    .news-title { margin:0; font-size: 1.1rem; font-weight: 600; color: #e6edf3; }
    .news-meta { color:#8b949e; font-size:0.85rem; }
    .news-summary { font-size: 1rem; color: #c9d1d9; line-height: 1.5; margin-top: 10px; }
    
    /* Custom Tabs */
    .stTabs [data-baseweb="tab-list"] { 
        gap: 8px; border-bottom: 1px solid #30363d; padding-bottom: 0px;
    }
    .stTabs [data-baseweb="tab"] { 
        background-color: transparent; border: none;
        padding: 12px 20px; color: #8b949e; font-size: 1rem; font-weight: 500;
        border-bottom: 2px solid transparent; border-radius: 0;
    }
    .stTabs [aria-selected="true"] { 
        background-color: transparent !important; 
        color: #2e7cf6 !important; 
        border-bottom: 2px solid #2e7cf6 !important; 
    }
</style>
""", unsafe_allow_html=True)

# --- 2. AKILLI VERİ YÜKLEME VE TEMİZLEME ---
@st.cache_data
def load_and_clean_data(mtime):
    with open("data/incidents.json", "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    df = pd.DataFrame(raw_data["incidents"])
    
    # 1. TARİH NORMALİZASYONU (Sıralama İçin Önce Yapılmalı)
    def normalize_date(row):
        """En güvenilir kaynaktan YYYY-MM-DD tarihi çıkar."""
        for field in ['real_publish_date', 'publish_date', 'created_at']:
            raw = row.get(field)
            if pd.isna(raw) or not raw or str(raw).lower() in ('unknown', 'null', ''):
                continue
            parsed = pd.to_datetime(str(raw), errors='coerce')
            if pd.notna(parsed):
                return parsed.strftime('%Y-%m-%d')
        return '1970-01-01' # Fallback for sorting
    
    def normalize_event_date(val):
        if pd.isna(val) or not val or str(val).lower() in ('unknown', 'null', ''):
            return None
        parsed = pd.to_datetime(str(val), errors='coerce')
        if pd.notna(parsed):
            return parsed.strftime('%Y-%m-%d')
        return None

    df['display_date'] = df.apply(normalize_date, axis=1)
    df['event_date_display'] = df['event_date'].apply(normalize_event_date)
    
    # KRONOLOJİK SIRALAMA DÜZELTMESİ (created_at yerine display_date)
    df['parsed_date'] = pd.to_datetime(df['display_date'], errors='coerce')
    df = df.sort_values(by='parsed_date', ascending=False)
    
    # 2. "UNKNOWN" ŞEHİR/ÜLKE YÖNETİMİ
    def get_country_name(code):
        if pd.isna(code) or str(code).lower() in ('unknown', '', 'null', 'none'):
            return None
        country = pycountry.countries.get(alpha_2=str(code).upper())
        return country.name if country else None

    # Country Code'dan isim kurtarma
    df['country'] = df['country'].replace(['unknown', '', None, 'null'], None)
    if 'country_code' in df.columns:
        mask = df['country'].isna()
        df.loc[mask, 'country'] = df.loc[mask, 'country_code'].apply(get_country_name)
        
    # Kalan Unknown'ları Türkçeleştir
    df['country'] = df['country'].fillna('Bilinmeyen Ülke')
    df['city'] = df['city'].replace(['unknown', '', None, 'null'], 'Bilinmeyen Şehir')
    df['country'] = df['country'].str.title() 
    df['city'] = df['city'].str.title()
    
    # 3. HARİTA İÇİN GEÇERLİ KOORDİNAT BAYRAĞI
    # Unknown olanları feed'de tut, haritada gösterme
    def is_valid_location(row):
        has_coords = pd.notna(row.get('geo_lat')) and pd.notna(row.get('geo_lon'))
        is_unknown = (row.get('country') == 'Bilinmeyen Ülke') and (row.get('city') == 'Bilinmeyen Şehir')
        return has_coords and not is_unknown

    df['has_valid_location'] = df.apply(is_valid_location, axis=1)

    # 4. RİSK RENKLERİ VE HARİTA YARIÇAPI
    color_map = {
        "critical": [255, 77, 77, 200],   # Red
        "high": [255, 140, 0, 200],       # Dark Orange
        "medium": [255, 215, 0, 200],     # Gold
        "low": [77, 166, 255, 200]        # Light Blue
    }
    radius_map = {
        "critical": 60000,
        "high": 40000,
        "medium": 25000,
        "low": 15000
    }
    
    df["color"] = df["severity"].apply(lambda x: color_map.get(str(x).lower(), [150, 150, 150, 150]))
    df["radius"] = df["severity"].apply(lambda x: radius_map.get(str(x).lower(), 15000))
    
    return df, raw_data["metadata"]

try:
    mtime = os.path.getmtime("data/incidents.json")
    data_df, metadata = load_and_clean_data(mtime)
except Exception as e:
    st.error(f"Veri yüklenirken kritik hata oluştu: {e}")
    st.stop()

# --- 3. GELİŞMİŞ SOL PANEL (KONTROL MENÜSÜ) ---
with st.sidebar:
    st.markdown("""
        <div class='logo-container'>
            <h2>S.I.C.</h2>
            <p>Security Intelligence Command</p>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("<h4 style='color: #c9d1d9; margin-bottom: 15px;'>🔍 Akıllı Filtreleme</h4>", unsafe_allow_html=True)
    
    search_query = st.text_input("Anahtar Kelime Ara", placeholder="Havalimanı, saldırı, drone...")

    # Filtrelerde Bilinmeyen Ülke en sona atılsın
    all_countries = sorted([c for c in data_df["country"].unique() if c != 'Bilinmeyen Ülke'])
    if 'Bilinmeyen Ülke' in data_df["country"].unique():
        all_countries.append('Bilinmeyen Ülke')
        
    selected_countries = st.multiselect("🏴 Ülke Seçimi", options=all_countries, default=all_countries)

    selected_types = st.multiselect("🎯 Olay Türü", options=data_df["incident_type"].unique(), default=data_df["incident_type"].unique())
    selected_severity = st.select_slider("⚠️ Minimum Risk", options=["low", "medium", "high", "critical"], value="low")

    st.markdown("<br><hr style='border-color: #30363d;'>", unsafe_allow_html=True)
    st.caption(f"DB Versiyon: {metadata.get('version')} | Toplam Olay: {len(data_df)}")

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
        filtered_df["city"].str.contains(search_query, case=False, na=False) |
        filtered_df["summary_en"].str.contains(search_query, case=False, na=False)
    ]

# --- 5. ANA PANEL VE DASHBOARD ---
st.title("Küresel Havacılık ve Güvenlik Monitörü")

# KAYAN İSTİHBARAT BANDI (TİCKER)
if not data_df.empty:
    latest_incidents = data_df.head(6)
    ticker_items = []
    for _, row in latest_incidents.iterrows():
        icon = "🔴" if row['severity'] in ['critical', 'high'] else "🟡"
        alert_class = "ticker-alert" if row['severity'] in ['critical', 'high'] else ""
        text = f"{row['city'].upper()}, {row['country'].upper()}: {row['summary_tr'] or row['summary_en']} ({row['display_date']})"
        ticker_items.append(f"<span class='ticker-item {alert_class}'>{icon} {text}</span>")
    
    ticker_html = f"<div class='ticker-wrapper'><div class='ticker'>{''.join(ticker_items)}</div></div>"
    st.markdown(ticker_html, unsafe_allow_html=True)

if not filtered_df.empty:
    # KPI Şeridi
    k1, k2, k3, k4 = st.columns(4)
    
    critical_count = len(filtered_df[filtered_df['severity'] == 'critical'])
    pulse_class = "pulse-alert" if critical_count > 0 else ""
    
    with k1: st.markdown(f"<div class='kpi-card'><div class='kpi-title'>📌 Filtrelenen Olay</div><div class='kpi-value'>{len(filtered_df)}</div></div>", unsafe_allow_html=True)
    with k2: st.markdown(f"<div class='kpi-card {pulse_class}' style='border-left-color: #ff4d4d;'><div class='kpi-title'>🔴 Kritik Risk</div><div class='kpi-value'>{critical_count}</div></div>", unsafe_allow_html=True)
    
    valid_cities = filtered_df[filtered_df['city'] != 'Bilinmeyen Şehir']['city'].nunique()
    with k3: st.markdown(f"<div class='kpi-card' style='border-left-color: #ffd700;'><div class='kpi-title'>🏴 Etkilenen Şehir</div><div class='kpi-value'>{valid_cities}</div></div>", unsafe_allow_html=True)
    
    if 'quality_score' in filtered_df.columns and not filtered_df['quality_score'].isnull().all():
        quality_mean = f"{filtered_df['quality_score'].mean():.2f}"
    else:
        quality_mean = "N/A"
    with k4: st.markdown(f"<div class='kpi-card' style='border-left-color: #a64dff;'><div class='kpi-title'>✅ Güvenilirlik Skoru</div><div class='kpi-value'>{quality_mean}</div></div>", unsafe_allow_html=True)

    st.write("") 
    
    # --- YENİ HARİTA YERLEŞİMİ (TAM EKRAN VE EN ÜSTTE) ---
    # Harita için lokasyonu geçerli olanları ayır (Unknown'ları gizle)
    map_df_raw = filtered_df[filtered_df['has_valid_location'] == True]
    
    if not map_df_raw.empty:
        # AKILLI SICAK NOKTA (SMART HOTSPOT) MİMARİSİ
        def get_max_severity(sev_list):
            if 'critical' in sev_list: return 'critical'
            if 'high' in sev_list: return 'high'
            if 'medium' in sev_list: return 'medium'
            return 'low'
        
        def format_tooltip(row):
            header = f"<h4 style='margin:0 0 5px 0; color:#2e7cf6;'>{row['city']}, {row['country']}</h4>"
            count_info = f"<div style='margin-bottom:8px; font-weight:bold;'>Toplam Olay: {row['incident_count']}</div>"
            
            summaries = row['summary_list']
            dates = row['date_list']
            types = row['type_list']
            sevs = row['sev_list']
            
            list_html = "<ul style='margin:0; padding-left:15px; font-size:0.9rem;'>"
            display_count = min(5, len(summaries))
            for i in range(display_count):
                color = "#ff4d4d" if sevs[i] == 'critical' else "#ff8c00" if sevs[i] == 'high' else "#ffd700" if sevs[i] == 'medium' else "#c9d1d9"
                list_html += f"<li style='margin-bottom:4px;'><span style='color:{color};'>[{sevs[i].upper()}]</span> {dates[i]} - {types[i]}: {str(summaries[i])[:60]}...</li>"
            list_html += "</ul>"
            
            extra = f"<div style='margin-top:8px; font-style:italic; color:#8b949e;'>ve +{len(summaries) - 5} olay daha...</div>" if len(summaries) > 5 else ""
            
            return f"<div style='font-family: sans-serif;'>{header}{count_info}{list_html}{extra}</div>"

        # DataFrame'i Aggregate Et
        agg_funcs = {
            'id': 'count',
            'severity': list,
            'summary_tr': list,
            'display_date': list,
            'incident_type': list
        }
        
        map_df = map_df_raw.groupby(['geo_lat', 'geo_lon', 'city', 'country']).agg(agg_funcs).reset_index()
        map_df.rename(columns={
            'id': 'incident_count', 
            'severity': 'sev_list',
            'summary_tr': 'summary_list',
            'display_date': 'date_list',
            'incident_type': 'type_list'
        }, inplace=True)
        
        # Renk ve Boyut ataması
        color_map = {
            "critical": [255, 77, 77, 230],   
            "high": [255, 140, 0, 230],       
            "medium": [255, 215, 0, 230],     
            "low": [77, 166, 255, 230]        
        }
        
        map_df['max_severity'] = map_df['sev_list'].apply(get_max_severity)
        map_df['color'] = map_df['max_severity'].apply(lambda x: color_map.get(x, [150, 150, 150, 200]))
        
        # Boyut formülü
        base_radius_map = {"critical": 80000, "high": 50000, "medium": 30000, "low": 20000}
        map_df['base_radius'] = map_df['max_severity'].apply(lambda x: base_radius_map.get(x, 20000))
        map_df['radius'] = map_df['base_radius'] + (map_df['incident_count'] * 8000)
        
        map_df['tooltip_html'] = map_df.apply(format_tooltip, axis=1)

        # BULANIKLIĞI VE DAĞILMAYI ÖNLEYEN KESKİN KATMAN AYARLARI
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position="[geo_lon, geo_lat]",
            get_fill_color="color",
            get_line_color=[255, 255, 255, 220], # Daha belirgin beyaz sınır
            get_radius="radius", 
            radius_min_pixels=6,   # Uzaklaştırıldığında çok küçülüp kaybolmasını/dağılmasını engeller
            radius_max_pixels=35,  # Yakınlaştırıldığında çok büyüyüp bulanıklaşmasını engeller
            pickable=True,
            opacity=0.9,
            stroked=True,
            filled=True,
            line_width_min_pixels=2 # Sınır çizgisini keskin tutar
        )

        view_state = pdk.ViewState(
            latitude=map_df["geo_lat"].mean(),
            longitude=map_df["geo_lon"].mean(),
            zoom=2.2,
            pitch=35
        )

        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            map_provider="carto",
            map_style="dark",
            tooltip={
                "html": "{tooltip_html}",
                "style": {"backgroundColor": "rgba(13, 17, 23, 0.95)", "color": "white", "border": "1px solid #30363d", "borderRadius": "8px", "padding": "15px", "maxWidth": "400px", "boxShadow": "0 4px 12px rgba(0,0,0,0.5)"}
            }
        )
        st.markdown("<h3 style='color:#e6edf3; margin-top:10px; margin-bottom:-10px;'>🌐 Küresel Operasyon Haritası</h3>", unsafe_allow_html=True)
        st.pydeck_chart(r, height=650, use_container_width=True)
    else:
        st.info("🗺️ Seçili filtrelere uygun, geçerli koordinata sahip olay bulunamadı.")

    st.write("<br>", unsafe_allow_html=True)
    
    # --- ALT SEKMELER (SADECE AKIŞ VE İSTATİSTİK) ---
    t_feed, t_stats = st.tabs(["🗞️ İstihbarat Akışı", "📊 Analitik Raporlar"])

    with t_feed:
        st.write("<br>", unsafe_allow_html=True)
        for i, row in filtered_df.iterrows():
            with st.container():
                risk_color = "#ff4d4d" if row['severity'] == "critical" else "#ff8c00" if row['severity'] == "high" else "#ffd700" if row['severity'] == "medium" else "#2e7cf6"
                
                event_info = f" (Olay Tarihi: {row['event_date_display']})" if row.get('event_date_display') and row['event_date_display'] != row['display_date'] else ""
                
                st.markdown(f"""
                <div class='news-card' style='border-left-color: {risk_color};'>
                    <div class='news-header'>
                        <div class='news-title'>{row['city']}, {row['country']} | {row['incident_type']}</div>
                        <span style='background: {risk_color}33; color: {risk_color}; padding: 3px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;'>{row['severity'].upper()}</span>
                    </div>
                    <div class='news-meta'>🗓️ Görüntülenme: {row['display_date']}{event_info} • 📌 {row['venue_name'] or 'Genel'}</div>
                    <div class='news-summary'>{row['summary_tr'] or row['summary_en']}</div>
                </div>
                """, unsafe_allow_html=True)
                
                if row.get('source_urls'):
                    links = " | ".join([f"[{'Kaynak '+str(idx+1)}]({url})" for idx, url in enumerate(row['source_urls'])])
                    st.caption(f"🔗 {links}")
                else:
                    st.write("")

    with t_stats:
        st.write("<br>", unsafe_allow_html=True)
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("<h4 style='color:#c9d1d9;'>🎯 Türlere Göre Dağılım</h4>", unsafe_allow_html=True)
            fig_pie = px.pie(filtered_df, names='incident_type', hole=0.5, template="plotly_dark", 
                             color_discrete_sequence=px.colors.sequential.Blues_r)
            fig_pie.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", margin=dict(t=20, b=20, l=0, r=0))
            st.plotly_chart(fig_pie, use_container_width=True)
        with col_b:
            st.markdown("<h4 style='color:#c9d1d9;'>⚠️ Risk Yoğunluğu</h4>", unsafe_allow_html=True)
            color_map_px = {"critical": "#ff4d4d", "high": "#ff8c00", "medium": "#ffd700", "low": "#4da6ff"}
            fig_bar = px.bar(filtered_df, x='severity', color='severity', color_discrete_map=color_map_px, template="plotly_dark")
            fig_bar.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", showlegend=False, margin=dict(t=20, b=20, l=0, r=0))
            st.plotly_chart(fig_bar, use_container_width=True)

    # Veri İndirme Alanı
    st.markdown("<hr style='border-color: #30363d; margin: 30px 0;'>", unsafe_allow_html=True)
    st.download_button(
        "📥 Filtrelenmiş Veriyi CSV Olarak İndir",
        data=filtered_df.to_csv(index=False).encode('utf-8'),
        file_name='security_intelligence_report.csv',
        mime='text/csv',
        type="primary"
    )

else:
    st.warning("⚠️ Filtrelerinize uygun hiçbir güvenlik olayı bulunamadı. Lütfen sol menüden ayarları esnetin.")
