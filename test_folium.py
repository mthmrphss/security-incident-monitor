import folium
m = folium.Map(location=[45.5236, -122.6750], zoom_start=13)
folium.Marker(
    [45.5236, -122.6750], popup="<i>Mt. Hood Meadows</i>", tooltip="Click me!"
).add_to(m)
print("Folium HTML length:", len(m._repr_html_()))
