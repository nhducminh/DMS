import streamlit as st
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from geopy.distance import geodesic
import matplotlib.pyplot as plt

# Đọc dữ liệu từ file
@st.cache
def load_data():
    df_kh = pd.read_csv("DSKH.csv")  # File danh sách khách hàng
    df_ghetham = pd.read_csv("DSGhetham.csv")  # File danh sách ghé thăm    
    return df_kh, df_ghetham

# Tải dữ liệu
df_kh, df_ghetham = load_data()



# Sidebar filters
st.sidebar.header("Bộ lọc")

# Lọc danh sách Miền
mien_filter = st.sidebar.selectbox("Chọn Miền", df_ghetham["Miền"].unique())

# Lọc danh sách NVTT dựa trên Miền đã chọn
filtered_nvtt = df_ghetham[df_ghetham["Miền"] == mien_filter]
nvtt_filter = st.sidebar.selectbox("Chọn NVTT", filtered_nvtt["MÃ NVTT"].unique())

# Lọc danh sách Ngày dựa trên Miền và NVTT đã chọn
filtered_date = filtered_nvtt[filtered_nvtt["MÃ NVTT"] == nvtt_filter]
date_filter = st.sidebar.date_input("Chọn Ngày", pd.to_datetime(filtered_date["Ngày"]).min())




# Sidebar thông tin
st.sidebar.header("Thông tin")
# Thống kê số lượng C2 ghé thăm theo Miền và NVTT
filtered_by_mien_nvtt = df_ghetham[
    (df_ghetham["Miền"] == mien_filter) &
    (df_ghetham["MÃ NVTT"] == nvtt_filter)
]

c2_stats_mien_nvtt = filtered_by_mien_nvtt.groupby(["MÃ NVTT", "Ngày"]).size().reset_index(name="Số lượng C2 ghé thăm")
c2_stats_mien_nvtt = c2_stats_mien_nvtt.rename(columns={"MÃ NVTT": "Tên NVTT", "Ngày": "Ngày ghé thăm"})

# Thêm dòng tổng
total_row = pd.DataFrame({
    "Tên NVTT": ["Total"],
    "Ngày ghé thăm": [""],
    "Số lượng C2 ghé thăm": [c2_stats_mien_nvtt["Số lượng C2 ghé thăm"].sum()]
})
c2_stats_mien_nvtt = pd.concat([c2_stats_mien_nvtt, total_row], ignore_index=True)

# Hiển thị bảng thống kê theo Miền và NVTT
st.sidebar.write("### Thống kê ghé thăm C2 của NVTT (Lọc theo Miền và NVTT)")
st.sidebar.dataframe(c2_stats_mien_nvtt)
# Lọc dữ liệu theo bộ lọc
filtered_ghetham = df_ghetham[
    (df_ghetham["Miền"] == mien_filter) &
    (df_ghetham["MÃ NVTT"] == nvtt_filter) &
    (pd.to_datetime(df_ghetham["Ngày"]) == pd.to_datetime(date_filter))
]

# Kết hợp dữ liệu ghé thăm với danh sách khách hàng
result = filtered_ghetham.merge(df_kh, left_on="Mã KH", right_on="Mã khách hàng", how="left")
result = result[["Tên KH", "LAT", "LNG", "Thời gian bắt đầu", "Thời gian kết thúc", "Thời gian Ghé thăm","Độ lệch khoảng cách khi ghé thăm"]]
result["LAT"] = pd.to_numeric(result["LAT"], errors="coerce")
result["LNG"] = pd.to_numeric(result["LNG"], errors="coerce")
result["Thời gian bắt đầu"] = pd.to_datetime(result["Thời gian bắt đầu"])
result = result.sort_values(by="Thời gian bắt đầu").reset_index(drop=True)

# Hiển thị dữ liệu đã lọc
st.write("### Dữ liệu ghé thăm đã lọc")
st.dataframe(result)


# Vẽ bản đồ
if not result.empty:
    # Tạo GeoDataFrame
    geometry = [Point(xy) for xy in zip(result["LNG"], result["LAT"])]
    gdf_customers = gpd.GeoDataFrame(result, geometry=geometry, crs="EPSG:4326")
    line = LineString(gdf_customers.geometry.tolist())

    # Vẽ bản đồ
    fig, ax = plt.subplots(figsize=(10, 10))
    vietnam_map = gpd.read_file("map/gadm41_VNM_3.shp")  # Đọc shapefile bản đồ Việt Nam
    vietnam_map.plot(ax=ax, color="lightgrey", edgecolor="black")
    gdf_customers.plot(ax=ax, color="blue", markersize=50, label="Khách hàng")
    gpd.GeoSeries([line], crs="EPSG:4326").plot(ax=ax, color="green", linewidth=1, label="Đường kết nối")

    # Ghi khoảng cách và thời gian di chuyển giữa các điểm
    for i in range(len(gdf_customers) - 1):
        point1 = gdf_customers.geometry.iloc[i]
        point2 = gdf_customers.geometry.iloc[i + 1]
        distance = geodesic((point1.y, point1.x), (point2.y, point2.x)).kilometers
        time1 = gdf_customers["Thời gian bắt đầu"].iloc[i]
        time2 = gdf_customers["Thời gian bắt đầu"].iloc[i + 1]
        travel_time = (time2 - time1).total_seconds() / 60
        mid_x = (point1.x + point2.x) / 2
        mid_y = (point1.y + point2.y) / 2
        ax.text(mid_x, mid_y, f"{distance:.2f} km\n{travel_time:.1f} phút", color="black", fontsize=8, ha="center")

    # Ghi số thứ tự ghé thăm và thời gian bắt đầu tại mỗi điểm
    for idx, row in gdf_customers.iterrows():
        ax.text(
            row.geometry.x,
            row.geometry.y,
            f"{idx + 1}\n{row['Thời gian bắt đầu'].strftime('%H:%M')}\n{row['Độ lệch khoảng cách khi ghé thăm']}",
            color="blue",
            fontsize=8,
            ha="center",
            va="center",
            bbox=dict(facecolor="white", edgecolor="blue", boxstyle="circle")
        )

    # Hiển thị bản đồ
    # Giới hạn bản đồ theo tọa độ
    min_lat, max_lat = gdf_customers['LAT'].min(), gdf_customers['LAT'].max()
    min_lng, max_lng = gdf_customers['LNG'].min(), gdf_customers['LNG'].max()
    ax.set_xlim([min_lng - 0.1, max_lng + 0.1])  # Thêm một chút khoảng trống
    ax.set_ylim([min_lat - 0.1, max_lat + 0.1])

    plt.title(f"Đường di chuyển của {nvtt_filter} ngày {date_filter.strftime('%d/%m/%Y')}", fontsize=16)
    plt.legend()
    st.pyplot(fig)
else:
    st.write("Không có dữ liệu phù hợp với bộ lọc.")
