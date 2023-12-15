# %%
import datetime as dt
import os
import shutil

import numpy as np
import openpyxl
import pandas as pd
import xlsxwriter
import xlwings as xw
import sys

print(sys.getdefaultencoding())

now = dt.datetime.now()
print(os.popen("taskkill /f /t /im EXCEL.exe").read())
print(os.popen("Taskkill /f /im onedrive.exe").read())

pd.options.display.float_format = "{:,.2f}".format


# %%
def DMStoExcel(dfExport, file_name, sheet_name, index):
    # exporting pandas DataFrames to Excel files.
    # Check if the Excel file already exists
    if os.path.exists(file_name):
        try:
            # If the file exists, open it in 'append' mode and replace the sheet if it already exists
            with pd.ExcelWriter(file_name, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                # Export the DataFrame to the specified sheet in the Excel file
                dfExport.to_excel(writer, sheet_name=sheet_name, index=index)

        except Exception as error:
            # If there's an error during the export, print the error message and continue
            print(error)
            pass
    else:
        # If the Excel file doesn't exist, create it
        with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
            # Export the DataFrame to the specified sheet in the Excel file
            dfExport.to_excel(writer, sheet_name=sheet_name, index=index)


# %%
def DMStoExcelRange(Worksheet, dfExport, range):
    Worksheet.range(range).options(index=False).value = dfExport
    range = Worksheet.range(range).expand()
    range.api.Borders.Weight = 2
    range.autofit()


# %%
# Get reported date
nowdate = now.date()
dateReport = pd.read_csv("DateReport.csv", dtype=str)
fromDate = dateReport.FromDate[0]
toDate = dateReport.ToDate[0]
reportFolder = str(fromDate) + "-" + str(toDate)
print(reportFolder)

# %%
# reportFolder = "26082023-01092023"
reportFolder = os.path.abspath(reportFolder)
print(os.listdir(reportFolder))
try:
    os.mkdir(reportFolder + "\\GSBH")
    print("done")
except Exception as error:
    # If there's an error during the export, print the error message and continue
    print(error)
    pass

try:
    os.mkdir(reportFolder + "\\NVTT")
    print("done")
except Exception as error:
    # If there's an error during the export, print the error message and continue
    print(error)
    pass


# %%


# %%
GSBH_CTVM = pd.read_csv("GSBH_CTVM.csv")
print(GSBH_CTVM)


# %%
import pandas as pd
import numpy as np
import xlwings as xw


def BCGheThamC2(dfExport, file_name, sheet_name, reportFolder):
    dfExport.loc[:, ["Tỉnh/thành phố"]].dropna()
    dfExport.loc[:, ["MÃ NVTT"]].dropna()
    dfExport.loc[:, ["Ngày"]].dropna()
    dfExport = dfExport.reset_index(drop=True)
    dfExport.index = dfExport.index + 1
    DMStoExcel(dfExport, file_name, sheet_name, False)

    noLoaiKH = dfExport["Loại KH"].drop_duplicates().count()
    LoaiKH = dfExport["Loại KH"].drop_duplicates()

    dfExport = dfExport.loc[:, ["Miền", "MÃ NVTT", "TÊN NVTT", "Mã KH", "Tỉnh/thành phố", "Ngày", "Loại KH"]]

    dfExport_pivot = pd.pivot_table(
        dfExport,
        values="Mã KH",
        index=["MÃ NVTT", "TÊN NVTT", "Tỉnh/thành phố", "Ngày"],
        columns=["Loại KH"],
        aggfunc="count",
        fill_value=0,
    )
    dfExport_pivot = dfExport_pivot.reset_index()
    dfExport_pivot["Tổng cộng"] = dfExport_pivot.apply(lambda x: np.sum([x[lkh] for lkh in LoaiKH]), axis=1)
    try:
        dfExport_group = dfExport_pivot.groupby("TÊN NVTT").sum(numeric_only=True).reset_index()
        dfExport_group.loc[:, "MÃ NVTT"] = "zTổng cộng"
    except Exception as error:
        print(error)
        pass
    Total = []
    for i in range(1, noLoaiKH + 2):
        Total.append(int(dfExport_pivot.iloc[0:, [-i]].sum().iloc[0]))

    dfExport_pivot = (
        pd.concat([dfExport_pivot, dfExport_group])
        .sort_values(by=["TÊN NVTT", "MÃ NVTT"], ascending=True)
        .reset_index(drop=True)
    )

    filter = dfExport_pivot["MÃ NVTT"] == "zTổng cộng"
    dfExport_pivot.loc[filter, "MÃ NVTT"] = "Tổng cộng     "
    dfExport_pivot.index = dfExport_pivot.index + 1
    dfExport_pivot = dfExport_pivot.reset_index()
    dfExport_pivot = dfExport_pivot.rename(columns={"index": "STT"})

    app = xw.App(visible=False)
    Workbook = xw.Book(file_name)
    try:
        Workbook.sheets.add(sheet_name)
    except:
        pass

    try:
        Worksheet = Workbook.sheets[sheet_name]

        Worksheet.clear_contents()

        Worksheet.range("A1").value = "Báo cáo ghé thăm C2 trong tuần"
        Worksheet.range("A1:D1").merge()
        Worksheet.range("A1:D1").autofit()

        Worksheet.range("A2").value = dfExport.loc[:, ["Miền"]].drop_duplicates()
        Worksheet.range("A2:D2").merge()
        Worksheet.range("A2:D2").autofit()

        Worksheet.range("A3").value = (
            "Từ ngày "
            + fromDate[0:2]
            + "/"
            + fromDate[2:4]
            + "/"
            + fromDate[4:]
            + " đến hết ngày "
            + toDate[0:2]
            + "/"
            + toDate[2:4]
            + "/"
            + toDate[4:]
        )
        Worksheet.range("A3:D3").merge()
        Worksheet.range("A3:D3").autofit()

        Worksheet.range("A5").options(index=False).value = dfExport_pivot
        Worksheet.range("A5:Z5").font.bold = True

        rng = Worksheet.range("A5").expand()
        for i in range(1, noLoaiKH + 2):
            Worksheet.range(rng.last_cell.row + 1, rng.last_cell.column - i + 1).value = Total[i - 1]

        Worksheet.range(rng.last_cell.row + 1, 1).value = "Số KH ghé thăm:"
        Worksheet.range((rng.last_cell.row + 1, 1), (rng.last_cell.row + 1, 30)).font.bold = True

        rng = Worksheet.range("A5").expand()
        rng.api.Borders.Weight = 2
        rng.autofit()

        for i in rng.rows:
            for j in i:
                if Worksheet.range(j).value == "Tổng cộng     ":
                    i.font.bold = True

        Workbook.save(file_name)
        Workbook.close()
        print("Saved file ", file_name)

    except:
        print("error Saved file ", file_name)
        pass

    print(f"BCGheThamC2 {file_name} => {sheet_name}")


# %%
def BCDonHangC2(dfExport, file_name, sheet_name, reportFolder):
    # Export summary report to Excel
    dfExport = dfExport.loc[
        :,
        [
            "Miền",
            "Mã NVTT",
            "Tên NVTT",
            "Mã KH",
            "Tên KH",
            "Số đơn hàng",
            "Mã SP",
            "Tên Sản phẩm",
            "Số lượng (tấn)",
            "Thành tiền (VND)",
        ],
    ]
    dfExport_pivot = pd.pivot_table(
        dfExport,
        values=["Số lượng (tấn)", "Thành tiền (VND)"],
        index=[
            "Miền",
            "Mã NVTT",
            "Tên NVTT",
            "Mã KH",
            "Tên KH",
            "Số đơn hàng",
            "Mã SP",
            "Tên Sản phẩm",
        ],
        aggfunc="sum",
        fill_value=0,
    )
    dfExport_pivot = dfExport_pivot.reset_index()
    dfExport_group = dfExport_pivot.groupby(["Tên NVTT"]).sum(numeric_only=True).reset_index()

    Total = []
    for i in range(1, 3):
        Total.append("{:,.2f}".format(int(dfExport_pivot.iloc[0:, [-i]].sum().iloc[0])))
        # Total.append("{:,.2f}".format(int(dfExport_pivot.iloc[0:, [-i]].sum())))

    try:
        dfExport_group.loc[:, "Miền"] = "zTổng cộng"
    except Exception as error:
        # If there's an error during the export, print the error message and continue
        print(error)
        pass
    dfExport_pivot = (
        pd.concat([dfExport_pivot, dfExport_group])
        .sort_values(by=["Tên NVTT", "Miền"], ascending=True)
        .reset_index(drop=True)
    )

    dfExport_pivot.index = dfExport_pivot.index + 1
    dfExport_pivot = dfExport_pivot.reset_index()
    dfExport_pivot = dfExport_pivot.rename(columns={"index": "STT"})
    dfExport_pivot = dfExport_pivot.reset_index(drop=True)

    filter = dfExport_pivot["Miền"] == "zTổng cộng"
    dfExport_pivot.loc[filter, "Miền"] = "Tổng cộng     "

    # Save summary report using xlwings
    app = xw.App(visible=False)
    Workbook = xw.Book(file_name)

    try:
        Workbook.sheets.add(sheet_name)
    except:
        pass
    Worksheet = Workbook.sheets[sheet_name]
    Worksheet.clear_contents()
    Worksheet.clear_formats()
    Worksheet.range("A1").value = "Báo cáo đơn hàng C2 trong tuần"
    Worksheet.range("A1:D1").merge()
    Worksheet.range("A1:D1").autofit()
    # Mien = dfExport.loc[:, ["Miền"]].drop_duplicates()

    Worksheet.range("A2").value = dfExport.loc[:, ["Miền"]].drop_duplicates()
    Worksheet.range("A2:D2").merge()
    Worksheet.range("A2:D2").autofit()

    Worksheet.range("A3").value = "Từ ngày " + fDate + " đến hết ngày " + tDate
    Worksheet.range("A3:D3").merge()
    Worksheet.range("A3:D3").autofit()

    Worksheet.range("A5:Z5").font.bold = True

    rng = Worksheet.range("A5").expand()
    row = len(dfExport_pivot)
    print(f"{file_name}-> {dfExport_pivot.size} / {len(dfExport_pivot)}")
    col = dfExport_pivot.size / len(dfExport_pivot)

    for i in range(1, 3):
        Worksheet.range(rng.last_cell.row + row + 1, rng.last_cell.column + col - i).value = Total[i - 1]

    Worksheet.range(rng.last_cell.row + row + 1, 1).value = "Sản lượng/Doanh thu đơn hàng:"
    Worksheet.range((rng.last_cell.row + row + 1, 1), (rng.last_cell.row + row + 1, 30)).font.bold = True

    dfExport_pivot["Số lượng (tấn)"] = dfExport_pivot["Số lượng (tấn)"].map("{:,.2f}".format)
    dfExport_pivot["Thành tiền (VND)"] = dfExport_pivot["Thành tiền (VND)"].map("{:,.0f}".format)

    Worksheet.range("A5").options(index=False).value = dfExport_pivot

    rng = Worksheet.range("A5").expand()
    rng.api.Borders.Weight = 2

    for i in rng.rows:
        for j in i:
            if Worksheet.range(j).value == "Tổng cộng     ":
                i.font.bold = True

    rng.autofit()

    Workbook.save(file_name)
    Workbook.close()
    print(f"BCdonHang {file_name} => {sheet_name}")


# %% # GetData from reportFolder

for f in os.listdir(reportFolder):
    if f.find("Bao_cao_chi_tiet_VTKHC2") > -1:
        dfVTKHC2 = pd.read_excel(reportFolder + "\\" + f, engine="openpyxl")
        dfVTKHC2 = dfVTKHC2[3:].reset_index().drop(columns={"index"})
        dfVTKHC2.columns = dfVTKHC2.loc[0]
        dfVTKHC2 = dfVTKHC2.loc[1:]
        dfVTKHC2 = dfVTKHC2.drop(columns="STT")
        dfVTKHC2.to_csv("dfVTKHC2.csv", index=False)

    if f.find("Bao_cao_du_lieu_don_hang") > -1:
        dfDuLieuDonHang = pd.read_excel(reportFolder + "\\" + f, engine="openpyxl")
        dfDuLieuDonHang = dfDuLieuDonHang[3:].reset_index().drop(columns={"index"})
        dfDuLieuDonHang.columns = dfDuLieuDonHang.loc[0]
        dfDuLieuDonHang = dfDuLieuDonHang.loc[1:]
        dfDuLieuDonHang = dfDuLieuDonHang.drop(columns="STT")
        dfDuLieuDonHang.to_csv("dfDuLieuDonHang.csv", index=False)


# %%
fDate = fromDate[0:2] + "/" + fromDate[2:4] + "/" + fromDate[4:]
tDate = toDate[0:2] + "/" + toDate[2:4] + "/" + toDate[4:]

# %% Báo cáo Ghé thăm khách hàng C2 của NVTT theo miền

dfVTKHC2.columns = dfVTKHC2.columns.str.strip()
filter = dfVTKHC2.loc[:, "Miền"].str.contains("đào tạo", case=False)
dfVTKHC2 = dfVTKHC2[~filter]
dsMienGheTham = list(dfVTKHC2.loc[:, ["Miền"]].drop_duplicates().reset_index().drop(columns="index")["Miền"])


for Mien in dsMienGheTham:
    filter = dfVTKHC2["Miền"] == Mien
    file_name = os.path.abspath(reportFolder + "\\GSBH\\GSBH_" + Mien + ".xlsx")
    # Xuất báo cáo chi tiết
    sheet_name = "BaocaoGheThamC2"
    dfExport = dfVTKHC2[filter]
    BCGheThamC2(dfExport, file_name, sheet_name, reportFolder)

dfDuLieuDonHang.columns = dfDuLieuDonHang.columns.str.strip()
filter = dfDuLieuDonHang.loc[:, "Khu vực"].str.contains("đào tạo", case=False)
dfDuLieuDonHang = dfDuLieuDonHang[~filter]
dsMienDonHang = list(dfDuLieuDonHang.loc[:, ["Miền"]].drop_duplicates().reset_index().drop(columns="index")["Miền"])

for Mien in dsMienDonHang:
    # Xuất báo cáo chi tiết
    print(Mien)
    filter = dfDuLieuDonHang["Miền"] == Mien
    dfExport = dfDuLieuDonHang[filter]
    file_name = os.path.abspath(reportFolder + "\\GSBH\\GSBH_" + Mien + ".xlsx")
    sheet_name = "BaocaoDonHangC2"
    BCDonHangC2(dfExport, file_name, sheet_name, reportFolder)

# %% Báo cáo Ghé thăm khách hàng C2 của NVTT theo NVTT

dsNVTDonHang = list(
    dfDuLieuDonHang.loc[:, ["Mã NVTT"]].drop_duplicates().reset_index().drop(columns="index")["Mã NVTT"]
)

for NVTT in dsNVTDonHang:
    filter = dfDuLieuDonHang["Mã NVTT"] == NVTT
    dfExport = dfDuLieuDonHang[filter]
    print(f"{NVTT} => {len(dfExport)}")
    # Xuất báo cáo chi tiết
    file_name = os.path.abspath(reportFolder + "\\NVTT\\NVTT_" + NVTT + ".xlsx")
    sheet_name = "BaocaoDonHangC2"
    BCDonHangC2(dfExport, file_name, sheet_name, reportFolder)

dsNVTTGheTHam = list(dfVTKHC2.loc[:, ["MÃ NVTT"]].drop_duplicates().reset_index().drop(columns="index")["MÃ NVTT"])

for NVTT in dsNVTTGheTHam:
    filter = dfVTKHC2["MÃ NVTT"] == NVTT
    file_name = os.path.abspath(reportFolder + "\\NVTT\\NVTT_" + NVTT + ".xlsx")
    # Xuất báo cáo chi tiết
    sheet_name = "BaocaoGheThamC2"
    dfExport = dfVTKHC2[filter]
    BCGheThamC2(dfExport, file_name, sheet_name, reportFolder)
