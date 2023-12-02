# %%
import datetime as dt
import os
import shutil

import numpy as np
import openpyxl
import pandas as pd
import win32com.client as win32
import xlsxwriter
import xlwings as xw
import sys

print(sys.getdefaultencoding())

win32c = win32.constants
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
dsMienDonHang = list(dfDuLieuDonHang.loc[:, ["Miền"]].drop_duplicates().reset_index().drop(columns="index")["Miền"])


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


# # %% Báo cáo GSBH
# # Miền
# # gui email
# outlook = win32.Dispatch("outlook.application")
# file_list = []
# for f in os.listdir(reportFolder + "\\GSBH"):
#     file_list.append(f)

# for i in range(0, len(GSBH_CTVM)):
#     print("GSBH_" + GSBH_CTVM.loc[i, "Miền"] + ".xlsx")

# GSBH_CTVM = GSBH_CTVM.assign(FileName=lambda x: ("GSBH_" + x["Miền"] + ".xlsx"))

# filter = GSBH_CTVM["FileName"].isin(file_list)
# GSBH_CTVM = GSBH_CTVM[filter]

# for i in range(0, len(GSBH_CTVM)):
#     file_name = reportFolder + "\\GSBH\\" + GSBH_CTVM.loc[i, "FileName"]
#     file_name = os.path.abspath(file_name)
#     # Nhân sự
#     Subject = "[DMS][GSBH] Báo cáo hệ thống DMS từ ngày " + tDate + " đến ngày " + fDate
#     HTMLBody = (
#         "<h2> Kính gửi anh/chị "
#         + GSBH_CTVM.loc[i, "Nhân sự"]
#         + "</h2></br>Tổ DMS kính gửi anh/chị báo cáo hệ thống DMS từ ngày "
#         + fDate
#         + " đến ngày "
#         + tDate
#         + " tại "
#         + GSBH_CTVM.loc[i, "Miền"]
#     )

#     signature = """
#             </br>
#             <p>Tr&acirc;n trọng.</p>
#             <p>--------------------------------------------------------</p>
#             <p>Nguyễn Ho&agrave;ng Đức Minh (Mr.) - Chuy&ecirc;n vi&ecirc;n Ban Tiếp thị v&agrave; Truyền th&ocirc;ng</p>
#             <p>Tổng c&ocirc;ng ty Ph&acirc;n b&oacute;n v&agrave; H&oacute;a chất Dầu kh&iacute; (PVFCCo)&nbsp;</p>
#             <p>PVFCCo Tower, 43 Mạc Đĩnh Chi, P. Đakao, Q. 1, Tp. Hồ Ch&iacute; Minh</p>
#             <p>SĐT: 028.3825.6258 &nbsp;Di động: 0935.396.887</p>
#             <p>Website: <a href="http://www.dpm.vn/">www.dpm.vn</a></p>
#             """
#     mail = outlook.CreateItem(0)
#     mail.To = "nhdminh@pvfcco.com.vn"
#     email_to = GSBH_CTVM.loc[i, "Email"]
#     mail.To = email_to
#     mail.CC = "nhdminh@pvfcco.com.vn"
#     mail.Subject = Subject
#     mail.HTMLBody = HTMLBody + signature
#     # To attach a file to the email (optional):
#     try:
#         mail.Attachments.Add(file_name)
#         #   mail.Send()
#         print("đã gửi email tới\n", file_name, email_to)
#     except:
#         pass


# %%
# # print(os.popen("C:\\Program Files\\Microsoft OneDrive\\OneDrive.exe").read())
# outlook = win32.Dispatch('outlook.application')

# for ff in os.listdir(reportFolder + "\\NVTT"):
#     email = ff[5:ff.find('.xlsx')].lower()+"@pvfcco.com.vn"

#     if ff[5:ff.find('.xlsx')].lower() =="nhlam":
#         email = ff[5:ff.find('.xlsx')].lower()+"01@pvfcco.com.vn"
#     if ff[5:ff.find('.xlsx')].lower() =="dhquan":
#         email = ff[5:ff.find('.xlsx')].lower()+".sbd@pvfcco.com.vn"


#     filename = os.path.abspath(f'{reportFolder}\\NVTT\\{ff}')
#     # print(filename)
#     Subject = '[DMS][NVTT] Báo cáo hệ thống DMS từ ngày ' + tDate + ' đến ngày '+ fDate
#     HTMLBody = '<h2> Kính gửi anh/chị '+ ff[5:ff.find('.xlsx')] +\
#                 '</h2></br>Tổ DMS kính gửi anh/chị báo cáo hệ thống DMS từ ngày ' + fDate + ' đến ngày '+ tDate

#     signature = '''
#                 </br>
#                 <p>Tr&acirc;n trọng.</p>
#                 <p>--------------------------------------------------------</p>
#                 <p>Nguyễn Ho&agrave;ng Đức Minh (Mr.) - Chuy&ecirc;n vi&ecirc;n Ban Tiếp thị v&agrave; Truyền th&ocirc;ng</p>
#                 <p>Tổng c&ocirc;ng ty Ph&acirc;n b&oacute;n v&agrave; H&oacute;a chất Dầu kh&iacute; (PVFCCo)&nbsp;</p>
#                 <p>PVFCCo Tower, 43 Mạc Đĩnh Chi, P. Đakao, Q. 1, Tp. Hồ Ch&iacute; Minh</p>
#                 <p>SĐT: 028.3825.6258 &nbsp;Di động: 0935.396.887</p>
#                 <p>Website: <a href="http://www.dpm.vn/">www.dpm.vn</a></p>
#                 '''
#     mail = outlook.CreateItem(0)
#     # mail.To = "nhdminh@pvfcco.com.vn"
#     mail.To = email
#     # mail.CC = "nhdminh@pvfcco.com.vn"
#     mail.Subject = Subject
#     mail.HTMLBody = HTMLBody +signature
#     # To attach a file to the email (optional):
#     try:
#         mail.Attachments.Add(filename)
#         # mail.Send()
#         print("đã gửi email tới",filename,email)
#     except Exception as e:
#         print(f"Có lỗi khi gửi email tới\n",filename,email)
#         print(e)
#         pass


# %%
