import pandas as pd
import os
import re

import pysharepoint as ps
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from sqlalchemy import create_engine

##############################################################
# This will be the URL that points to your sharepoint site.
# Make sure you change only the parts of the link that start with "Your"
sharepoint_base_url = "https://pvfcco.sharepoint.com"
url_shrpt = "https://pvfcco.sharepoint.com/sites/DMSTNB"
username_shrpt = "nhdminh@pvfcco.com.vn"
password_shrpt = "a3671c389"
folder_url_shrpt = "/sites/DMSTNB/Shared%20Documents"
subfolder_url_shrpt = "Báo cáo - DMS/Daily"
# subfolder_url_shrpt = "Báo cáo - DMS"
localPath = "/home/nhdminh/airflow/DMS_daily"


# %%
###Authentication
###For authenticating into your sharepoint site###
ctx_auth = AuthenticationContext(url_shrpt)
if ctx_auth.acquire_token_for_user(username_shrpt, password_shrpt):
    ctx = ClientContext(url_shrpt, ctx_auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    print("Authenticated into sharepoint as: ", web.properties["Title"])

else:
    print(ctx_auth.get_last_error())

site = ps.SPInterface(sharepoint_base_url, username_shrpt, password_shrpt)

##############################################################


class ReportGenerator:
    def __init__(self, report_folder, output_file):
        self.report_folder = report_folder
        # self.output_file = os.path.join(report_folder, "merged_reports.xlsx")
        self.output_file = output_file

    #
    def load_data(self, file_path):
        return pd.read_excel(file_path, engine="openpyxl")

    # def save_to_excel(self, df, file_name, sheet_name):
    #     with pd.ExcelWriter(file_name, engine="openpyxl", mode="a") as writer:
    #         df.to_excel(writer, sheet_name=sheet_name, index=False)

    def merge_reports_into_one(self, output_sheet_names, files_to_merge):
        with pd.ExcelWriter(self.output_file, engine="xlsxwriter") as writer:
            if not os.path.exists(self.output_file):
                df = pd.DataFrame()
                df.to_excel(writer, self.output_file)

            for sheet_name, file in zip(output_sheet_names, files_to_merge):
                df = pd.read_excel(file)
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    def generate_ghe_tham_c2_report(self, data_frame, file_name, sheet_name):
        # Implement your logic for generating Ghe Tham C2 report here
        self.save_to_excel(data_frame, file_name, sheet_name)

    def generate_don_hang_c2_report(self, data_frame, file_name, sheet_name):
        # Implement your logic for generating Don Hang C2 report here
        self.save_to_excel(data_frame, file_name, sheet_name)

    def send_email(self, file_name, email_to):
        # Implement your logic for sending email here
        pass

    def save_to_excel(self, data_frame, file_name, sheet_name):
        # exporting pandas DataFrames to Excel files.
        # Check if the Excel file already exists
        if os.path.exists(file_name):
            try:
                # If the file exists, open it in 'append' mode and replace the sheet if it already exists
                with pd.ExcelWriter(file_name, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    # Export the DataFrame to the specified sheet in the Excel file
                    data_frame.to_excel(writer, sheet_name=sheet_name, index=False)

            except Exception as error:
                # If there's an error during the export, print the error message and continue
                print(error)
                pass
        else:
            # If the Excel file doesn't exist, create it

            with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
                # Export the DataFrame to the specified sheet in the Excel file
                data_frame.to_excel(writer, sheet_name=sheet_name, index=False)

    def export_excel_gsbh(self, dfTotal, export_list, columns, prefix, sheet_name):
        for group in export_list:
            filter = dfTotal[columns] == group
            dfExport = dfTotal[filter]
            folder = f"/home/nhdminh/airflow/DMS_daily"
            # file_name = f"{self.report_folder}_{prefix}_{group}.xlsx"
            mien_folder = str.strip(dfExport["Miền"].drop_duplicates().reset_index(drop=True)[0])
            file_name = f"{folder}/{mien_folder}/{prefix}_{group}.xlsx"
            try:
                os.mkdir(os.path.abspath(file_name + "/.."))
            except:
                pass

            self.generate_ghe_tham_c2_report(dfExport, file_name, sheet_name)


def main():
    report_folder = "/home/nhdminh/airflow/DMS_daily/"
    pattern = "^\d+$"
    for f in sorted(os.listdir(report_folder)):
        if len(re.findall(pattern, f)) > 0:
            last = f
    report_folder = "/home/nhdminh/airflow/DMS_daily/" + last
    print(report_folder)
    report_generator = ReportGenerator(report_folder, "merged_reports.xlsx")
    print(os.path.abspath(report_generator.report_folder))

    # Load data to Dataframe
    for f in os.listdir(report_folder):
        if f.find("Bao_cao_chi_tiet_VTKHC2") > -1:
            vtkhc2_data = report_generator.load_data(os.path.join(report_folder, f))
            vtkhc2_data = vtkhc2_data[3:].reset_index().drop(columns={"index"})
            vtkhc2_data.columns = vtkhc2_data.loc[0]
            vtkhc2_data = vtkhc2_data.loc[1:]
            vtkhc2_data = vtkhc2_data.drop(columns="STT")
            vtkhc2_data.to_csv("/home/nhdminh/airflow/DMS_daily/Bao_cao_chi_tiet_VTKHC2.csv", index=False)

        if f.find("Bao_cao_du_lieu_don_hang") > -1:
            du_lieu_don_hang = report_generator.load_data(os.path.join(report_folder, f))
            du_lieu_don_hang = du_lieu_don_hang[3:].reset_index().drop(columns={"index"})
            du_lieu_don_hang.columns = du_lieu_don_hang.loc[0]
            du_lieu_don_hang = du_lieu_don_hang.loc[1:]
            du_lieu_don_hang = du_lieu_don_hang.drop(columns="STT")
            du_lieu_don_hang.to_csv("/home/nhdminh/airflow/DMS_daily/Bao_cao_du_lieu_don_hang.csv", index=False)
    # %% Báo cáo Ghé thăm khách hàng C2 của NVTT theo miền

    # input DS Miền/ data frame total
    # output file excel theo từng miền
    vtkhc2_data.columns = vtkhc2_data.columns.str.strip()
    filter = vtkhc2_data.loc[:, "Miền"].str.contains("đào tạo", case=False)
    vtkhc2_data = vtkhc2_data[~filter]
    dsMienGheTham = list(vtkhc2_data.loc[:, ["Miền"]].drop_duplicates().reset_index().drop(columns="index")["Miền"])
    print(f"print vtkhc2_data {dsMienGheTham}")
    report_generator.export_excel_gsbh(vtkhc2_data, dsMienGheTham, "Miền", "GSBH", "vtkhc2_data")

    du_lieu_don_hang.columns = du_lieu_don_hang.columns.str.strip()
    filter = du_lieu_don_hang.loc[:, "Khu vực"].str.contains("đào tạo", case=False)
    du_lieu_don_hang = du_lieu_don_hang[~filter]
    dsMienDonHang = list(
        du_lieu_don_hang.loc[:, ["Miền"]].drop_duplicates().reset_index().drop(columns="index")["Miền"]
    )
    print(f"du_lieu_don_hang {dsMienDonHang}")
    report_generator.export_excel_gsbh(vtkhc2_data, dsMienDonHang, "Miền", "GSBH", "du_lieu_don_hang")

    output_sheet_names = ["GheThamC2", "DonHangC2"]
    files_to_merge = ["Bao_cao_chi_tiet_VTKHC2.xlsx", "Bao_cao_chi_tiet_VTKHC2.xlsx"]
    report_generator.merge_reports_into_one(output_sheet_names, files_to_merge)
    # report_generator.send_email(report_generator.output_file, "recipient@example.com")

    dsNVTGheTham = list(
        vtkhc2_data.loc[:, ["MÃ NVTT"]].drop_duplicates().reset_index().drop(columns="index")["MÃ NVTT"]
    )
    print(f"vtkhc2_data {dsNVTGheTham}")
    report_generator.export_excel_gsbh(vtkhc2_data, dsNVTGheTham, "MÃ NVTT", "NVTT", "vtkhc2_data")

    dsNVTDonHang = list(
        du_lieu_don_hang.loc[:, ["Mã NVTT"]].drop_duplicates().reset_index().drop(columns="index")["Mã NVTT"]
    )
    print(f"dsNVTDonHang {dsNVTDonHang}")

    report_generator.export_excel_gsbh(du_lieu_don_hang, dsNVTDonHang, f"Mã NVTT", "NVTT", "du_lieu_don_hang")
    # site.upload_file_sharepoint(
    #     localPath + "/Summary/",
    #     f"{folder_url_shrpt}/{subfolder_url_shrpt}/Summary",
    #     "dfVTKHC2.csv",
    #     url_shrpt,
    # )


if __name__ == "__main__":
    main()
