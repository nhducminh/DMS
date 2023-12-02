import pandas as pd
import os


class ReportGenerator:
    def __init__(self, report_folder):
        self.report_folder = report_folder
        self.output_file = os.path.join(report_folder, "merged_reports.xlsx")

    def load_data(self, file_path):
        return pd.read_excel(file_path, engine="openpyxl")

    def save_to_excel(self, df, file_name, sheet_name):
        with pd.ExcelWriter(file_name, engine="openpyxl", mode="a") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    def merge_reports_into_one(self, output_sheet_names, files_to_merge):
        with pd.ExcelWriter(self.output_file, engine="xlsxwriter") as writer:
            for sheet_name, file in zip(output_sheet_names, files_to_merge):
                df = pd.read_excel(file)
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    def generate_ghe_tham_c2_report(self, data_frame, file_name, sheet_name):
        # Implement your logic for generating Ghe Tham C2 report here
        data_frame = data_frame[3:].reset_index().drop(columns={"index"})
        data_frame.columns = data_frame.loc[0]
        data_frame = data_frame.loc[1:]
        data_frame = data_frame.drop(columns="STT")
        self.save_to_excel(data_frame, file_name, sheet_name)

    def generate_don_hang_c2_report(self, data_frame, file_name, sheet_name):
        # Implement your logic for generating Don Hang C2 report here
        data_frame = data_frame[3:].reset_index().drop(columns={"index"})
        data_frame.columns = data_frame.loc[0]
        data_frame = data_frame.loc[1:]
        data_frame = data_frame.drop(columns="STT")
        self.save_to_excel(data_frame, file_name, sheet_name)

    def send_email(self, file_name, email_to):
        # Implement your logic for sending email here
        pass


def main():
    report_folder = "your_report_folder_path"
    report_generator = ReportGenerator(report_folder)
    for f in os.listdir(report_folder):
        if f.find("Bao_cao_chi_tiet_VTKHC2") > -1:
            vtkhc2_data = report_generator.load_data(os.path.join(report_folder, f))

        if f.find("Bao_cao_du_lieu_don_hang") > -1:
            du_lieu_don_hang = report_generator.load_data(os.path.join(report_folder, f))

    report_generator.generate_ghe_tham_c2_report(vtkhc2_data)
    report_generator.generate_don_hang_c2_report(du_lieu_don_hang)

    output_sheet_names = ["GheThamC2", "DonHangC2"]
    files_to_merge = ["ghe_tham_c2.xlsx", "don_hang_c2.xlsx"]

    report_generator.merge_reports_into_one(output_sheet_names, files_to_merge)

    report_generator.send_email(report_generator.output_file, "recipient@example.com")


if __name__ == "__main__":
    main()
