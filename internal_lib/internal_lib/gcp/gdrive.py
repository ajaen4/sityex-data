from googleapiclient.discovery import build


class GDrive:
    def __init__(self, service_acc_creds: dict):
        self.g_drive_service = build("sheets", "v4", credentials=service_acc_creds)

    def fetch_gsheets_data(self, spread_sheet_id: str):
        result = (
            self.g_drive_service.spreadsheets()
            .values()
            .get(spreadsheetId=spread_sheet_id, range="Feed")
            .execute()
        )
        values = result.get("values", [])
        return values
