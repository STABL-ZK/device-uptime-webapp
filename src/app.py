from datetime import datetime, timedelta
import dash
from dash import dcc, html, Input, Output
from dash.exceptions import PreventUpdate
import dash_mantine_components as dmc
import dash_table
import pandas as pd
import plotly.express as px
from influxdb_client import InfluxDBClient
import warnings
from influxdb_client.client.warnings import MissingPivotFunction
import uptime_calculation

# Disable influx pivot warnings
warnings.simplefilter("ignore", MissingPivotFunction)


class DataManager:

    table_data = None
    start_date = None
    stop_date = None

    def update_data(self, table_data):
        self.table_data = table_data

    def update_start_date(self, start_date):
        self.start_date = start_date

    def update_stop_date(self, stop_date):
        self.stop_date = stop_date

    def get_data(self):
        if self.table_data is None:
            raise Exception("No data present!")
        return self.table_data

    def get_start_date(self):
        if self.start_date is None:
            raise Exception("No start date present!")
        return self.start_date

    def get_stop_date(self):
        if self.stop_date is None:
            raise Exception("No stop date present!")
        return self.stop_date


radio_data = [["date","Date Range"],["now","Past Week"],["mon","Past Week Mon. - Mon."]]

# Initialize the Dash app
app = dash.Dash(__name__)

data_manager = DataManager()

# Define the layout of the app
app.layout = html.Div(
    [
        html.H1("InfluxDB Query and Results"),
        dmc.SimpleGrid(
            cols=3,
            spacing="md",
            verticalSpacing="md",
            children=[
                dmc.DateRangePicker(
                    id="date_range",
                    label="Time Range",
                ),
            ],
        ),
        dmc.Space(h=50),
        dmc.RadioGroup(
            children=dmc.Group([dmc.Radio(l, value=k) for k, l in radio_data], my=10),
            id="radiogroup-simple",
            value="now",
            label="Pick the time window to use",
            size="sm",
            mb=10,
        ),
        dmc.SimpleGrid(
            cols=3,
            spacing="md",
            verticalSpacing="md",
            children=[
                dmc.Button("Run Query", id="run-button", n_clicks=0),
                dmc.Button("Download Data", id="download-button"),
            ],
        ),
        dmc.Space(h=50),
        html.H2("Uptime visuals:"),
        dash_table.DataTable(
            id="results-table",
            columns=[
                {"name": "Device ID", "id": "device_id"},
                {"name": "Uptime", "id": "uptime"},
            ],
            data=[],
        ),
        dcc.Graph(id="uptime-bar-chart"),
        dcc.Download(id="download-dataframe-csv"),
    ]
)


# Define the callback to run the code, update the table and chart, and handle download
@app.callback(
    [
        Output("results-table", "data"),
        Output("uptime-bar-chart", "figure"),
    ],
    [
        Input("run-button", "n_clicks"),
        Input("download-button", "n_clicks"),
        Input("date_range", "value"),
        Input("radiogroup-simple", "value")
    ],
    prevent_initial_call=True,
)
def update_table_chart_and_download(run_clicks, download_clicks, date_range, radio_val):
    if download_clicks is not None:
        raise PreventUpdate
    # Run the InfluxDB query if the run button is clicked
    if run_clicks is not None and run_clicks > 0:
        if radio_val == "date":
            start_date_str, stop_date_str = date_range
            y, m, d = start_date_str.split("-")
            start_date = datetime(int(y), int(m), int(d))
            y, m, d = stop_date_str.split("-")
            stop_date = datetime(int(y), int(m), int(d))
            _, result_df, start_date, stop_date = uptime_calculation.get_uptimes_between(start_date, stop_date)
        elif radio_val == "now":
            _, result_df, start_date, stop_date = uptime_calculation.get_uptimes_of_last_week_until_now()
        elif radio_val == "mon":
            _, result_df, start_date, stop_date = uptime_calculation.get_uptimes_last_week_starting_monday()
        
        result_df = result_df._append({"device_id": "total_average", "uptime": (mean := result_df.uptime.mean()), "uptime_readable": "{:.2f}%".format(mean*100)}, ignore_index=True, )
        # Create a bar chart with Plotly
        fig = px.bar(
            result_df,
            x="device_id",
            y="uptime",
            title="Uptime by Device ID",
            labels={"uptime": "Uptime", "device_id": "Device ID"},
        )

        data_manager.update_data(result_df)
        data_manager.update_start_date(start_date)
        data_manager.update_stop_date(stop_date)

        table_data = result_df[["device_id", "uptime"]].to_dict("records")
        return table_data, fig

    return [], {}


# Define the callback to run the code, update the table and chart, and handle download
@app.callback(
    [Output("download-dataframe-csv", "data")],
    [Input("download-button", "n_clicks")],
    prevent_initial_call=True,
)
def update_table_chart_and_download(download_clicks):

    if download_clicks is not None and download_clicks > 0:
        # Prepare data for download as a CSV
        data_df = data_manager.get_data()
        start_date = data_manager.get_start_date()
        stop_date = data_manager.get_stop_date()
        return [dcc.send_data_frame(data_df.to_csv, f"uptimes_{start_date}--{stop_date}.csv")]

    else:
        raise PreventUpdate


# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
