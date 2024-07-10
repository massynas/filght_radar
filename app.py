import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
from pyspark.sql import SparkSession

STORAGE_PATH = "/data/flight_data/"

spark = SparkSession.builder.appName("FlightDataDashboard").getOrCreate()

def get_data(data_type):
    df = spark.read.parquet(f"{STORAGE_PATH}{data_type}")
    return df.toPandas()

app = dash.Dash(__name__)

app.layout = html.Div(children=[
    html.H1(children='Flight Data Dashboard'),
    dcc.Dropdown(
        id='data-type-dropdown',
        options=[
            {'label': 'Flights', 'value': 'flights'},
            {'label': 'Airports', 'value': 'airports'},
            {'label': 'Airlines', 'value': 'airlines'},
            {'label': 'Zones', 'value': 'zones'}
        ],
        value='flights'
    ),
    dcc.Graph(id='example-graph')
])

@app.callback(
    dash.dependencies.Output('example-graph', 'figure'),
    [dash.dependencies.Input('data-type-dropdown', 'value')]
)
def update_graph(data_type):
    df = get_data(data_type)
    if data_type == 'flights':
        fig = px.line(df, x='timestamp', y='flight_count', title='Flight Data')
    elif data_type == 'airports':
        fig = px.scatter(df, x='latitude', y='longitude', title='Airports')
    elif data_type == 'airlines':
        fig = px.bar(df, x='name', y='country', title='Airlines')
    elif data_type == 'zones':
        fig = px.choropleth(df, geojson=df.geometry, title='Zones')
    return fig

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')
