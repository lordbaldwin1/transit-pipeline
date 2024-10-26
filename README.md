# 🚍 Data Engineering Visualization Project

This project visualizes bus speeds across Portland, Oregon by calculating speed from breadcrumb location and timestamp data at bus stops. The data pipeline was automated with Google Cloud PubSub and processed into a PostgreSQL database, using Google Compute Engine with Linux Virtual Machines for continuous data flow and processing.

## 📊 Overview
This project leverages real-time data from Portland bus stops, using geolocation and timestamp data to compute speed metrics. The computed speeds are then visualized on a dynamic map for insights into speed trends by location across the city.

## 🛠️ Tools & Technologies
- **Python** (`pandas`): Data cleaning and processing
- **Google Cloud PubSub**: Real-time data ingestion
- **Google Compute Engine (Linux VMs)**: Pipeline automation
- **PostgreSQL**: Data storage and querying
- **Mapbox GL**: Geospatial visualization
- **UNIX**: Task automation and data handling

## 📈 Data Pipeline
1. **Data Ingestion**: Breadcrumb data (location and timestamp) is transmitted through Google Cloud PubSub to ensure continuous data flow.
2. **Processing**: Data is processed in Python using `pandas` to compute speed metrics based on timestamp and geolocation.
3. **Storage**: Processed data is stored in a PostgreSQL database for efficient retrieval and visualization.
4. **Visualization**: Using Mapbox GL, speeds are displayed dynamically on a map, highlighting speed by location for easy analysis.

## 🖼️ Example Visualizations
Here are sample visualizations showcasing speed data across Portland's bus stops:

| Map Visualization Example |
|---------------------------|
| ![Speed by Location 1](https://github.com/user-attachments/assets/83f8175b-1069-4121-a0af-fc49ef80ee65) |
| ![Speed by Location 2](https://github.com/user-attachments/assets/2ab0905d-1ca4-4cbb-a51a-cf6a94f65bdd) |
| ![Speed by Location 3](https://github.com/user-attachments/assets/270785bd-1f73-4cab-a789-d9f56a6b449f) |
| ![Speed by Location 4](https://github.com/user-attachments/assets/d0795933-fd9b-495b-8694-b7219de8f3fa) |
| ![Speed by Location 5](https://github.com/user-attachments/assets/fae44132-53f2-48d5-b3e5-94c9019fbaee) |
| ![Speed by Location 6](https://github.com/user-attachments/assets/b8e00db3-e2dc-42fa-b1f5-5a3205f9baf4) |
