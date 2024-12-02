import streamlit as st
import pandas as pd
import geopandas as gpd
import json
import yaml
import xmltodict
import io
import base64
import fiona
import zipfile
from typing import Tuple, Any

# Register KML driver
fiona.drvsupport.supported_drivers['KML'] = 'rw'
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'

def extract_kmz(kmz_file) -> io.BytesIO:
    """Extract KML from KMZ file."""
    with zipfile.ZipFile(kmz_file) as kmz:
        for name in kmz.namelist():
            if name.endswith('.kml'):
                return io.BytesIO(kmz.read(name))
    return None

def load_file(uploaded_file, input_format: str) -> Tuple[Any, str]:
    """Load the uploaded file based on its format."""
    try:
        if input_format == 'CSV':
            return pd.read_csv(uploaded_file), None
        elif input_format in ['JSON', 'GeoJSON']:
            if input_format == 'GeoJSON':
                return gpd.read_file(uploaded_file), None
            return pd.read_json(uploaded_file), None
        elif input_format == 'Excel':
            return pd.read_excel(uploaded_file), None
        elif input_format == 'XML':
            xml_data = uploaded_file.getvalue().decode('utf-8')
            dict_data = xmltodict.parse(xml_data)
            return pd.json_normalize(dict_data), None
        elif input_format == 'YAML':
            yaml_data = yaml.safe_load(uploaded_file)
            return pd.json_normalize(yaml_data), None
        elif input_format == 'KML':
            return gpd.read_file(uploaded_file, driver='KML'), None
        elif input_format == 'KMZ':
            kml_file = extract_kmz(uploaded_file)
            if kml_file:
                return gpd.read_file(kml_file, driver='KML'), None
            return None, "No KML file found in KMZ archive"
        else:
            return None, f"Unsupported input format: {input_format}"
    except Exception as e:
        return None, f"Error loading file: {str(e)}"

def convert_file(data: Any, output_format: str) -> Tuple[Any, str]:
    """Convert the data to the specified output format."""
    try:
        if isinstance(data, gpd.GeoDataFrame):
            if output_format == 'CSV':
                # Convert GeoJSON/KML to CSV, handling geometry column
                csv_data = data.copy()
                csv_data['geometry'] = csv_data['geometry'].apply(lambda x: str(x))
                return csv_data, None
            elif output_format in ['GeoJSON', 'KML']:
                return data, None
        else:
            if output_format == 'CSV':
                return data, None
            elif output_format == 'JSON':
                return pd.DataFrame(json.loads(data.to_json(orient='records'))), None
            elif output_format == 'GeoJSON':
                # Basic CSV to GeoJSON conversion (assuming lat/lon columns)
                if 'latitude' in data.columns and 'longitude' in data.columns:
                    gdf = gpd.GeoDataFrame(
                        data,
                        geometry=gpd.points_from_xy(data.longitude, data.latitude)
                    )
                    return gdf, None
                return None, "Missing latitude/longitude columns for GeoJSON conversion"
            elif output_format == 'Excel':
                return data, None
            elif output_format == 'XML':
                xml_data = data.to_dict(orient='records')
                xml_string = xmltodict.unparse({'root': {'record': xml_data}})
                return pd.read_xml(io.StringIO(xml_string)), None
            elif output_format == 'YAML':
                yaml_data = data.to_dict(orient='records')
                return pd.DataFrame(yaml_data), None
            elif output_format == 'KML':
                if isinstance(data, gpd.GeoDataFrame):
                    return data, None
                return None, "Data must be spatial (GeoDataFrame) to convert to KML"
            else:
                return None, f"Unsupported output format: {output_format}"
    except Exception as e:
        return None, f"Error converting file: {str(e)}"

def get_file_extension(format_name: str) -> str:
    """Get the appropriate file extension for the given format."""
    extensions = {
        'CSV': 'csv',
        'JSON': 'json',
        'GeoJSON': 'geojson',
        'Excel': 'xlsx',
        'XML': 'xml',
        'YAML': 'yml',
        'KML': 'kml',
        'KMZ': 'kmz'
    }
    return extensions.get(format_name, '')

def get_download_data(data: Any, output_format: str) -> Tuple[bytes, str]:
    """Prepare data for download based on format."""
    if output_format == 'Excel':
        output = io.BytesIO()
        data.to_excel(output, index=False)
        return output.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif output_format == 'CSV':
        return data.to_csv(index=False).encode('utf-8'), "text/csv"
    elif output_format == 'JSON':
        return data.to_json(orient='records').encode('utf-8'), "application/json"
    elif output_format == 'GeoJSON':
        return data.to_json().encode('utf-8'), "application/json"
    elif output_format == 'XML':
        xml_data = data.to_dict(orient='records')
        return xmltodict.unparse({'root': {'record': xml_data}}).encode('utf-8'), "application/xml"
    elif output_format == 'YAML':
        yaml_data = data.to_dict(orient='records')
        return yaml.dump(yaml_data).encode('utf-8'), "application/x-yaml"
    elif output_format == 'KML':
        output = io.BytesIO()
        data.to_file(output, driver='KML')
        return output.getvalue(), "application/vnd.google-earth.kml+xml"

def main():
    st.title("File Format Converter")
    st.write("Convert between different file formats easily!")

    # Available formats
    formats = ['CSV', 'JSON', 'GeoJSON', 'Excel', 'XML', 'YAML', 'KML', 'KMZ']
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload your file",
        type=['csv', 'json', 'geojson', 'xlsx', 'xls', 'xml', 'yml', 'yaml', 'kml', 'kmz']
    )

    # Format selection
    col1, col2 = st.columns(2)
    with col1:
        input_format = st.selectbox('Input Format', formats)
    with col2:
        # Filter output formats based on input format
        if input_format in ['KML', 'KMZ', 'GeoJSON']:
            output_formats = ['CSV', 'GeoJSON', 'KML']
        else:
            output_formats = formats
        output_format = st.selectbox('Output Format', output_formats)

    if uploaded_file is not None:
        # Load the file
        data, error = load_file(uploaded_file, input_format)
        
        if error:
            st.error(error)
        else:
            st.success("File loaded successfully!")
            
            # Show preview of the input data
            st.subheader("Input Data Preview (First 100 rows)")
            if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                st.dataframe(data.head(100))
            
            # Convert button
            if st.button("Convert"):
                converted_data, error = convert_file(data, output_format)
                
                if error:
                    st.error(error)
                else:
                    # Show preview of the converted data
                    st.subheader("Converted Data Preview (First 100 rows)")
                    if isinstance(converted_data, (pd.DataFrame, gpd.GeoDataFrame)):
                        st.dataframe(converted_data.head(100))
                    
                    # Prepare download button
                    file_extension = get_file_extension(output_format)
                    download_data, mime_type = get_download_data(converted_data, output_format)
                    
                    st.download_button(
                        label="Download Converted File",
                        data=download_data,
                        file_name=f"converted.{file_extension}",
                        mime=mime_type
                    )
                    
                    st.success("Conversion completed successfully!")

if __name__ == "__main__":
    main()