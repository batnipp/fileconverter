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
import tempfile
import os
import shutil
from typing import Tuple, Any
from pathlib import Path

# Register all drivers
fiona.drvsupport.supported_drivers['KML'] = 'rw'
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
fiona.drvsupport.supported_drivers['ESRI Shapefile'] = 'rw'
fiona.drvsupport.supported_drivers['GPKG'] = 'rw'

# Format categories
GEOSPATIAL_FORMATS = ['Shapefile', 'KML/KMZ', 'GeoJSON', 'GPKG', 'CSV (with coordinates)']
GENERAL_FORMATS = ['JSON', 'Excel', 'XML', 'YAML', 'CSV (general)']

def create_zip_from_files(files: dict) -> bytes:
    """Create a zip file from multiple files."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for file_name, file_content in files.items():
            zip_file.writestr(file_name, file_content)
    return zip_buffer.getvalue()

def extract_kmz(kmz_file) -> io.BytesIO:
    """Extract KML from KMZ file."""
    with zipfile.ZipFile(kmz_file) as kmz:
        for name in kmz.namelist():
            if name.endswith('.kml'):
                return io.BytesIO(kmz.read(name))
    return None

def handle_shapefile_upload(uploaded_files) -> Tuple[gpd.GeoDataFrame, str]:
    """Handle multiple files for shapefile upload."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        for uploaded_file in uploaded_files:
            file_path = os.path.join(tmp_dir, uploaded_file.name)
            with open(file_path, 'wb') as f:
                f.write(uploaded_file.getvalue())
        
        shp_files = list(Path(tmp_dir).glob('*.shp'))
        if not shp_files:
            return None, "No .shp file found in uploaded files"
        
        try:
            gdf = gpd.read_file(shp_files[0])
            return gdf, None
        except Exception as e:
            return None, f"Error reading shapefile: {str(e)}"

def load_file(uploaded_file, input_format: str, additional_files=None) -> Tuple[Any, str]:
    """Load the uploaded file based on its format."""
    try:
        if 'CSV' in input_format:
            return pd.read_csv(uploaded_file), None
        elif input_format in ['JSON', 'GeoJSON']:
            json_data = json.load(uploaded_file)
            if 'type' in json_data and json_data['type'] == 'FeatureCollection':
                features = json_data['features']
                return gpd.GeoDataFrame.from_features(features), None
            return pd.DataFrame(json_data), None
        elif input_format == 'Excel':
            return pd.read_excel(uploaded_file), None
        elif input_format == 'XML':
            xml_data = uploaded_file.getvalue().decode('utf-8')
            dict_data = xmltodict.parse(xml_data)
            return pd.json_normalize(dict_data), None
        elif input_format == 'YAML':
            yaml_data = yaml.safe_load(uploaded_file)
            return pd.json_normalize(yaml_data), None
        elif input_format == 'KML/KMZ':
            if uploaded_file.name.endswith('.kmz'):
                kml_file = extract_kmz(uploaded_file)
                if kml_file:
                    return gpd.read_file(kml_file, driver='KML'), None
                return None, "No KML file found in KMZ archive"
            return gpd.read_file(uploaded_file, driver='KML'), None
        elif input_format == 'Shapefile':
            if not additional_files:
                return None, "Shapefile requires additional supporting files (.dbf, .shx, etc.)"
            return handle_shapefile_upload([uploaded_file] + additional_files)
        elif input_format == 'GPKG':
            return gpd.read_file(uploaded_file, driver='GPKG'), None
        else:
            return None, f"Unsupported input format: {input_format}"
    except Exception as e:
        return None, f"Error loading file: {str(e)}"

def convert_file(data: Any, output_format: str) -> Tuple[Any, str]:
    """Convert the data to the specified output format."""
    try:
        if isinstance(data, gpd.GeoDataFrame):
            if 'CSV' in output_format:
                csv_data = data.copy()
                csv_data['geometry'] = csv_data['geometry'].apply(lambda x: str(x))
                return csv_data, None
            elif output_format in ['GeoJSON', 'KML/KMZ', 'Shapefile', 'GPKG']:
                return data, None
        else:
            if 'CSV' in output_format:
                return data, None
            elif output_format == 'JSON':
                return data, None
            elif output_format == 'GeoJSON':
                if 'geometry' in data.columns:
                    gdf = gpd.GeoDataFrame.from_features([{
                        'type': 'Feature',
                        'geometry': row['geometry'],
                        'properties': {col: row[col] for col in data.columns if col != 'geometry'}
                    } for _, row in data.iterrows()])
                    return gdf, None
                return None, "Missing geometry column for GeoJSON conversion"
            elif output_format == 'Excel':
                return data, None
            elif output_format == 'XML':
                xml_data = data.to_dict(orient='records')
                xml_string = xmltodict.unparse({'root': {'record': xml_data}})
                return pd.read_xml(io.StringIO(xml_string)), None
            elif output_format == 'YAML':
                yaml_data = data.to_dict(orient='records')
                return pd.DataFrame(yaml_data), None
            elif output_format in ['KML/KMZ', 'Shapefile', 'GPKG']:
                return None, "Data must be spatial (GeoDataFrame) for this format"
            else:
                return None, f"Unsupported output format: {output_format}"
    except Exception as e:
        return None, f"Error converting file: {str(e)}"

def get_file_extension(format_name: str) -> str:
    """Get the appropriate file extension for the given format."""
    extensions = {
        'CSV (general)': 'csv',
        'CSV (with coordinates)': 'csv',
        'JSON': 'json',
        'GeoJSON': 'geojson',
        'Excel': 'xlsx',
        'XML': 'xml',
        'YAML': 'yml',
        'KML/KMZ': 'kml',
        'Shapefile': 'zip',
        'GPKG': 'gpkg'
    }
    return extensions.get(format_name, '')

def get_download_data(data: Any, output_format: str) -> Tuple[bytes, str]:
    """Prepare data for download based on format."""
    if output_format == 'Excel':
        output = io.BytesIO()
        data.to_excel(output, index=False)
        return output.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif 'CSV' in output_format:
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
    elif output_format == 'KML/KMZ':
        output = io.BytesIO()
        data.to_file(output, driver='KML')
        return output.getvalue(), "application/vnd.google-earth.kml+xml"
    elif output_format == 'Shapefile':
        with tempfile.TemporaryDirectory() as tmp_dir:
            shp_path = os.path.join(tmp_dir, 'output.shp')
            data.to_file(shp_path, driver='ESRI Shapefile')
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for file_path in Path(tmp_dir).glob('output.*'):
                    zip_file.write(file_path, file_path.name)
            return zip_buffer.getvalue(), "application/zip"
    elif output_format == 'GPKG':
        output = io.BytesIO()
        data.to_file(output, driver='GPKG')
        return output.getvalue(), "application/geopackage+sqlite3"

def display_conversion_matrix():
    """Display the geospatial format conversion matrix."""
    st.markdown("""
    <style>
    .conversion-matrix {
        background-color: #1a1a1a;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .matrix-table {
        width: 100%;
        border-collapse: collapse;
    }
    .matrix-table th, .matrix-table td {
        border: 1px solid #404040;
        padding: 0.5rem;
        text-align: center;
    }
    .matrix-table th {
        background-color: #262626;
    }
    .full-support {
        color: #00ff00;
        font-weight: bold;
    }
    .limited-support {
        color: #ffcc00;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

    geo_formats = ['Shapefile', 'KML/KMZ', 'GeoJSON', 'GPKG', 'CSV']
    conversion_matrix = {
        'Shapefile': {'Shapefile': '✓', 'KML/KMZ': '✓', 'CSV': '⚠', 'GeoJSON': '✓', 'GPKG': '✓'},
        'KML/KMZ': {'Shapefile': '✓', 'KML/KMZ': '✓', 'CSV': '⚠', 'GeoJSON': '✓', 'GPKG': '✓'},
        'CSV': {'Shapefile': '⚠', 'KML/KMZ': '⚠', 'CSV': '✓', 'GeoJSON': '⚠', 'GPKG': '⚠'},
        'GeoJSON': {'Shapefile': '✓', 'KML/KMZ': '✓', 'CSV': '⚠', 'GeoJSON': '✓', 'GPKG': '✓'},
        'GPKG': {'Shapefile': '✓', 'KML/KMZ': '✓', 'CSV': '⚠', 'GeoJSON': '✓', 'GPKG': '✓'}
    }

    st.markdown("<div class='conversion-matrix'>", unsafe_allow_html=True)
    st.markdown("### Geospatial Format Conversion Matrix", unsafe_allow_html=True)
    
    matrix_html = "<table class='matrix-table'><tr><th></th>"
    for format in geo_formats:
        matrix_html += f"<th>{format}</th>"
    matrix_html += "</tr>"
    
    for from_format in geo_formats:
        matrix_html += f"<tr><td><strong>{from_format}</strong></td>"
        for to_format in geo_formats:
            symbol = conversion_matrix[from_format][to_format]
            css_class = 'full-support' if symbol == '✓' else 'limited-support'
            matrix_html += f"<td class='{css_class}'>{symbol}</td>"
        matrix_html += "</tr>"
    
    matrix_html += "</table>"
    st.markdown(matrix_html, unsafe_allow_html=True)
    
    st.markdown("""
    <div style='margin-top: 1rem;'>
        <p><span class='full-support'>✓</span> = Full conversion supported</p>
        <p><span class='limited-support'>⚠</span> = Limited conversion (points only)</p>
    </div>
    </div>
    """, unsafe_allow_html=True)

def main():
    st.title("Enhanced File Format Converter")
    st.write("Convert between various file formats, including geospatial data!")

    # Format selection
    col1, col2 = st.columns(2)
    with col1:
        format_category = st.radio(
            "Select Format Category",
            ["Geospatial Formats", "General Formats"]
        )
        
        formats = GEOSPATIAL_FORMATS if format_category == "Geospatial Formats" else GENERAL_FORMATS
        input_format = st.selectbox('Input Format', formats)
    
    with col2:
        if format_category == "Geospatial Formats":
            output_formats = GEOSPATIAL_FORMATS
        else:
            output_formats = GENERAL_FORMATS
        output_format = st.selectbox('Output Format', output_formats)

    # Display conversion matrix for geospatial formats
    if format_category == "Geospatial Formats":
        display_conversion_matrix()
        st.markdown("---")

    # File upload
    file_types = {
        'CSV (general)': ['csv'],
        'CSV (with coordinates)': ['csv'],
        'JSON': ['json'],
        'GeoJSON': ['geojson', 'json'],
        'Excel': ['xlsx', 'xls'],
        'XML': ['xml'],
        'YAML': ['yml', 'yaml'],
        'KML/KMZ': ['kml', 'kmz'],
        'Shapefile': ['shp'],
        'GPKG': ['gpkg']
    }

    uploaded_file = st.file_uploader(
        "Upload your main file",
        type=file_types.get(input_format, ['*'])
    )

    # Additional file upload for shapefiles
    additional_files = None
    if input_format == 'Shapefile':
        st.write("Upload additional shapefile components (.dbf, .shx, etc.)")
        additional_files = st.file_uploader(
            "Upload supporting files",
            type=['dbf', 'shx', 'prj', 'cpg', 'sbn', 'sbx'],
            accept_multiple_files=True
        )

    if uploaded_file is not None:
        # Load the file
        data, error = load_file(uploaded_file, input_format, additional_files)
        
        if error:
            st.error(error)
        else:
            st.success("File loaded successfully!")
            
            # Show preview of the input data
            st.subheader("Input Data Preview")
            if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                st.dataframe(data.head(100))
            
            # Convert button
            if st.button("Convert"):
                converted_data, error = convert_file(data, output_format)
                
                if error:
                    st.error(error)
                else:
                    # Show preview of the converted data
                    st.subheader("Converted Data Preview")
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
