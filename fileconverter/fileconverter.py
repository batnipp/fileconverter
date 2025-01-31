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
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Register all drivers
fiona.drvsupport.supported_drivers['KML'] = 'rw'
fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
fiona.drvsupport.supported_drivers['ESRI Shapefile'] = 'rw'
fiona.drvsupport.supported_drivers['GPKG'] = 'rw'

# Define supported formats and their compatibility
FORMATS = {
    'CSV': {'description': 'Comma-separated values file', 'extensions': ['.csv']},
    'JSON': {'description': 'JavaScript Object Notation', 'extensions': ['.json']},
    'GeoJSON': {'description': 'Geographic JSON format', 'extensions': ['.geojson']},
    'Excel': {'description': 'Microsoft Excel format', 'extensions': ['.xlsx', '.xls']},
    'XML': {'description': 'Extensible Markup Language', 'extensions': ['.xml']},
    'YAML': {'description': 'YAML Ain\'t Markup Language', 'extensions': ['.yaml', '.yml']},
    'KML': {'description': 'Keyhole Markup Language', 'extensions': ['.kml', '.kmz']},
    'Shapefile': {'description': 'ESRI Shapefile', 'extensions': ['.shp', '.zip']},
    'GPKG': {'description': 'GeoPackage format', 'extensions': ['.gpkg']}
}

# Conversion compatibility matrix
COMPATIBILITY_MATRIX = {
    'CSV': ['JSON', 'Excel', 'XML', 'YAML'],
    'JSON': ['CSV', 'Excel', 'XML', 'YAML'],
    'GeoJSON': ['CSV', 'KML', 'Shapefile', 'GPKG'],
    'Excel': ['CSV', 'JSON', 'XML', 'YAML'],
    'XML': ['CSV', 'JSON', 'Excel', 'YAML'],
    'YAML': ['CSV', 'JSON', 'Excel', 'XML'],
    'KML': ['GeoJSON', 'Shapefile', 'GPKG'],
    'Shapefile': ['GeoJSON', 'KML', 'GPKG'],
    'GPKG': ['GeoJSON', 'KML', 'Shapefile']
}

def show_compatibility_matrix():
    """Display the format compatibility matrix"""
    st.subheader("Format Compatibility Matrix")
    
    # Create matrix data
    formats = list(COMPATIBILITY_MATRIX.keys())
    matrix_data = []
    
    for input_format in formats:
        row = []
        for output_format in formats:
            if output_format in COMPATIBILITY_MATRIX[input_format]:
                row.append('✅')
            else:
                row.append('❌')
        matrix_data.append(row)
    
    # Create DataFrame for display
    df = pd.DataFrame(matrix_data, columns=formats, index=formats)
    st.write("From ⬇️ To ➡️")
    st.dataframe(df)

def detect_file_format(file_name: str) -> str:
    """Detect the format of the uploaded file based on its extension"""
    ext = os.path.splitext(file_name)[1].lower()
    for format_name, format_info in FORMATS.items():
        if ext in format_info['extensions']:
            return format_name
    return None

def get_compatible_formats(input_format: str) -> list:
    """Get list of compatible output formats for the given input format"""
    return COMPATIBILITY_MATRIX.get(input_format, [])

def validate_file_size(file_size: int) -> str:
    """Validate file size and return error message if too large"""
    MAX_SIZE = 200 * 1024 * 1024  # 200MB
    if file_size > MAX_SIZE:
        return f"File too large. Maximum size is {MAX_SIZE / (1024 * 1024)}MB"
    return None

def process_uploaded_zip(zip_file) -> Tuple[gpd.GeoDataFrame, str]:
    """Process an uploaded ZIP file containing shapefile components."""
    try:
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            # First, save the uploaded file to a temporary file
            temp_zip_path = os.path.join(tmp_dir, "temp.zip")
            with open(temp_zip_path, 'wb') as f:
                f.write(zip_file.getvalue())
            
            # Now try to extract it
            with zipfile.ZipFile(temp_zip_path) as z:
                z.extractall(tmp_dir)
            
            # Look for .shp files
            shp_files = list(Path(tmp_dir).glob('**/*.shp'))
            if not shp_files:
                return None, "No .shp file found in the ZIP archive"
            
            # Try to read the shapefile
            gdf = gpd.read_file(shp_files[0])
            return gdf, None

    except zipfile.BadZipFile:
        return None, "The uploaded file is not a valid ZIP archive"
    except Exception as e:
        logger.error(f"Error processing shapefile: {str(e)}")
        return None, f"Error processing shapefile: {str(e)}"

def load_file(uploaded_file, input_format: str) -> Tuple[Any, str]:
    """Load the uploaded file based on its format."""
    try:
        size_error = validate_file_size(uploaded_file.size)
        if size_error:
            return None, size_error

        with st.spinner('Loading file...'):
            if 'CSV' in input_format:
                encoding = st.selectbox('Select file encoding', ['utf-8', 'latin1', 'iso-8859-1'])
                return pd.read_csv(uploaded_file, encoding=encoding), None
            elif input_format == 'Shapefile':
                # Check if it's a zip file by looking at the extension
                if not uploaded_file.name.lower().endswith('.zip'):
                    return None, "Shapefiles must be uploaded as a ZIP archive containing all required files (.shp, .dbf, .shx)"
                return process_uploaded_zip(uploaded_file)
            elif input_format in ['KML/KMZ', 'GeoJSON', 'GPKG']:
                return gpd.read_file(uploaded_file), None
            elif input_format == 'JSON':
                data = json.load(uploaded_file)
                if isinstance(data, list):
                    return pd.DataFrame(data), None
                elif isinstance(data, dict):
                    return pd.DataFrame([data]), None
            elif input_format == 'Excel':
                return pd.read_excel(uploaded_file), None
            elif input_format == 'XML':
                data = xmltodict.parse(uploaded_file.read())
                return pd.DataFrame([data]), None
            elif input_format == 'YAML':
                data = yaml.safe_load(uploaded_file)
                return pd.DataFrame([data]), None

    except Exception as e:
        logger.error(f"Error loading file: {str(e)}")
        return None, f"Error loading file: {str(e)}"

def main():
    st.set_page_config(
        page_title="Enhanced File Format Converter",
        page_icon="🔄",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("Enhanced File Format Converter")
    st.write("Convert between various file formats - just drag and drop!")

    # File upload section - now first!
    uploaded_file = st.file_uploader(
        "Drag and drop your file here",
        type=[ext[1:] for format_info in FORMATS.values() for ext in format_info['extensions']],
        help="Upload any supported file format"
    )

    # Compatibility Matrix in expander
    with st.expander("View Format Compatibility Matrix"):
        show_compatibility_matrix()

    if uploaded_file is not None:
        input_format = detect_file_format(uploaded_file.name)
        if input_format:
            st.write(f"Detected format: **{input_format}**")
            
            # Show compatible output formats
            compatible_formats = get_compatible_formats(input_format)
            if compatible_formats:
                col1, col2 = st.columns([1, 2])
                with col1:
                    output_format = st.selectbox(
                        "Convert to:",
                        compatible_formats
                    )
                
                # Load and convert file
                with st.spinner('Processing file...'):
                    data, error = load_file(uploaded_file, input_format)
                
                if error:
                    st.error(error)
                else:
                    st.success("File loaded successfully!")
                    
                    # Show preview in tabs
                    tab1, tab2 = st.tabs(["Data Preview", "Map Preview"])
                    
                    with tab1:
                        if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                            st.dataframe(data.head(100))
                    
                    with tab2:
                        # Add map preview for geospatial data
                        if isinstance(data, gpd.GeoDataFrame):
                            try:
                                import folium
                                from streamlit_folium import st_folium
                                
                                m = folium.Map()
                                data.explore(m=m)
                                st_folium(m, width=700, height=500)
                            except Exception as e:
                                logger.error(f"Error displaying map: {str(e)}")
                                st.warning("Could not display map preview")
                        else:
                            st.info("Map preview is only available for geospatial data formats.")

                    # Convert and download section
                    try:
                        if isinstance(data, gpd.GeoDataFrame):
                            geojson_str = data.to_crs(4326).to_json()
                            st.download_button(
                                label=f"Download as {output_format}",
                                data=geojson_str,
                                file_name=f"{uploaded_file.name.split('.')[0]}.{output_format.lower()}",
                                mime="application/json"
                            )
                        st.success("Conversion completed successfully!")
                    except Exception as e:
                        logger.error(f"Error converting file: {str(e)}")
                        st.error(f"Error during conversion: {str(e)}")
            else:
                st.warning(f"No compatible output formats found for {input_format}")
        else:
            st.error("Unsupported file format")

    # Add a footer with supported formats
    st.markdown("---")
    st.caption("Supported formats: CSV, JSON, GeoJSON, Excel, XML, YAML, KML, Shapefile, GPKG")
    st.caption("Max file size: 200MB")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error("An unexpected error occurred. Please try again.")
