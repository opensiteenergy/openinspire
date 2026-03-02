import geopandas as gpd
import json
import math
import numpy as np
import os
import platform
import re
import sys
import yaml
import requests
import time
import zipfile
import shutil
import subprocess
import glob
import concurrent.futures
import logging
import uuid
import importlib.resources as pkg_resources
from bs4 import BeautifulSoup
from importlib import resources
from shapely.geometry import box
from urllib.parse import urljoin, urlparse

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# [2026-01-24] Imports must stay at the top.
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(threadName)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("open_inspire.log")
    ]
)
logger = logging.getLogger(__name__)

class OpenINSPIRE:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.cache_dir = self.config.get('cache_dir', './cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        self.sources = self.config.get('sources', {})
        self.area = self.config.get('area', {})

    def run(self):
        """Processes each source defined in the YAML strategy."""

        output_file = self.config.get('output', 'output.gpkg')

        # Always delete output file to be safe and prevent amalgamation 
        # adding to previous failed processing run
        if os.path.exists(output_file): os.remove(output_file)

        for name, cfg in self.sources.items():
            logger.info(f"--- Processing {name} ---")
            if cfg.get('strategy') == "selenium_web":
                self.handle_selenium_web(cfg)
            elif cfg.get('strategy') == "simple_web":
                self.handle_simple_web(cfg)

        self.amalgamate_gpkgs(self.cache_dir, output_file)
        self.convert_to_utm30n(output_file)

        if self.area:
            self.apply_post_amalgamation_filter(output_file)
            self.apply_simple_buffer(output_file)
            self.dissolve_overlaps(output_file)

    def convert_to_utm30n(self, output_file):
        """
        Converts the GeoPackage in-place to EPSG:25830.
        This ensures all subsequent ST_Area and ST_Buffer operations are metric.
        """
        temp_file = f"reproject_{uuid.uuid4().hex}.gpkg"
        
        logger.info(f"Converting {output_file} to EPSG:25830 (UTM 30N)...")
        
        try:
            # -t_srs sets the target projection
            # -s_srs is often needed if the input source is missing a PRJ file 
            # (Assuming EPSG:4326 for now, or let GDAL auto-detect)
            cmd = [
                "ogr2ogr",
                "-f", "GPKG",
                temp_file,
                output_file,
                "-t_srs", "EPSG:25830",
                "-nln", "merged_data",
                "-overwrite"
            ]
            
            subprocess.run(cmd, check=True, capture_output=True)
            
            # In-place swap
            os.remove(output_file)
            os.rename(temp_file, output_file)
            
            logger.info("Reprojection complete. File is now metric (25830).")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Reprojection failed: {e.stderr.decode()}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            return False
    
    def apply_simple_buffer(self, target_gpkg):
        """
        Applies a 0.1m buffer in-place by using a temporary file and swapping.
        Ensures spatial indexes and metadata are rebuilt correctly.
        """
        # Create a unique temporary filename
        temp_gpkg = f"temp_buffer_{uuid.uuid4().hex}.gpkg"
        
        # SQL logic for the buffer
        # Note: We select specific columns or * to preserve data, 
        # but geom must be the first column for ogr2ogr's default behavior.
        sql = """
        SELECT 
            ST_Buffer(
                ST_MakeValid(ST_SnapToGrid(geom, 0.1)), 
                0.1, 
                1
            ) AS geom,
            *
        FROM merged_data
        """

        logger.info(f"Applying 0.1m buffer to {target_gpkg} in-place to eliminate slivers and amalgamate neighbouring polygons...")
        
        try:
            # Run ogr2ogr to create the buffered temp file
            cmd = [
                "ogr2ogr",
                "-f", "GPKG",
                temp_gpkg,
                target_gpkg,
                "-dialect", "sqlite",
                "-sql", sql,
                "-nlt", "PROMOTE_TO_MULTI",
                "-nln", "merged_data", 
                "-overwrite"
            ]
            
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Replace the original file with the buffered one
            os.replace(temp_gpkg, target_gpkg)
            logger.info(f"In-place buffer complete for: {target_gpkg}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Buffer operation failed: {e.stderr}")
            if os.path.exists(temp_gpkg):
                os.remove(temp_gpkg)
            raise
        except Exception as e:
            logger.error(f"File swap failed: {str(e)}")
            if os.path.exists(temp_gpkg):
                os.remove(temp_gpkg)
            raise

    def generate_processing_grid(self, clipping_url, grid_size, output_path="processing_grid.gpkg"):
        # 1. Fetch bounds from the remote URL
        logger.info(f"Fetching extent from: {clipping_url}")
        gdf_meta = gpd.read_file(clipping_url)
        minx, miny, maxx, maxy = gdf_meta.total_bounds
        crs = gdf_meta.crs

        # 2. Create the grid coordinates
        x_coords = np.arange(minx, maxx, grid_size)
        y_coords = np.arange(miny, maxy, grid_size)

        grid_cells = []
        for x in x_coords:
            for y in y_coords:
                # Create a square/rectangular cell
                grid_cells.append(box(x, y, x + grid_size, y + grid_size))

        # 3. Build GeoDataFrame and save
        grid_gdf = gpd.GeoDataFrame({'geometry': grid_cells}, crs=crs)
        
        # Optional: Filter only cells that actually intersect the clipping layer
        # This prevents creating grid cells over the open ocean
        logger.info("Filtering grid to intersection...")
        grid_gdf = grid_gdf[grid_gdf.intersects(gdf_meta.unary_union)]

        grid_gdf.to_file(output_path, driver="GPKG")
        logger.info(f"Successfully saved {len(grid_gdf)} cells to {output_path}")

    def dissolve_overlaps(self, input_gpkg, grid_size_meters=50000):
        """
        Runs ST_Union on grid squares
        """

        clipping_url = resources.files("openinspire").joinpath("clipping-master-EPSG-25830.gpkg")
        processing_grid = "processing_grid.gpkg"
        processing_grid_size = 50000
        if not os.path.exists(processing_grid):
            self.generate_processing_grid(clipping_url, processing_grid_size, processing_grid)
        grid_abs = os.path.abspath(processing_grid)
        data_abs = os.path.abspath(input_gpkg)

        temp_output = f"dissolved_temp_{uuid.uuid4().hex}.gpkg"

        extent_raw = subprocess.check_output(["ogrinfo", "-json", "-so", input_gpkg, "merged_data"], text=True)
        extent_json = json.loads(extent_raw)
        extent = extent_json['layers'][0]['geometryFields'][0]['extent']
        minx, miny, maxx, maxy = extent

        # Add processing_grid to input gpkg if not already added 
        # This makes SQLite queries straightfoward
        layer_info = subprocess.check_output(["ogrinfo", "-so", "-q", input_gpkg], text=True)
        if "processing_grid" not in layer_info:
            logger.info("Layer 'processing_grid' not found. Adding it now...")
            subprocess.run([
                "ogr2ogr", "-update", "-append", 
                input_gpkg, processing_grid, 
                "-nln", "processing_grid"
            ], check=True)
        else:
            logger.info("Layer 'processing_grid' already exists. Skipping copy.")

        # 3. Get FIDs that actually overlap the data area
        sql_fids = f"SELECT fid FROM processing_grid WHERE ST_Intersects(geom, BuildMBR({minx}, {miny}, {maxx}, {maxy}))"
        fids_raw = subprocess.check_output(["ogrinfo", "-sql", sql_fids, "-q", input_gpkg], text=True)
        fids = re.findall(r"OGRFeature\(SELECT\):(\d+)", fids_raw)
        total_cells = len(fids)

        logger.info(f"Starting dissolve using {total_cells} grid cells from {processing_grid}")

        # 2. Iterate through each grid cell by FID
        for index in range(len(fids)):
            fid = int(fids[index])
            logger.info(f"Processing grid cell {index + 1}/{total_cells}")

            sql = f"""
            SELECT ST_Union(ST_Intersection(ST_MakeValid(d.geom), g.geom)) AS geom 
            FROM merged_data d 
            JOIN processing_grid g ON ST_Intersects(d.geom, g.geom) 
            WHERE g.fid = {fid} 
            GROUP BY g.fid
            """

            cmd = [
                "ogr2ogr", "-update", "-append", "-f", "GPKG", temp_output, input_gpkg,
                "-dialect", "sqlite",
                "-sql", sql,
                "-a_srs", "EPSG:25830",
                "-explodecollections",
                "-nln", "merged_data",
                "-nlt", "POLYGON"
            ]
            
            # Setup the output table on the first pass
            if not os.path.exists(temp_output):
                cmd.extend(["-lco", "GEOMETRY_NAME=geom", "-lco", "SPATIAL_INDEX=YES"])

            try:
                subprocess.run(cmd, check=True, capture_output=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"ogr2ogr failed {e}")
                continue

        # 3. Swap files
        if os.path.exists(temp_output):
            if os.path.exists(input_gpkg):
                os.remove(input_gpkg)
            os.rename(temp_output, input_gpkg)
            logger.info("dissolve_overlaps complete.")
                
    def handle_simple_web(self, cfg):
        links = self._get_links_from_page(cfg)
        self.run_parallel_pipeline(links)

    def handle_selenium_web(self, cfg):
        session = self._get_authenticated_session(cfg['url'])
        scotland_codes = [
            "ABN", "ANG", "ARG", "AYR", "BNF", "BER", "BUT", "CTH", "CLK", "DMB", 
            "DMF", "ELN", "FFE", "GLA", "INV", "KNC", "KNR", "KRK", "LAN", "MID", 
            "MOR", "NRN", "OAZ", "PBL", "PTH", "REN", "ROS", "ROX", "SEL", "STG", 
            "STH", "WLN", "WGN"
        ]
        links = [f"{cfg['url']}/maps/download/ros-cp.cadastralparcel/{c}" for c in scotland_codes]
        self.run_parallel_pipeline(links, session=session)

    def run_parallel_pipeline(self, links, session=None):
        logger.info(f"Starting parallel pipeline for {len(links)} files...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.pipeline_worker, url, session) for url in links]
            for future in concurrent.futures.as_completed(futures):
                logger.info(future.result())

    def pipeline_worker(self, url, session=None):
        """Download -> Explicit Unzip -> Convert -> Cleanup."""
        filename = os.path.basename(urlparse(url).path)
        if not filename.endswith('.zip'): filename += ".zip"
        
        zip_path = os.path.join(self.cache_dir, filename)
        gpkg_path = os.path.join(self.cache_dir, filename.replace(".zip", ".gpkg"))

        if os.path.exists(gpkg_path): return f"SKIPPED: {filename}"

        try:
            # 1. Download

            logger.info(f"DOWNLOADING: {filename}")

            client = session if session else requests
            r = client.get(url, stream=True, timeout=60)
            r.raise_for_status()
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"PROCESSING: {filename}")

            # 2. Explicit Unzip (using your logic)
            extracted_files = []
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Support both GML (England) and Shapefiles (Scotland)
                targets = [m for m in zip_ref.namelist() if m.lower().endswith(('.gml', '.shp', '.shx', '.dbf', '.prj'))]
                for member in targets:
                    unique_name = f"{os.path.splitext(filename)[0]}_{os.path.basename(member)}"
                    target_path = os.path.join(self.cache_dir, unique_name)
                    with zip_ref.open(member) as source, open(target_path, "wb") as target:
                        shutil.copyfileobj(source, target)
                    extracted_files.append(target_path)

            os.remove(zip_path)

            # 3. Convert
            # We target the primary data file (GML or SHP)
            primary_file = next((f for f in extracted_files if f.lower().endswith(('.gml', '.shp'))), None)
        
            if primary_file:
                subprocess.run([
                    "ogr2ogr", "-f", "GPKG", gpkg_path, primary_file,
                    "-nln", "merged_data", "-nlt", "PROMOTE_TO_MULTI",
                    "-lco", "GEOMETRY_NAME=geom",
                    "-oo", "GML_SKIP_RESOLVE_ELEMS=YES"
                ], check=True, capture_output=True)

            # 4. Cleanup extracted source files
            for f in extracted_files:
                if os.path.exists(f): os.remove(f)

                # If it was a GML, look for the REPLACED extension .gfs
                if f.lower().endswith('.gml'):
                    gfs_ghost = os.path.splitext(f)[0] + ".gfs"
                    if os.path.exists(gfs_ghost):
                        os.remove(gfs_ghost)

            return f"SUCCESS: {filename}"

        except Exception as e:
            if os.path.exists(zip_path): os.remove(zip_path)
            return f"FAILED: {filename} - {e}"

    def _get_links_from_page(self, cfg):
        try:
            r = requests.get(cfg['url'], headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
            soup = BeautifulSoup(r.text, 'html.parser')
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                text = a.get_text(strip=True).lower()
                if href.endswith(cfg.get('ext', '.zip')) and any(m in text for m in cfg.get('match_text', [])):
                    links.append(urljoin(cfg['url'], href))
            return sorted(list(set(links)))
        except: return []

    def _get_authenticated_session(self, url):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        if platform.system() == "Darwin":
            service = Service("/opt/homebrew/bin/chromedriver")
            options.binary_location = "/Applications/Chromium.app/Contents/MacOS/Chromium"
        else:
            service = Service("/usr/bin/chromedriver")
            options.binary_location = "/usr/bin/chromium"

        driver = webdriver.Chrome(service=service, options=options)

        try:
            driver.get(url)
            wait = WebDriverWait(driver, 15)
            cb = wait.until(EC.element_to_be_clickable((By.ID, "terms-and-conditions")))
            driver.execute_script("arguments[0].click();", cb)
            wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Submit') and not(@disabled)]"))).click()
            time.sleep(2)
            session = requests.Session()
            for c in driver.get_cookies(): session.cookies.set(c['name'], c['value'])
            return session
        finally: driver.quit()

    def amalgamate_gpkgs(self, cache_dir, output_master):
        """
        Python equivalent of the shell loop to merge multiple GeoPackages 
        into a single master file within the specified cache directory.
        """
        # Find all .gpkg files in the cache folder
        gpkg_files = sorted(glob.glob(os.path.join(cache_dir, "*.gpkg")))

        if not gpkg_files:
            logger.warning(f"No GeoPackage files found in {cache_dir}.")
            return

        first_file = gpkg_files[0]
        
        result = subprocess.run(["ogrinfo", "-so", "-al", first_file], capture_output=True, text=True)
        if "27700" in result.stdout:
            logger.info("Verified: Input data is in EPSG:27700 (Meters)")
        else:
            logger.error('Input data is NOT in EPSG:27700 (Meters)')
            exit()

        # 1. Initialize the master file with the first file found
        # Using -select "" to keep it lightweight (geometry only) as per your script
        logger.info(f"Initializing master file with: {os.path.basename(first_file)}")
        init_cmd = [
            "ogr2ogr", "-f", "GPKG", output_master, first_file,
            "-select", "",
            "-nln", "merged_data",
            "-nlt", "PROMOTE_TO_MULTI",
            "-lco", "GEOMETRY_NAME=geom",
        ]
        subprocess.run(init_cmd, check=True)

        # Intermediate buffer to handle the append logic safely
        buffer_file = f"buffer-{str(uuid.uuid4())}.gpkg"

        # 2. Loop through the remaining files and append
        for f in gpkg_files[1:]:
            # Skip the master file if it happens to be in the same directory
            if os.path.abspath(f) == os.path.abspath(output_master):
                continue

            logger.info(f"Processing {os.path.basename(f)}...")
                        
            # Extract to buffer
            subprocess.run([
                "ogr2ogr", buffer_file, f,
                "-select", "",
                "-nln", "temp_layer",
                "-nlt", "PROMOTE_TO_MULTI",
                "-lco", "GEOMETRY_NAME=geom",
            ], check=True)

            # Append buffer to master
            subprocess.run([
                "ogr2ogr", "-update", "-append", output_master, buffer_file,
                "-nln", "merged_data",
                "-nlt", "PROMOTE_TO_MULTI",
            ], check=True)

            # Cleanup intermediate buffer
            if os.path.exists(buffer_file):
                os.remove(buffer_file)

        logger.info(f"Amalgamation complete: {output_master}")
        
    def apply_post_amalgamation_filter(self, output_file):
        """Applies area filtering to the finished master file."""
        tmp_output = f"tmp-{output_file}"
        where_parts = []
        min_ha = self.area.get('min')
        max_ha = self.area.get('max')
        
        if min_ha is not None:
            where_parts.append(f"ST_Area(geom) >= {min_ha * 10000}")
        if max_ha is not None:
            where_parts.append(f"ST_Area(geom) <= {max_ha * 10000}")
        
        if not where_parts:
            return

        sql_where = " AND ".join(where_parts)
        logger.info(f"Applying final area filter to {output_file}: {sql_where}")

        try:
            # We use ST_Area(geom) because it is standard OGC SQL for GeoPackages
            subprocess.run([
                "ogr2ogr", "-f", "GPKG", tmp_output, output_file,
                "-where", sql_where,
                "-nln", "merged_data",
                "-lco", "GEOMETRY_NAME=geom",
            ], check=True)

            # Cleanup and Rename
            os.remove(output_file)
            os.rename(tmp_output, output_file)
            logger.info("Filtering complete. Output file updated.")
        except Exception as e:
            logger.error(f"Failed to apply area filter: {e}")
            if os.path.exists(tmp_output): os.remove(tmp_output)

def main():
    if len(sys.argv) >= 2:
        config_path = sys.argv[1]
    else:
        logger.info("[openinspire] No config provided. Searching for internal inspire.yml...")
        try:
            ref = pkg_resources.files('openinspire').joinpath('inspire.yml')
            with pkg_resources.as_file(ref) as p:
                config_path = str(p)
        except Exception:
            import openinspire as openinspire_module
            package_dir = os.path.dirname(openinspire_module.__file__)
            config_path = os.path.join(package_dir, 'inspire.yml')

    if not os.path.exists(config_path):
        logger.error(f"Error: Could not find config at {config_path}")
        sys.exit(1)

    app = OpenINSPIRE(config_path)
    app.run()

if __name__ == "__main__":
    main()