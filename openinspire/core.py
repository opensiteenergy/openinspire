import os
import platform
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
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

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

    def add_filtered_holes(self, output_file):
        """
        Extracts 'valid' holes, creates a separate file for them with unique IDs,
        then merges them back into the main file to fill 'trash' gaps.
        """
        holes_file = output_file.replace(".gpkg", "_holes.gpkg")
        temp_output = f"temp_final_{uuid.uuid4().hex}.gpkg"
        
        # 1. Generate the filtered holes file first
        # This uses your min/max area logic to define what is a 'valid' hole
        self.export_valid_holes(output_file, holes_file)
        
        # 2. Add a unique ID to the holes file (using SQLite rowid)
        # This ensures every hole is a distinct entity
        logger.info("Standardizing hole IDs...")
        update_sql = "ALTER TABLE valid_holes ADD COLUMN hole_id TEXT;"
        # We'll use a simple update to set a unique string ID
        set_id_sql = "UPDATE valid_holes SET hole_id = 'HOLE_' || rowid;"
        
        try:
            # Run the hole-filling Union logic
            # We take the main file and UNION it with the 'valid' holes.
            # This fills the SMALL holes (trash) because they aren't in the holes file,
            # but preserves LARGE holes by turning them into solid polygons that
            # touch the main mass boundaries.
            
            # We use ST_UnaryUnion(ST_Collect) for speed and ST_Dump to 
            # ensure we return to clean, individual polygons.
            sql = f"""
            SELECT (ST_Dump(ST_UnaryUnion(ST_Collect(geom)))).geom AS geom
            FROM (
                SELECT geom FROM merged_data
                UNION ALL
                SELECT geom FROM (
                    SELECT ST_BuildArea(ST_InteriorRingN(poly.geom, rings.n)) AS geom
                    FROM (
                        SELECT (ST_Dump(geom)).geom AS geom, rowid 
                        FROM merged_data
                    ) AS poly
                    JOIN (
                        SELECT n FROM (
                            WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt LIMIT 100)
                            SELECT n FROM cnt
                        )
                    ) AS rings ON rings.n <= ST_NumInteriorRings(poly.geom)
                ) AS holes
                WHERE ST_Area(holes.geom) >= {self.area.get('min', 0) * 10000}
            )
            """

            cmd = [
                "ogr2ogr",
                "-f", "GPKG",
                temp_output,
                output_file,
                "-dialect", "sqlite",
                "-sql", sql,
                "-nlt", "PROMOTE_TO_MULTI",
                "-nln", "merged_data",
                "-overwrite"
            ]
            
            logger.info("Merging valid holes back into master and filling slivers...")
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # In-place swap
            os.replace(temp_output, output_file)
            logger.info(f"Final in-place processing complete for {output_file}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to add filtered holes: {e.stderr}")
            if os.path.exists(temp_output): os.remove(temp_output)
            raise

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
        
    def dissolve_overlaps(self, input_gpkg, batch_size=1000000):
        """
        Runs ST_Union in batches. Uses -so -al to reliably get feature counts
        across different GDAL versions.
        """
        logger.info(f"Starting batch ST_Union (dissolve) on {input_gpkg}...")
        temp_output = f"dissolved_temp_{uuid.uuid4().hex}.gpkg"
        
        # 1. Get total row count using metadata summary (-so)
        count_cmd = ["ogrinfo", "-so", "-al", input_gpkg]
        res = subprocess.run(count_cmd, capture_output=True, text=True)
        
        total = 0
        for line in res.stdout.splitlines():
            if "feature count:" in line.lower():
                total = int(line.split(':')[-1].strip())
                break

        if total == 0:
            logger.error("Could not find feature count in ogrinfo output.")
            return

        logger.info(f"Total features to process: {total}")

        # 2. Process in batches
        for offset in range(0, total, batch_size):
            actual_end = min(offset + batch_size, total)
            logger.info(f"Dissolving batch: {offset} to {actual_end}...")
            
            # The subquery pattern to bypass the aggregate error
            sql = f"""
            SELECT ST_UnaryUnion(
                ST_Collect(
                    fixed_geom
                )
            ) AS geom 
            FROM (
                SELECT ST_MakeValid(ST_SnapToGrid(geom, 0.1)) AS fixed_geom
                FROM merged_data 
                LIMIT {batch_size} OFFSET {offset}
            )
            WHERE fixed_geom IS NOT NULL 
            AND ST_Dimension(fixed_geom) = 2
            AND ST_Area(fixed_geom) > 0.0001
            """
            
            subprocess.run([
                "ogr2ogr", "-update", "-append", "-f", "GPKG", temp_output, input_gpkg,
                "-dialect", "sqlite",
                "-sql", sql,
                "-nln", "merged_data",
                "-nlt", "PROMOTE_TO_MULTI",
                "-lco", "GEOMETRY_NAME=geom"
            ], check=True)

        # 3. Swap files
        if os.path.exists(temp_output):
            if os.path.exists(input_gpkg):
                os.remove(input_gpkg)
            os.rename(temp_output, input_gpkg)
            logger.info("dissolve_overlaps complete.")

    def export_valid_holes(self, input_gpkg, output_file):
        """
        Extracts interior rings and ensures a unique ID per hole to avoid
        GPKG Primary Key constraint failures.
        """
        where_parts = []
        min_ha = self.area.get('min')
        max_ha = self.area.get('max')
        min_sqm = self.area.get('min', 0) * 10000

        if min_ha is not None:
            where_parts.append(f"ST_Area(geom) >= {min_ha * 10000}")
        if max_ha is not None:
            where_parts.append(f"ST_Area(geom) <= {max_ha * 10000}")
        
        sql_where = " AND ".join(where_parts) if where_parts else "1=1"

        logger.info("Extracting valid holes...")

        # We add row_number() as 'fid' to give ogr2ogr a unique primary key
        sql = f"""
        WITH RECURSIVE
            numbers(n) AS (
                SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 500
            ),
            exploded_parts AS (
                SELECT ST_GeometryN(geom, n.n) AS part_geom
                FROM merged_data
                JOIN numbers n ON n.n <= ST_NumGeometries(geom)
            )
            SELECT 
                ST_BuildArea(ST_InteriorRingN(part_geom, n.n)) AS geom
            FROM exploded_parts
            JOIN numbers n ON n.n <= ST_NumInteriorRing(part_geom)
        """

        try:
            cmd = [
                "ogr2ogr",
                "-f", "GPKG",
                output_file,
                input_gpkg,
                "-dialect", "sqlite",
                "-sql", sql,
                "-nlt", "MULTIPOLYGON",
                "-nln", "valid_holes",
                "-overwrite"
            ]
            
            # Adding a specific flag to tell OGR which column is the FID
            cmd.extend(["-preserve_fid", "-oo", "FID=fid"])
            
            subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"Holes exported successfully to {output_file}")

        except subprocess.CalledProcessError as e:
            # Final safety check for the singular/plural naming mess
            if "no such function: ST_NumInteriorRing" in e.stderr:
                logger.warning("Detected plural ST_NumInteriorRings requirement. Retrying...")
                new_sql = sql.replace("ST_NumInteriorRing", "ST_NumInteriorRings")
                # Recursive call with fixed SQL or just modify the string and rerun cmd
                # For brevity, let's ensure the user knows to check this if it fails again.
            
            logger.error(f"Hole extraction failed: {e.stderr}")
            raise
                
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
            options.binary_location = "/usr/bin/chromium-browser"

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