"""Flask web interface for db-nvrmap."""

import io
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path

from flask import Flask, render_template, request, send_file, flash, redirect, url_for

from .core import (
    ProcessingOptions,
    OutputFormat,
    generate_shapefile_to_gdf,
    write_shapefile,
    get_schema_for_format,
)


def extract_shapefile_from_zip(zip_file, temp_dir: str) -> str:
    """
    Extract shapefile from uploaded ZIP and return path to .shp file.

    Args:
        zip_file: Werkzeug FileStorage object
        temp_dir: Temporary directory path

    Returns:
        Path to extracted .shp file

    Raises:
        ValueError: If ZIP doesn't contain valid shapefile components
    """
    extract_path = Path(temp_dir) / "input"
    extract_path.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_file, 'r') as zf:
        # Validate ZIP contents for path traversal attacks
        for member in zf.namelist():
            if member.startswith('/') or '..' in member:
                raise ValueError("Invalid file path in ZIP: path traversal detected")

        zf.extractall(extract_path)

    # Find .shp file
    shp_files = list(extract_path.glob("**/*.shp"))
    if not shp_files:
        raise ValueError("No .shp file found in uploaded ZIP")
    if len(shp_files) > 1:
        raise ValueError("Multiple .shp files found in ZIP. Please upload only one shapefile.")

    shp_path = shp_files[0]

    # Validate required components exist
    required_extensions = ['.shp', '.shx', '.dbf', '.prj']
    for ext in required_extensions:
        if not (shp_path.parent / (shp_path.stem + ext)).exists():
            raise ValueError(f"Missing required shapefile component: {shp_path.stem}{ext}")

    return str(shp_path)


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__, template_folder=Path(__file__).parent / "templates")
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-key-change-in-production")

    # Set maximum upload size (50MB)
    app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

    @app.route("/")
    def index():
        """Render the main form."""
        return render_template("index.html")

    @app.route("/generate", methods=["POST"])
    def generate():
        """Process PFIs or uploaded shapefile and return ZIP download."""
        input_method = request.form.get("input_method", "pfi")

        # Get common form options
        output_format_str = request.form.get("output_format", "nvrmap")
        filename = request.form.get("filename", "").strip() or "output"
        gainscore_str = request.form.get("gainscore", "").strip()

        # Validate filename (server-side security check) — block path traversal, allow spaces/dots/parens
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9 ._(),-]*$', filename) or '..' in filename:
            flash("Invalid filename. Use letters, numbers, spaces, dots, underscores, hyphens, and parentheses.", "error")
            return redirect(url_for("index"))

        # Parse gain score
        gainscore = None
        if gainscore_str:
            try:
                gainscore = float(gainscore_str)
            except ValueError:
                flash("Invalid gain score. Please enter a number.", "error")
                return redirect(url_for("index"))

        # Map output format
        format_map = {
            "nvrmap": OutputFormat.NVRMAP,
            "ensym_2017": OutputFormat.ENSYM_2017,
            "ensym_2013": OutputFormat.ENSYM_2013,
        }
        output_format = format_map.get(output_format_str, OutputFormat.NVRMAP)

        # Handle different input methods
        if input_method == "shapefile":
            # Handle shapefile upload
            if 'shapefile_upload' not in request.files:
                flash("Please upload a shapefile.", "error")
                return redirect(url_for("index"))

            file = request.files['shapefile_upload']
            if file.filename == '':
                flash("No file selected.", "error")
                return redirect(url_for("index"))

            if not file.filename.endswith('.zip'):
                flash("Please upload a ZIP file containing shapefile components.", "error")
                return redirect(url_for("index"))

            # Will extract shapefile in temp directory during processing
            # Store the file for extraction later
            input_shapefile_file = file
            opts = ProcessingOptions(
                view_pfi=[],  # Empty for shapefile mode
                shapefile=filename,
                gainscore=gainscore,
                property_view=False,  # Not applicable for shapefile input
                output_format=output_format,
                input_shapefile="__TEMP__",  # Placeholder, will be replaced
            )
        else:
            # Handle PFI input
            pfi_text = request.form.get("pfis", "").strip()
            if not pfi_text:
                flash("Please enter at least one PFI number.", "error")
                return redirect(url_for("index"))

            # Split by any combination of commas, spaces, newlines
            pfi_strings = re.split(r"[,\s\n]+", pfi_text)
            pfi_strings = [p.strip() for p in pfi_strings if p.strip()]

            # Validate and convert to integers
            try:
                pfis = [int(p) for p in pfi_strings]
            except ValueError:
                flash("Invalid PFI format. Please enter only numbers.", "error")
                return redirect(url_for("index"))

            if not pfis:
                flash("Please enter at least one PFI number.", "error")
                return redirect(url_for("index"))

            view_type = request.form.get("view_type", "parcel")
            input_shapefile_file = None
            opts = ProcessingOptions(
                view_pfi=pfis,
                shapefile=filename,
                gainscore=gainscore,
                property_view=(view_type == "property"),
                output_format=output_format,
            )

        # Generate shapefile in temp directory, then ZIP
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                # If shapefile mode, extract uploaded file first
                if input_method == "shapefile":
                    try:
                        extracted_shp_path = extract_shapefile_from_zip(input_shapefile_file, tmpdir)
                        opts.input_shapefile = extracted_shp_path
                    except ValueError as e:
                        flash(f"Shapefile extraction error: {e}", "error")
                        return redirect(url_for("index"))

                shapefile_path = Path(tmpdir) / filename

                # Generate the GeoDataFrame
                output_gdf = generate_shapefile_to_gdf(opts)

                # Write to temp directory
                write_shapefile(output_gdf, output_format, str(shapefile_path))

                # Create ZIP in memory
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    # Add all shapefile components
                    for file_path in shapefile_path.iterdir():
                        zf.write(file_path, file_path.name)

                zip_buffer.seek(0)

                return send_file(
                    zip_buffer,
                    mimetype="application/zip",
                    as_attachment=True,
                    download_name=f"{filename}.zip",
                )

        except EnvironmentError as e:
            flash(f"Configuration error: {e}", "error")
            return redirect(url_for("index"))
        except ValueError as e:
            flash(f"Processing error: {e}", "error")
            return redirect(url_for("index"))
        except Exception as e:
            flash(f"An error occurred: {e}", "error")
            return redirect(url_for("index"))

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
