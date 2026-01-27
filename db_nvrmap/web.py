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


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__, template_folder=Path(__file__).parent / "templates")
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-key-change-in-production")

    @app.route("/")
    def index():
        """Render the main form."""
        return render_template("index.html")

    @app.route("/generate", methods=["POST"])
    def generate():
        """Process PFIs and return ZIP download."""
        # Parse PFIs from textarea (supports comma, space, newline separation)
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

        # Get form options
        view_type = request.form.get("view_type", "parcel")
        output_format_str = request.form.get("output_format", "nvrmap")
        filename = request.form.get("filename", "").strip() or "output"
        gainscore_str = request.form.get("gainscore", "").strip()

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

        # Create processing options
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
