"""Command-line interface for db-nvrmap."""

import argparse
import sys
from typing import Optional

from .core import ProcessingOptions, OutputFormat, generate_shapefile


def parse_args(args: Optional[list] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Process View PFIs to an Ensym shapefile.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  db-nvrmap 12345678                    # Process a single PFI
  db-nvrmap -e -p 12345678 87654321     # Property view PFIs in EnSym format
  db-nvrmap -i boundary.shp -s output   # Process custom shapefile input
  db-nvrmap -i boundary.shp -e          # Custom shapefile in EnSym format
  db-nvrmap --web                        # Start web interface on localhost:5000
  db-nvrmap --web --port 8080            # Start on custom port
"""
    )

    # Positional arguments (optional when using --web or --input)
    parser.add_argument(
        'view_pfi',
        metavar='N',
        type=int,
        nargs='*',
        help="PFI of the Parcel View (optional when using --input)"
    )

    # Input options
    parser.add_argument(
        "-i", "--input",
        type=str,
        dest="input_shapefile",
        help="Path to input shapefile (boundary only). System will intersect with EVC/bioregion data."
    )
    parser.add_argument(
        "--site-id-field",
        type=str,
        help="Field name in input shapefile to use for site_id grouping (reserved for future use)"
    )

    # Output options
    parser.add_argument(
        "-s", "--shapefile",
        default='nvrmap',
        help="Name of the shapefile/directory to write. Default is 'nvrmap'."
    )
    parser.add_argument(
        "-g", "--gainscore",
        type=float,
        help="Override gainscore value"
    )
    parser.add_argument(
        "-p", "--property",
        action='store_true',
        help="Use Property View PFIs"
    )
    parser.add_argument(
        "-e", "--ensym",
        action='store_true',
        help="Output in EnSym format"
    )
    parser.add_argument(
        "-b", "--sbeu",
        action='store_true',
        help="Output in 2013 SBEU format"
    )

    # Web server options
    parser.add_argument(
        "--web",
        action='store_true',
        help="Start the web interface instead of processing PFIs"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port for web server (default: 5000)"
    )
    parser.add_argument(
        "--host",
        default='127.0.0.1',
        help="Host for web server (default: 127.0.0.1). Use 0.0.0.0 for network access."
    )
    parser.add_argument(
        "--production",
        action='store_true',
        help="Use Gunicorn production server instead of Flask development server"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of Gunicorn worker processes (default: 4, only used with --production)"
    )

    return parser.parse_args(args)


def args_to_options(args: argparse.Namespace) -> ProcessingOptions:
    """Convert argparse Namespace to ProcessingOptions."""
    if args.sbeu:
        output_format = OutputFormat.ENSYM_2013
    elif args.ensym:
        output_format = OutputFormat.ENSYM_2017
    else:
        output_format = OutputFormat.NVRMAP

    return ProcessingOptions(
        view_pfi=args.view_pfi,
        shapefile=args.shapefile,
        gainscore=args.gainscore,
        property_view=args.property,
        output_format=output_format,
        input_shapefile=args.input_shapefile,
        site_id_field=args.site_id_field,
    )


def run_cli(args: argparse.Namespace) -> int:
    """Run the CLI processing mode."""
    has_pfi = bool(args.view_pfi)
    has_shapefile = args.input_shapefile is not None

    if not has_pfi and not has_shapefile:
        print("Error: Either PFI values or --input shapefile is required.", file=sys.stderr)
        return 1

    if has_pfi and has_shapefile:
        print("Error: Cannot specify both PFI values and --input shapefile.", file=sys.stderr)
        return 1

    # Warn if -p flag is used with shapefile input
    if has_shapefile and args.property:
        print("Warning: --property flag is ignored when using --input shapefile.", file=sys.stderr)

    try:
        opts = args_to_options(args)
        generate_shapefile(opts)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def run_web(args: argparse.Namespace) -> int:
    """Run the web server."""
    from .web import create_app

    if args.production:
        import gunicorn.app.base

        class StandaloneApplication(gunicorn.app.base.BaseApplication):
            def __init__(self, app, options=None):
                self.options = options or {}
                self.application = app
                super().__init__()

            def load_config(self):
                for key, value in self.options.items():
                    if key in self.cfg.settings and value is not None:
                        self.cfg.set(key.lower(), value)

            def load(self):
                return self.application

        app = create_app()
        options = {
            'bind': f'{args.host}:{args.port}',
            'workers': args.workers,
        }
        print(f"Starting Gunicorn with {args.workers} workers at http://{args.host}:{args.port}")
        StandaloneApplication(app, options).run()
    else:
        app = create_app()
        print(f"Starting web interface at http://{args.host}:{args.port}")
        app.run(host=args.host, port=args.port, debug=False)
    return 0


def main(args: Optional[list] = None) -> int:
    """Main entry point."""
    parsed_args = parse_args(args)

    if parsed_args.web:
        return run_web(parsed_args)
    else:
        return run_cli(parsed_args)


if __name__ == "__main__":
    sys.exit(main())
