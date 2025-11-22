#!/bin/bash
# FRED Query Tool Setup

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

echo "🔧 Setting up FRED Query Tool..."

# Create virtual environment
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Install dependencies
echo "Installing dependencies..."
"$VENV_DIR/bin/pip" install -q --upgrade pip
"$VENV_DIR/bin/pip" install -q deltalake polars adlfs azure-identity pyarrow pandas

echo "✅ Setup complete!"
echo ""
echo "Usage:"
echo "  source $VENV_DIR/bin/activate"
echo "  python -c \"from fred_query import *; print(series('M2MSL').head(10))\""
echo ""
echo "Or run interactively:"
echo "  $VENV_DIR/bin/python -i fred_query.py"
