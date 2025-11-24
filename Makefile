.PHONY: help install setup run run-features run-scoring test clean lint notebook

help:
	@echo "IF Recommender - Brazilian Fixed Income Fund Recommendation System"
	@echo ""
	@echo "Available commands:"
	@echo "  make setup          - Complete first-time setup (install + data setup)"
	@echo "  make install        - Install Python dependencies"
	@echo "  make data-setup     - Instructions for data setup from Google Drive"
	@echo "  make run            - Run complete pipeline"
	@echo "  make test           - Run tests"
	@echo "  make lint           - Run code linting"


install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt
	@echo "✅ Dependencies installed"

data-setup:
	@echo "DATA SETUP INSTRUCTIONS"
	@echo "======================="
	@echo "1. Download data.zip from Google Drive link (provided separately)"
	@echo "2. Unzip into project root:"
	@echo "   unzip data.zip"
	@echo "3. Verify structure:"
	@echo "   ls data/01_raw/cvm/data/"
	@echo "   ls data/01_raw/anbima/"
	@echo ""
	@echo "Expected structure:"
	@echo "  data/01_raw/cvm/data/pl/*.csv "
	@echo "  data/01_raw/cvm/data/blc_1/*.csv"
	@echo "  ... (blc_2 through blc_8)"
	@echo "  data/01_raw/anbima/*.xlsx (1 file)"

setup: install data-setup
	@echo ""
	@echo "✅ Setup complete! Next steps:"
	@echo "   1. Download and extract data (see above)"
	@echo "   2. Run: make run"

run:
	@echo "Running complete pipeline..."
	kedro run
	@echo "✅ Pipeline complete! Check data/08_reporting/recommendations.csv"

test:
	@echo "Running tests..."
	pytest

lint:
	@echo "Running linter..."
	ruff check src/