# Run App
Run the backend 
`uv run uvicorn src.api.main:app --reload --port 8000`

Run the frontend
`uv run streamlit run src/frontend/streamlit.py`

# Dependencies
Install packages
`uv sync`

Add dependency
`uv add fastapi`

Add Dev dependency
`uv add --dev pytest`

# Run Tests
Run all tests
`uv run pytest`