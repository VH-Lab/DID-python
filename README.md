# DID-Python

This is a Python port of the VH-Lab/DID-matlab project.

## Setup

To set up the environment and run the tests, follow these steps:

1.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    ```

2.  **Activate the virtual environment:**
    *   On Windows:
        ```bash
        venv\Scripts\activate
        ```
    *   On macOS and Linux:
        ```bash
        source venv/bin/activate
        ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run tests:**
    ```bash
    python -m unittest discover tests
    ```

## Documentation

This project uses [MkDocs](https://www.mkdocs.org/) for documentation. To view the documentation locally, run the following command:

```bash
mkdocs serve
```

Then, open your web browser and navigate to `http://127.0.0.1:8000`.