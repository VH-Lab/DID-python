# DID-Python

This is a Python port of the VH-Lab/DID-matlab project.

## Setup

1.  **Create a virtual environment:**

    ```bash
    python -m venv venv
    ```

2.  **Activate the virtual environment:**

    *   On macOS and Linux:

        ```bash
        source venv/bin/activate
        ```

    *   On Windows:

        ```bash
        .\\venv\\Scripts\\activate
        ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

## Running Tests

To run the tests, execute the following command from the root of the project:

```bash
python -m unittest discover tests
```
