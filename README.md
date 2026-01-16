# Spark TMDB Movie Data Analysis

This project performs data analysis on TMDB movie data using Apache Spark. It includes scripts for data extraction, transformation, analysis, and visualization.

## Project Structure

- `tmdb-analysis.ipynb`: Main Jupyter notebook for performing the data analysis.
- `config.py`: Configuration file for settings like API keys or file paths.
- `requirements.txt`: Lists the Python dependencies required for the project.
- `utils/`: Contains utility scripts for various tasks:
    - `data_extractor.py`: Handles extracting data from the TMDB API or other sources.
    - `data_schema.py`: Defines the schema for the movie data.
    - `transform.py`: Contains functions for data cleaning and transformation using Spark.
    - `data_analysis.py`: Functions for performing analytical operations on the data.
    - `visualizations.py`: Scripts for generating visualizations from the analyzed data.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/manaf-dev/spark-tmdb-movie-data-analysis.git
    cd spark-tmdb-movie-data-analysis
    ```

2.  **Install dependencies:**
    It is recommended to use a virtual environment.
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configuration:**
    Create a `.env` file in the root directory of the project and add your TMDB API key to it:
    ```
    TMDB_API_KEY=your_tmdb_api_key_here
    ```

4.  **Run the analysis:**
    Open and run the `tmdb-analysis.ipynb` Jupyter notebook to execute the data analysis workflow.
    ```bash
    jupyter notebook tmdb-analysis.ipynb
    ```