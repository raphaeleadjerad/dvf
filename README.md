dvf
==============================

First step is to load DVF in a postgresql db, for this check `src/dvf_psql.py`.

Project Organization

```
├── data
│   ├── raw                 <- The original, immutable data dump.
│   ├── interim             <- Intermediate data that has been transformed.
│   ├── processed           <- The final, canonical data sets for modeling.
│   ├── output              <- Output from models
│   ├── schemas             <- Raw and processed data schemas, based on Table Schema standard
|
├── documentation           <- Documentation for the project
|
├── notebooks               <- Notebooks Jupyter (only include jupytext --to .py version of notebooks) 
|
├── reports                 <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures             <- Generated graphics and figures to be used in reporting
|
├── setup.py                <- makes project pip installable (pip install -e .) so src can be imported
├── src                     <- Source code
│   ├── __init__.py         <- Makes src a Python module
|
├── tests                   <- Tests for our projet
|            
├── LICENCE.txt
├── README.md
├── requirements.txt        <- The requirements file for reproducing the analysis environment, e.g.
│                            generated with `pip freeze > requirements.txt`
```

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>