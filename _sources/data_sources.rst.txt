Data Sources
============

Curtailments Data
-----------------

The :term:`CAISO` publishes daily reports of curtailed solar and load at 5-minute increments.   A massive simplifying assumption was to treat the load data as we would a dayahead load forecast.  Curtailment events were classified based on the amount of solar production curtailed relative to the entire day's worth of production.

.. toctree::
  :maxdepth: 1
  :caption: Exploration

  notebooks/0-ttu-curtailments_eda.ipynb


Historic Weather Forecasts
--------------------------

Historic forecasts from the Global Forecast System were extracted from the NOAA.  A dayahead forecast at 1° spatial resolution at 3-hour intervals were used for this study.  Extracting historic weather forecasts represented a significant amount of data engineering work in this project.  Two features of importance were taken from the dayahead weather forecasts - Temperature and downward shortwave radiation.

CA County Boundaries
--------------------

Geographic data from California's open data portal were used to subset global 

CEC Powerplant Locations
------------------------

Powerplant locations were used to weight weather features by the installed solar capacity in each county.