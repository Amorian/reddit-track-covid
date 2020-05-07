# Using Reddit To Track COVID 19 Spread

Team Members:

* Alankrith Krishnan
* Alisha Sawant

All folders have a .sh script attached to them, use it to run the code for each section as necessary. If the scripts do not have the required permissions, make sure to give them permissions with
`chmod +x {script_name}.sh`

Data Ingestion:

* Install the Anaconda environment
```bash
conda env create -f environment.yml
```
* Activate the environment
```bash
conda activate reddit-scraper
```
* Run the `ingestion.sh` script or run the python code manually

Cleaning:

* Run the `reddit_cleaning.sh` or `nytimes_cleaning.sh` scripts for ease of use.
* If you choose not to use the scripts, load the required modules and run it through spark2-submit. The commands can be derived from the scripts themselves

Profiling:

* Run the `reddit_profiling.sh` or `nytimes_profiling.sh` scripts for ease of use.
* If you choose not to use the scripts, load the required modules and run it through spark2-submit. The commands can be derived from the scripts themselves

Analysis:

* Run the `analysis.sh` script for ease of use.
* If you choose not to use the script, load the required modules and run it through spark2-submit. The commands can be derived from the scripts themselves

Application:

* * Install the Anaconda environment
```bash
conda env create -f environment.yml
```
* Activate the environment
```bash
conda activate reddit_covid
```
* Start the flask server with
```bash
python webpage.py
```
* To view the webpage, go to `localhost:8080` for the dashboard and `localhost:8080/states` for separate plots and correlation for each state.

Outputs:

* Run `get-output.sh` to get all the outputs to the local directory.
* If you choose not to use the script, the commands to get all the output data can be found there.

The tableau workbook is available as `Normalized_Plots.twb`
