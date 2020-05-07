# Make sure to install and activate the reddit_covid environment first with Anaconda using the environment.yml file

from flask import Flask, request, render_template
import csv

# Set port
PORT = 8080

# Set states
states = ["Alaska",
          "Alabama",
          "Arkansas",
          "American Samoa",
          "Arizona",
          "California",
          "Colorado",
          "Connecticut",
          "District of Columbia",
          "Delaware",
          "Florida",
          "Georgia",
          "Guam",
          "Hawaii",
          "Iowa",
          "Idaho",
          "Illinois",
          "Indiana",
          "Kansas",
          "Kentucky",
          "Louisiana",
          "Massachusetts",
          "Maryland",
          "Maine",
          "Michigan",
          "Minnesota",
          "Missouri",
          "Mississippi",
          "Montana",
          "North Carolina",
          "North Dakota",
          "Nebraska",
          "New Hampshire",
          "New Jersey",
          "New Mexico",
          "Nevada",
          "New York",
          "Northern Mariana Islands",
          "Ohio",
          "Oklahoma",
          "Oregon",
          "Pennsylvania",
          "Puerto Rico",
          "Rhode Island",
          "South Carolina",
          "South Dakota",
          "Tennessee",
          "Texas",
          "Utah",
          "Virginia",
          "Virgin Islands",
          "Vermont",
          "Washington",
          "Wisconsin",
          "West Virginia",
          "Wyoming"]

# Set app
app = Flask(__name__)

# Landing page for dashboard
@app.route("/")
def home():
    return render_template('index.html')


# Page for statewise correlation and plots
@app.route("/states", methods = ['GET', 'POST'])
def states_graph():
    # If POST, send back the chosen state and graph
    if request.method == 'POST':
        option = request.form.get("states")
        correlation = list(csv.reader(open("correlations.csv")))[states.index(option) + 1][5]
        return render_template('states.html', states=states, embedState=option, correlation=correlation)

    # if GET, just set up the initial page to submit
    else:
        return render_template('states.html', states=states)


# Start Flask app
if __name__ == '__main__':
    app.run(host="localhost", port=PORT)
