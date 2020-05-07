from flask import Flask, request, render_template
import csv

PORT = 8080

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

app = Flask(__name__)

@app.route("/", methods = ['GET', 'POST'])
def home():
    if request.method == 'POST':
        option = request.form.get("states")
        correlation = list(csv.reader(open("correlations.csv")))[states.index(option) + 1][5]
        return render_template('index.html', states=states, embedState=option, correlation=correlation) #insert
    else:
        return render_template('index.html', states=states)

if __name__ == '__main__':
    app.run(host="localhost", port=PORT)
