# HistorianDataCompletenessReporter
Example application that scans and operates on data read from the openHistorian 2.0:  
Runs through a given set of signals over a given time period and creates data completeness .csv reports for each signal.

## Usage:
* Download the binaries from the [latest release](https://github.com/GridProtectionAlliance/HistorianDataCompletenessReporter/releases).
* Extract the binaries .zip file.
* Double-click on HistorianDataCompletenessReporter.exe.
* If you get a popup that says "Microsoft Defender SmartScreen prevented an unrecognized app from running", click "More info", then "Run anyway" this screen pops up because the app is still in pre-production and hasn’t been signed by GPA yet.
*	Enter the correct IP address for the openHistorian you want to query in the Host Address box. The default Data Port, Meta-data Port, and Instance Name should work for querying the openHistorian unless the target openHistorian instance has been set up with non-default settings.
* Enter a point list or filter expression for the meters/channels you want to query.
* Enter a time range for your query.
* Select UTC or local time.
* Make sure the framerate estimate is set correctly.
* Enter a sample count window, the default is 60 seconds. 
* The framerate estimate and sample count window determine the number of expected data points per row.
* Select a destination folder. If this field is left blank, .csv file will be saved in the same directory as the HistorianDataCompletenessReporter.
* Click Go.
* If there are no errors, a .csv file will be created in the destination folder for each channel.
