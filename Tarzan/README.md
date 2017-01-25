## About
This project seeks to detect abnormal energy usage (high or low) for any building on the University of Chicago's campus given electricity meter data.

This module is a python implementation of the Tarzan Algorithm based on ["Finding Surprising Patterns in a Time Series Database in Linear Time and Space"](http://www.cs.ucr.edu/~eamonn/sigkdd_tarzan.pdf) by Keogh, et. al (2002). In short, the algorithm converts a discretizes a time series and turns it into a string. It then uses Markov models to find surprising patterns.

## The Code
The implementation of the algorithm lives in the general purpose module tarzan.py. tarzan-pipeline.py is the code that runs the electricity data through the algorithm and includes the discretization of the data.

Other files include attempts at multiprocessing and multithreading, as well as a general exploration notebook.

## The Data
The dataset was provided to us by the Facilities Management Division at the University of Chicago. It includes electrical meter readings taken every 30 minutes from march 2014 - March 2016 for the University from ComEd, the electricity provider. The readings were taken from over 300 meters across more than 100 buildings.

## More Information
Read the report over at https://github.com/cszc/123_Electricity_Data/blob/master/CS123FinalReportSpring2016.pdf
