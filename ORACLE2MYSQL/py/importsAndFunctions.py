import pandas as pd
import csv
import os
import numpy as np
import MySQLdb
import d6tstack.utils
import time
import glob
from mysqlConnect import *
from oracleConnect import *
import xlrd
import threading

pd.options.mode.chained_assignment = None

def fileDir(dir):
    link = (f'{dir}')
    os.chdir(link)