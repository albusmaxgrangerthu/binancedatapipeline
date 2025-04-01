import os # operating system operations like check files existance
from pathlib import Path
import gc # garbage collector
import time # time related utility functions
import getpass
import traceback
import requests

import pandas as pd # data frames wrangling
import numpy as np # fast and vectorized math functions
import scipy as sp # scientific calculation toolkit

import matplotlib.pyplot as plt # MATLAB-like plotting library
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
import plotly.graph_objects as go
from plotly.subplots import make_subplots
plt.rcParams["figure.figsize"] = (14, 12)
plt.rcParams['axes.unicode_minus'] = False  # This line is to ensure the minus sign displays correctly

from dateutil.relativedelta import relativedelta
#import datetime
from datetime import datetime,timedelta,timezone
import math
import re