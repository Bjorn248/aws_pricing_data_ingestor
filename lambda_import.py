import os
import re
import requests
import hashlib
import pymysql.cursors
import json
from pricing_import import do_import


def lambda_handler(event, context):

    do_import()
