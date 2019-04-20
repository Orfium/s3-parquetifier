"""
This file is specifically for reporting logs for the whole process
"""
import logging


# Initiate the logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s: %(levelname)-8s -  %(message)s',
                    datefmt='%m-%d %H:%M')

logger = logging.getLogger('siakon.s3Parquetifier')
