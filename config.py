# services/users/project/config.py


import os

from dotenv import load_dotenv
# basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv()


class Config(object):
    ENV = 'production'
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    TABLE1 = "general_ratio"
    TABLE2 = "life_ratio"
    TABLE3 = "combined_rating"
    TABLE4 = "general_data"
    TABLE5 = "life_data"
    BADGE = " "


class ProductionConfig(Config):
    pass


class DevConfig(Config):
    ENV = 'development'
    DEBUG = True
    TABLE1 = "dev_general_ratio"
    TABLE2 = "dev_life_ratio"
    TABLE3 = "dev_combined_rating"
    TABLE4 = "dev_general_data"
    TABLE5 = "dev_life_data"
    BADGE = "!! DEVELOPMENT ENVIRONMENT !!"


class TestConfig(Config):
    ENV = 'testing'
    TESTING = True
    TABLE1 = "test_general_ratio"
    TABLE2 = "test_life_ratio"
    TABLE3 = "test_combined_rating"
    TABLE4 = "test_general_data"
    TABLE5 = "test_life_data"
    BADGE = "!! TESTING ENVIRONMENT !!"

class ResetConfig(Config):
    ENV = 'reset'
    TESTING = True
    TABLE1 = "test_general_ratio"
    TABLE2 = "test_life_ratio"
    TABLE3 = "test_combined_rating"
    TABLE4 = "test_general_data"
    TABLE5 = "test_life_data"
    BADGE = "!! TESTING ENVIRONMENT !!"

    
