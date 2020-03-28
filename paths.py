import os

# root path of the project
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# directory for logs
LOG_DIR = os.path.join(ROOT_DIR, 'logs')
RUNTIME_LOG_PATH = os.path.join(LOG_DIR, 'runtime.log')

# dir for all config
CONFIGS_DIR = os.path.join(ROOT_DIR, 'config')
LOG_CONFIG_PATH = os.path.join(CONFIGS_DIR, 'logger-conf.json')
PUMP_CONFIG_PATH = os.path.join(CONFIGS_DIR, 'pump_conf.ini')

# dir for all cache
CACHE_DIR = os.path.join(ROOT_DIR, 'cache')

# dir for all persistence
PERSISTENCE_DIR = os.path.join(ROOT_DIR, 'persistence')
PUMP_MANAGER_PERSISTENCE_PATH = os.path.join(PERSISTENCE_DIR, 'pump_manager.shelve')

# path for keywords.txt
KEYWORDS_PATH = os.path.join(ROOT_DIR, 'keywords.txt')
