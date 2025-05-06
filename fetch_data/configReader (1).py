import configparser as iniReader

class ConfigReader:
    def __init__(self, config_file):
        self.config = iniReader.ConfigParser()
        self.config.read(config_file)
    
    def get_db_params(self):
        db_params = self.config['DatabaseConfig']
        conn_str = f"dbname='{db_params['dbname']}' user='{db_params['user']}' host='{db_params['host']}' port='{db_params['port']}' password='{db_params['password']}'"
        return conn_str