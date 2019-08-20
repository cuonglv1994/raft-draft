import configparser

node_config = []

for i in range(1,4):

    config_parser = configparser.ConfigParser()
    config_parser.read_file(open('./config_{}.conf'.format(i)))
    print(config_parser.sections())


