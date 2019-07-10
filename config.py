import json


def get_config_from_json(json_file):
    """
    Get the config from a json file
    :param json_file:
    :return: config(namespace) or config(dictionary)
    """
    # parse the configurations from the config json file provided
    with open(json_file, 'r') as config_file:
        config = json.load(config_file)

    return config

if __name__ == '__main__':
    get_config_from_json('.//Keys//keys.json')
