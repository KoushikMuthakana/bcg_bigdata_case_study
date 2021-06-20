import yaml

with open("config.yml", "r") as yml:
    cfg = yaml.load(yml, Loader=yaml.FullLoader)

files = cfg['files']

