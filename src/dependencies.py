import yaml

with open("config.yml", "r") as yml:
    cfg = yaml.load(yml, Loader=yaml.FullLoader)

files = cfg['files']
source_path = files['inputpath']
charges_use_csv = source_path+"/"+cfg["files"]["charges"]
damages_use_csv = source_path+"/"+cfg["files"]['damages']
endorse_use_csv = source_path+"/"+cfg["files"]['endorse']
primary_person_use_csv = source_path+"/"+cfg["files"]['person']
restrict_use_csv = source_path+"/"+cfg["files"]['charges']
units_use_csv = source_path+"/"+cfg["files"]['units']

