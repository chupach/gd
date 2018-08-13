# gd

GD class mainly provide walk-like methods on google drive for local to drive sync.

- sync and sync_from_ls_drive (fast when large number of files in the drive and only a fraction to sync) will not check md5, use sync_naive instead (will go over all drive files, can be slow).

- With md5 check: when differs from local value, upload new instance of the file

- If local files have disappeared, they are not removed on the drive

Note: tried gdcp on github, but would duplicate directories, no check for existence.

Several methods enable the user to get id, create folder or upload file giving a local path in unix format, which is very convenient:
    ls, ls_by_path with wildcard
    get_id, get_id_by_path
    create_folder, create_folder_by_path
    upload_by_path, upload

Get it running:

    1. download this file and credential_json_filename to dir, add dir to python path
    
    2. then: go in parent dir of the location dir_to_sync you want to sync and in cli python3:
    
        - from goog_drive_sync import *; gd=GD(credential_json_filename)
        
        - gd.sync_from_ls_drive(dir_to_sync)

