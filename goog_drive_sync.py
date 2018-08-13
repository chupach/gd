"""
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
    2. then: go in parent dir of the location dir_to_sync you want to sync and in python:
    3. in cli python3, e.g.
        - from goog_drive_sync import *; gd=GD(credential_json_filename)
        - gd.sync_from_ls_drive(dir_to_sync)
"""


import os,sys
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from apiclient.http import MediaFileUpload
from mimetypes import MimeTypes
import googleapiclient

mime = MimeTypes()

import time
import ssl
import subprocess
import hashlib
from itertools import groupby
import pickle

num_retries = 3
dt_retries_ms = 2000

def usage():
    print(__doc__)


def md5sum(self, filename, bs=65536):
#        with open(filename, mode='rb') as f:
#            d = hashlib.md5()
#            while True:
#                buf = f.read(bs)
#                if not buf: break
#                d.update(buf)
#            return d.hexdigest()
    md5chksum = subprocess.check_output(["openssl", "md5", filename], stderr=subprocess.STDOUT).split()[-1].decode()
    return md5chksum


def load_fl(self, fname='filelist_dump_raw.pickle'):
    return pickle.load(open(fname,'rb'))



class GD:
    def __init__(self, credential_filename = 'credentials2.json'):
        self.folder_mimetype = 'application/vnd.google-apps.folder'
        cred_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), credential_filename)
        SCOPES='https://www.googleapis.com/auth/drive' # all drive write scope
        store = file.Storage(cred_filename)
        creds = store.get()

        # rerun this anyway if change scape (now, we can do whatever write on the whole drive)
        if (not creds) or creds.invalid or (list(creds.scopes)[0] != SCOPES):
            flow = client.flow_from_clientsecrets('client_secret_google_drive.json', SCOPES)
            creds = tools.run_flow(flow, store)
        self.service = build('drive', 'v3', http=creds.authorize(Http()))
        self.root_id = self.ls()['parents'][0][0]


    def walk_diff(self, gen_walk_os, gen_walk_drive):
        """ Return only what's not in drive. Skip when local fn and dn empty """
        local_files_dict = {dp:(dn,fn) for dp,dn,fn in gen_walk_os}
        drive_files_dict = {dp:(dn,fn) for dp,dn,fn in gen_walk_drive}
        for el in local_files_dict:
            if el not in drive_files_dict:
                dn_local,fn_local = local_files_dict[el]
                if dn_local or fn_local:
                    yield (el,dn_local,fn_local)
            else:
                dn_drive,fn_drive = drive_files_dict[el]
                dn_local,fn_local = local_files_dict[el]
                dn_diff = list(set(dn_local)-set(dn_drive))
                fn_diff = list(set(fn_local)-set(fn_drive))
                if dn_diff or fn_diff:
                    yield (el,dn_diff,fn_diff)


    def walk(self, top_dir):
        """ similar to os.walk: for dp,dn,fn in gd.walk(folder_name):print(dp,dn,fn) """
        def lsw(dp):
            l = self.ls(self.get_id_by_path(dp))
            dn, fn = [], []
            if l:
                for i,el in enumerate(l['mimeType']):
                    if el == self.folder_mimetype:
                        dn.append(l['name'][i])
                    else:
                        fn.append(l['name'][i])
                return dp,dn,fn
            else:
                return dp,[],[]
        dp, dn, fn = lsw(top_dir)
        dn.sort()
        yield (dp, dn, sorted(fn))
        for dir in dn:
            yield from self.walk(dp+'/'+dir)


    def sync_from_list(self, g):
        for dp, dn, fn in g:
            print('syncing',dp)
            parent_id = self.get_id_by_path(dp)
            if parent_id is None:
                parent_id = self.create_folder_by_path(dp)
            for d in dn:
                self.create_folder_by_path( os.path.join(dp, d) )
            for f in fn:
                self.upload( os.path.join(dp, f), parent_id )
            print()


    def list_files(self, q, fields):
        """ use for instance :
            q = "'parent_id' in parents", "name contains 'xxxyyy'",
                "mimeType='%s'" % self.folder_mimetype
            fields = 'id, name, parents, mimeType, md5Checksum'
        """
        page_token = None
        filelist = []

        if q:
            q += ' and trashed=false'
        else:
            q = 'trashed=false'

        print('list files q =', q, ', fields =', fields)
        while True:
            for _ in range(num_retries):
                try:
                    response = self.service.files().list(q = q, pageSize=1000,
                                              fields='nextPageToken, files(%s)' % fields,
                                              pageToken=page_token).execute()
                    break
                except Exception:
                    time.sleep(5)
            else:
                print('ERROR list_files',q,fields)
                raise
            
            filelist += response.get('files', [])
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        if 'id' in fields.split(','):
            filelist.sort(key=lambda k: k['id'])
        return filelist


    def file_exists(self, id):
        try:
            if self.service.files().get(fileId=id, fields='trashed').execute()['trashed']:
                return False
        except googleapiclient.errors.HttpError as e:
            if "File not found" in str(e):
                return False
            else:
                print('file_exists: unknown error',e)
        return True


    def ls(self, parent_id='root', filename = None): # should allow wildcard for filename
        if parent_id is None:
            return None

        if filename is None:
            q=""" "%s" in parents""" % parent_id
        elif filename.endswith('*'): # google drive api "contains" is actually "startswith"
            q=""" "%s" in parents and name contains "%s" """ % (parent_id, filename[:-1])
        else:
            q=""" "%s" in parents and name="%s" """ % (parent_id, filename)

        filelist = self.list_files(q,fields='id, name, parents, md5Checksum, mimeType')
        for i,file in enumerate(filelist):
            if 'md5Checksum' not in file:
                 filelist[i]['md5Checksum'] = None
        return {k: [d[k] for d in filelist] for k in filelist[0].keys()} if filelist else {}


    def ls_by_path(self, path, filename = None):
        return self.ls( self.get_id_by_path(path.rstrip('/')), filename )

    def get_id(self, filename, parent_id='root'):
        filedict = self.ls(parent_id,filename)
        return filedict['id'][0] if filedict else None
        
    def get_id_by_path(self, path):
        l = path.split('/')
        for i,filename in enumerate(l):
            if i == 0:
                id = self.get_id(filename,parent_id='root')
            else:
                id = self.get_id(filename,id)
            if id is None:
                return
        return id



    def create_folder_by_path(self, folder_name):
        """ create folder in drive, e.g 'a/b/c' will create a in root, b in a and c in b """
        folder_list = folder_name.split('/')
        for i,folder in enumerate(folder_list):
            if i == 0:
                id = self.create_folder(folder,parent_id='root')
            else:
                id = self.create_folder(folder,parent_id=id)
        return id


    def create_folder(self, folder_name, parent_id = 'root'):
        if not self.file_exists(parent_id):
             print('parent folder does not exists',parent_id)
             return

        id = self.get_id(folder_name, parent_id)
        if id is not None:
            # print('folder',folder_name,'already exists with id',id)
            return id

        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }

        for _ in range(num_retries):
            try:
                id = self.service.files().create(body=file_metadata, fields='id').execute()['id']
                break
            except Exception:
                time.sleep(5)
        else:
            print('ERROR create_folder',file_metadata)
            raise

        print('folder', folder_name, 'created with id',id,'in parent folder',file_metadata['parents'])
        return id



    def upload(self, filename_with_path, parent_id, md5_check=True):
        filename = os.path.basename(filename_with_path)
        filedict = self.ls(parent_id, filename)
        if filedict:
            ids = filedict['id']
            print('file',filename_with_path,'already uploaded with ids',ids,'. Checking ckecksum')
            md5_local = md5sum(filename_with_path)
            md5_drive = filedict['md5Checksum']

            if md5_local in md5_drive:
                # print( md5_drive.index(md5_local), filedict['md5Checksum'])
                return ids[ md5_drive.index(md5_local) ]
            else:
                print('delete current and upload new one not implemented. Upload new instance of file',filename)

        mime_type = mime.guess_type(filename_with_path)[0]
        body = {'name': filename, 'mimeType': mime_type, 'parents': [parent_id]}
        media = MediaFileUpload(filename_with_path, mimetype = mime_type)

        for _ in range(num_retries):
            try:
                id = self.service.files().create(body=body, media_body=media, fields='id').execute()['id']
                break
            except Exception:
                time.sleep(5)
        else:
            print('ERROR upload',body)
            raise

        print('uploaded', filename_with_path, mime_type, 'with id', id)
        return id


    def upload_by_path(self, filename_with_path, md5_check=True):
        filename = os.path.basename(filename_with_path)
        dirname =  os.path.dirname(filename_with_path)
        parent_id = self.get_id_by_path(dirname) if dirname else 'root'
        id = self.upload(filename_with_path,parent_id)
        return id


    def ls_drive(self, only_dir = False, dump = True):
        """
        recursive ls from basedir: takes about 10 minute for 200'000 files in the drive
        (retrieve 1000 entries every 3 seconds)
        """
        t0 = time.time()
        fl = self.list_files(q = "mimeType='%s'" % self.folder_mimetype if only_dir else '',
                                   fields = 'id, name, parents, mimeType')
        fl = [e for e in fl if len(e.keys())==4] # list of dict

        if dump:
            fff = open('filelist_dump_raw.pickle','wb')
            pickle.dump(fl,fff)
            fff.close()

        return fl



    def get_tree(self, topdir, fl):
        """ build dict-like directory hirarchy tree from a given root """
        w = {e['id']: e for e in fl if len(e.keys())==4}
        w[self.root_id] = {'id': self.root_id, 'parents': [None], 'name': '', 'mimeType': self.folder_mimetype}
        for id in w:
            del w[id]['id']
            w[id]['parent'] = w[id]['parents'][0]
            del w[id]['parents']
            w[id]['tree'] = w[id]['name']
            w[id]['is_folder'] = 1 if w[id]['mimeType'] == self.folder_mimetype else 0
            del w[id]['mimeType']

        # set the tree (add children dicts to parent dict with implicit recursion)
        for id in w:
            if w[id]['is_folder']:
                w[id]['content'] = []
        for id in w:
            pid = w[id]['parent']
            if pid is not None:
                try:
                    w[pid]['content'].append(w[id])
                except KeyError:
                    print(pid, id, w[id]['name'])
                    raise

        topdir_id = self.get_id_by_path(topdir) if topdir else self.root_id
        return w[topdir_id] if topdir_id in w else {}


    def walk_from_treedict(self, d, max_depth=-1):
        if not d:
            return

        fn,dn=[],[]

        for el in d['content']:
            el['tree'] = (d['tree']+'/'+el['name']).strip('/')
            if el['is_folder']:
                dn.append(el['name'])
            else:
                fn.append(el['name'])

        yield (d['tree'],sorted(dn),sorted(fn))

        if max_depth==0:
            return
        for el in d['content']:
            if el['is_folder']:
                yield from self.walk_from_treedict(d = el, max_depth = max_depth - 1)


    def walk_from_ls_drive(self, top_dir, max_depth=-1):
        #fl = load_fl()
        fl = self.ls_drive()
        d = self.get_tree(top_dir,fl)
        yield from self.walk_from_treedict(d, max_depth)
    

#    def print_treedict(self, top_dir, max_depth=-1):
#        g = self.walk_from_ls_drive(top_dir, max_depth)
#        for el in g: print(el)


    def sync_naive(self, top_dir):
        """ sync local not in the drive (basic one way sync).
            Walk through local and sync missing drive files/dir
        """
        t0 = time.time()
        for dp, dn, filenames in os.walk(top_dir):
            print('syncing',dp)
            parent_id = self.get_id_by_path(dp)
            if parent_id is None:
                parent_id = self.create_folder_by_path(dp)
            for f in filenames:
                filename = os.path.join(dp, f)
                self.upload(filename,parent_id)
            print()
        t1 = time.time()
        print('walk diff dt',t1-t0)


    def sync(self, top_dir):
        """ generate differences of local and drive walks. Then sync over this result
        """
        t0 = time.time()
        g = self.walk_diff(os.walk(top_dir), self.walk(top_dir))
        self.sync_from_list(g)
        t1 = time.time()
        print('walk diff dt',t1-t0)


    def sync_from_ls_drive(self,top_dir):
        """
            Sync with first retrieving full file list and the compute differences.
            No md5 check
        """
        t0 = time.time()
        g = self.walk_diff(os.walk(top_dir), self.walk_from_ls_drive(top_dir))
        g = [e for e in g] # convert gen to list to measure time
        t1 = time.time()
        print('walk diff dt',t1-t0)
        
        self.sync_from_list(g)
        t2 = time.time()
        print('sync dt',t2-t1)
        return g



# from goog_drive_sync_v8 import *;gd=GD()
# top_dir='psitest'
# gd.sync(top_dir) # or sync_naive or sync_from_ls_drive



