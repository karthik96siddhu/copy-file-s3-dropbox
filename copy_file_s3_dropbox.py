import dropbox
import boto3
from tqdm import tqdm

import os
import datetime

from config.constant import AWS_BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_KEY, DROP_BOX_ACCESS_TOKEN

def copy_file_s3_dropbox(folder_name, local):
    try:
        client = boto3.client('s3', aws_acess_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        dbx = dropbox.Dropbox(DROP_BOX_ACCESS_TOKEN)

        keys = []
        dirs = []
        next_token = ''
        base_kwargs = {
            'Bucket': AWS_BUCKET_NAME,
            'Prefix': folder_name
        }
        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})
            results = client.list_objects_v2(**kwargs)
            contents = results.get('Contents')
            for i in contents:
                k = i.get('Key')
                if k[-1] != '/':
                    keys.append(k)
                else:
                    dirs.append(k)
            next_token = results.get('NextContinuationToken')
            for d in dirs:
                dest_pathname - os.path.join(local, d)
                if not os.path.exists(os.path.dirname(dest_pathname)):
                    os.makedirs(os.path.dirname(dest_pathname))
            for k in keys:
                dest_pathname - os.path.join(local, k)
                if not os.path.exists(os.path.dirname(dest_pathname)):
                    os.makedirs(os.path.dirname(dest_pathname))
                start_download = datetime.datetime.now()
                print('start-download => ', start_download)
                client.download_file(AWS_BUCKET_NAME,k,dest_pathname)
            end_download = datetime.datetime.now()
            print('end-download => ', end_download)

            dropbox_temp_path = local + '/' + folder_name
            directory = os.fsencode(dropbox_temp_path)

            for file in os.listdir(directory):
                filename = os.fsdecode(file)
                with open(dropbox_temp_path + '/' + filename, 'rb') as f:
                    start_upload = datetime.datetime.now()
                    print('start-upload => ', start_upload)
                    file_size = os.path.getsize(dropbox_temp_path + '/' + filename)
                    CHUNK_SIZE = 4 * 1024 *1024
                    if file_size <= CHUNK_SIZE:
                        dbx.files_upload(f.read(), folder_name + '/' + filename)
                    else:
                        with tqdm(total=file_size, desc='Uploaded') as pbar:
                            upload_session_start_result = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                            pbar.update(CHUNK_SIZE)
                            cursor = dropbox.files.UploadSessionCursor(
                                session_id=upload_session_start_result.session_id,
                                offset=f.tell(),
                            )
                            commit = dropbox.files.CommitInfo(path=folder_name + '/' + filename)
                            while f.tell() < file_size:
                                if (file_size - f.tell()) <= CHUNK_SIZE:
                                    print(
                                        dbx.files_upload_session_finish(
                                            f.read(CHUNK_SIZE), cursor, commit
                                        )
                                    )
                                else:
                                    dbx.files_upload_session_append(
                                        f.read(CHUNK_SIZE),
                                        cursor.session_id,
                                        cursor.offset
                                    )
                                    cursor.offset = f.tell()
                                pbar.update(CHUNK_SIZE)
            end_upload = datetime.datetime.now()
            print('end-upload => ', end_upload)
            if os.path.exists(dropbox_temp_path + '/' + filename):
                os.remove(dropbox_temp_path + '/' + filename)
            os.rmdir(dropbox_temp_path)
            return {'status': True, 'msg': 'Files copied successfully'}
    except Exception as e:
        print('error while copying file from s3-dropbox => ', e)
        return {'status': False, 'msg': f'{e}'}
